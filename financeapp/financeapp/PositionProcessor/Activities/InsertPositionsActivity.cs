using Microsoft.Azure.Functions.Worker;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using PositionProcessor.Models;
using System.Data;
using System.Diagnostics;

namespace PositionProcessor.Activities;

/// <summary>
/// Calculates P&amp;L from trade records and bulk-inserts to Azure SQL.
/// Also writes a run log entry to SQL including structured error info for AI reasoning.
///
/// HOW APP INSIGHTS SHOWS SqlClient TRACES:
///   Microsoft.Data.SqlClient emits DiagnosticSource events.
///   The App Insights SDK picks these up via SqlClientDiagnosticListener
///   and records each query/command as a SQL dependency.
///   This is why you see:
///     - BulkInsertAsync duration as a dependency call
///     - Individual INSERT/UPDATE timings
///     - SQL errors with full exception details
///     - Connection open/close events
///
///   No extra code needed — just having Microsoft.ApplicationInsights.WorkerService
///   in the project enables this automatically.
/// </summary>
public class InsertPositionsActivity
{
    private readonly string _connectionString;
    private readonly ILogger<InsertPositionsActivity> _logger;

    public InsertPositionsActivity(IConfiguration config, ILogger<InsertPositionsActivity> logger)
    {
        _connectionString = config["SqlConnectionString"]!;
        _logger           = logger;
    }

    [Function(nameof(InsertPositionsActivity))]
    public async Task<int> Run([ActivityTrigger] InsertInput input)
    {
        var sw = Stopwatch.StartNew();

        _logger.LogInformation(
            "InsertPositionsActivity started. RunId={RunId} Status={Status} TradeCount={Count}",
            input.RunId, input.Status, input.Trades?.Count ?? 0);

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();

        int insertedCount = 0;

        if (input.Trades?.Count > 0)
        {
            // Calculate net positions per Portfolio + Instrument
            var positions = CalculatePositions(input.Trades, input.RunId);

            // BulkInsert — App Insights records duration of this SqlBulkCopy operation
            insertedCount = await BulkInsertPositionsAsync(conn, positions);

            _logger.LogInformation("BulkInsert complete. Rows={Rows} Duration={Ms}ms",
                insertedCount, sw.ElapsedMilliseconds);
        }

        // Always write a run log entry (success or failure) with full error context
        await WriteRunLogAsync(conn, input.RunId, input.Status, insertedCount, sw.Elapsed, input.ErrorMessage);

        return insertedCount;
    }

    private static List<PositionResult> CalculatePositions(List<TradeRecord> trades, string runId)
    {
        // Group by Portfolio + Instrument, net out BUY vs SELL
        return trades
            .GroupBy(t => (t.Portfolio, t.Instrument))
            .Select(g =>
            {
                var netQty = g.Sum(t => t.Direction == "BUY" ? t.Quantity : -t.Quantity);
                var avgPx  = g.Average(t => t.Price);
                var pnl    = netQty * avgPx * 0.02m; // simplified PnL calc for POC

                return new PositionResult(
                    Portfolio:    g.Key.Portfolio,
                    Instrument:   g.Key.Instrument,
                    NetQuantity:  netQty,
                    MarketValue:  netQty * avgPx,
                    PnL:          pnl,
                    CalculatedAt: DateTime.UtcNow,
                    RunId:        runId
                );
            })
            .ToList();
    }

    /// <summary>
    /// SqlBulkCopy is tracked by App Insights as a SQL dependency.
    /// Duration shows up in the dependency timeline.
    /// </summary>
    private async Task<int> BulkInsertPositionsAsync(
        SqlConnection conn,
        List<PositionResult> positions)
    {
        var table = new DataTable();
        table.Columns.Add("Portfolio",    typeof(string));
        table.Columns.Add("Instrument",   typeof(string));
        table.Columns.Add("NetQuantity",  typeof(decimal));
        table.Columns.Add("MarketValue",  typeof(decimal));
        table.Columns.Add("PnL",          typeof(decimal));
        table.Columns.Add("CalculatedAt", typeof(DateTime));
        table.Columns.Add("RunId",        typeof(string));

        foreach (var p in positions)
            table.Rows.Add(p.Portfolio, p.Instrument, p.NetQuantity,
                           p.MarketValue, p.PnL, p.CalculatedAt, p.RunId);

        using var bulk = new SqlBulkCopy(conn)
        {
            DestinationTableName = "dbo.Positions",
            BatchSize            = 500,
            BulkCopyTimeout      = 120
        };

        foreach (DataColumn col in table.Columns)
            bulk.ColumnMappings.Add(col.ColumnName, col.ColumnName);

        await bulk.WriteToServerAsync(table);

        _logger.LogInformation("SqlBulkCopy wrote {Count} rows to dbo.Positions", positions.Count);
        return positions.Count;
    }

    /// <summary>
    /// Writes a run log entry with full structured error info.
    /// ErrorMessage is JSON-serialized with exception type, message, stack summary,
    /// failed stage, blob path, and timestamp — optimised for AI reasoning.
    /// Uses MERGE (upsert) so Durable Function retries don't cause duplicate key errors.
    /// </summary>
    private async Task WriteRunLogAsync(
        SqlConnection conn,
        string runId,
        string status,
        int rowsInserted,
        TimeSpan duration,
        string? errorMessage = null)
    {
        const string sql = """
            MERGE dbo.ProcessingRunLog AS target
            USING (SELECT @RunId AS RunId) AS source ON target.RunId = source.RunId
            WHEN MATCHED THEN
                UPDATE SET
                    Status       = @Status,
                    RowsInserted = @RowsInserted,
                    DurationMs   = @DurationMs,
                    CompletedAt  = @CompletedAt,
                    ErrorMessage = @ErrorMessage
            WHEN NOT MATCHED THEN
                INSERT (RunId, Status, RowsInserted, DurationMs, CompletedAt, ErrorMessage)
                VALUES (@RunId, @Status, @RowsInserted, @DurationMs, @CompletedAt, @ErrorMessage);
            """;

        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@RunId",        runId);
        cmd.Parameters.AddWithValue("@Status",       status);
        cmd.Parameters.AddWithValue("@RowsInserted", rowsInserted);
        cmd.Parameters.AddWithValue("@DurationMs",   (int)duration.TotalMilliseconds);
        cmd.Parameters.AddWithValue("@CompletedAt",  DateTime.UtcNow);
        cmd.Parameters.AddWithValue("@ErrorMessage", (object?)errorMessage ?? DBNull.Value);

        await cmd.ExecuteNonQueryAsync();

        _logger.LogInformation("Run log written. RunId={RunId} Status={Status}", runId, status);
    }
}
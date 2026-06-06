using Microsoft.Azure.Functions.Worker;
using Microsoft.Data.SqlClient;
using Microsoft.DurableTask;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using PositionProcessor.Activities;
using PositionProcessor.Models;
using System.Text.Json;

namespace PositionProcessor;

public class PositionOrchestrator
{
    private readonly string _connectionString;

    public PositionOrchestrator(IConfiguration config)
    {
        _connectionString = config["SqlConnectionString"]!;
    }

    [Function(nameof(PositionOrchestrator))]
    public async Task RunOrchestrator(
        [OrchestrationTrigger] TaskOrchestrationContext context)
    {
        var logger  = context.CreateReplaySafeLogger(nameof(PositionOrchestrator));
        var request = context.GetInput<ProcessingRequest>()!;

        logger.LogInformation("Orchestrator started. RunId={RunId} BlobPath={BlobPath}",
            request.RunId, request.BlobPath);

        List<TradeRecord>? trades       = null;
        string             status       = "SUCCESS";
        string?            errorMessage = null;

        try
        {
            trades = await context.CallActivityAsync<List<TradeRecord>>(
                nameof(LoadTradesActivity), request);
        }
        catch (TaskFailedException ex)
        {
            status       = "FAILED";
            errorMessage = ex.Message;

            logger.LogError(ex,
                "Orchestrator failed. RunId={RunId} Stage={Stage}",
                request.RunId, ex.TaskName);

            // Only write to dbo.ApplicationErrors for genuine errors
            // SimulateFailure throws InvalidOperationException with fabricated JSON —
            // we detect it by checking if the message is valid JSON
            if (!request.SimulateFailure)
            {
                await WriteApplicationErrorAsync(
                    request.RunId,
                    "PositionProcessor",
                    BuildErrorJson(request, ex, ex.TaskName),
                    ex.StackTrace);
            }
        }

        // Always write to dbo.ProcessingRunLog — success or failure, real or simulated
        try
        {
            await context.CallActivityAsync(
                nameof(InsertPositionsActivity),
                new InsertInput(
                    Trades:       trades,
                    RunId:        request.RunId,
                    Status:       status,
                    ErrorMessage: errorMessage));
        }
        catch (TaskFailedException logEx)
        {
            logger.LogWarning(logEx,
                "Failed to write run log to SQL. RunId={RunId}", request.RunId);
        }
    }

    private async Task WriteApplicationErrorAsync(
        string runId,
        string serviceName,
        string errorMessage,
        string? stackTrace)
    {
        const string sql = """
            INSERT INTO dbo.ApplicationErrors
                (ServiceName, ErrorMessage, StackTrace, RunId, CreatedAt)
            VALUES
                (@ServiceName, @ErrorMessage, @StackTrace, @RunId, @CreatedAt)
            """;

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();

        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@ServiceName",  serviceName);
        cmd.Parameters.AddWithValue("@ErrorMessage", errorMessage);
        cmd.Parameters.AddWithValue("@StackTrace",   (object?)stackTrace ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@RunId",        runId);
        cmd.Parameters.AddWithValue("@CreatedAt",    DateTime.UtcNow);

        await cmd.ExecuteNonQueryAsync();
    }

    private static string BuildErrorJson(ProcessingRequest request, Exception ex, string stage)
    {
        var detail = new
        {
            FailedAt      = DateTime.UtcNow,
            RunId         = request.RunId,
            BlobPath      = request.BlobPath,
            BusinessDate  = request.BusinessDate,
            Stage         = stage,
            ExceptionType = ex.GetType().FullName,
            Message       = ex.Message,
            FullChain     = GetExceptionChain(ex),
        };
        return JsonSerializer.Serialize(detail);
    }

    private static string[] GetExceptionChain(Exception ex)
    {
        var chain = new List<string>();
        for (var cur = ex; cur != null; cur = cur.InnerException)
            chain.Add($"{cur.GetType().Name}: {cur.Message}");
        return chain.ToArray();
    }
}
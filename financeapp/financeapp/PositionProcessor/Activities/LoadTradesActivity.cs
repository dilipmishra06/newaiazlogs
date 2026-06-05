using Azure;
using Azure.Storage.Blobs;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using PositionProcessor.Models;
using System.Data;
using System.Text.Json;

namespace PositionProcessor.Activities;

/// <summary>
/// Loads trade CSV from Azure Blob Storage.
///
/// App Insights tracking:
///   - Blob download logged as an outbound dependency automatically
///     by the Azure SDK instrumentation (Azure.Storage.Blobs telemetry)
///   - Duration of the activity appears in App Insights as a dependency
///   - RequestFailedException details are logged with Status and ErrorCode
///     to improve RCA accuracy in AIOps
///
/// Dev Error Injection
/// ───────────────────
/// Set app setting  InjectDevError=true  to skip the real blob download
/// and instead write one of 5 realistic failure scenarios directly to
/// dbo.ApplicationErrors (mirroring what the orchestrator catch block
/// writes on a real failure).
///
/// The 5 injected error scenarios span the key failure domains this system
/// is likely to encounter in production:
///
///   1. ADF pipeline copy activity timeout feeding upstream blob data
///   2. Storage account public-access firewall blocking the download
///   3. SQL Managed Identity login failure after infra redeploy
///   4. VNet/NSG rule blocking outbound to the private SQL endpoint
///   5. Malformed CSV — non-numeric price field causing FormatException
///
/// Toggle via Azure Portal → Function App → Configuration → App Settings:
///   InjectDevError  =  true   (inject a random error instead of real run)
///   InjectDevError  =  false  (normal production behaviour — default)
/// </summary>
public class LoadTradesActivity
{
    private readonly BlobServiceClient _blobClient;
    private readonly ILogger<LoadTradesActivity> _logger;
    private readonly string _connectionString;
    private readonly bool _injectDevError;

    // ── 5 realistic injected error scenarios ─────────────────────────────────
    //
    // Each entry is the full structured JSON that the orchestrator catch block
    // would normally write into dbo.ApplicationErrors.ErrorMessage.
    // They are written verbatim so the AIOps ErrorPoller sees them as real
    // failures and exercises the full RAG → Claude → Teams pipeline.
    //
    // Scenarios:
    //   [0]  ADF copy activity timed out — upstream blob data never arrived
    //   [1]  Storage account firewall blocked download (403 AuthorizationFailure)
    //   [2]  SQL Managed Identity login failed after server was recreated
    //   [3]  VNet NSG outbound rule blocked TCP 1433 to SQL private endpoint
    //   [4]  CSV FormatException — Price field contains "N/A" instead of decimal

    private static readonly IReadOnlyList<(string Message, string StackTrace)> DevErrors =
    [
        // ── Scenario 0: ADF upstream pipeline timeout ────────────────────────
        (
            Message: JsonSerializer.Serialize(new
            {
                FailedAt      = DateTime.UtcNow.ToString("o"),
                Stage         = "LoadTradesActivity",
                ExceptionType = "AdfPipelineTimeoutException",
                Message       = "ADF pipeline 'CopyTradesToBlob' did not complete within the 3600s timeout. " +
                                "The blob trades/2026-06-05/positions.csv was not present at trigger time. " +
                                "Pipeline run ID: adf-run-7f3e2a1b-4c9d-4e8f-b3a2-1d9e7c6f5b4a. " +
                                "Last observed activity: 'Copy trades from on-prem SQL to ADLS' status=InProgress.",
                FullChain     = new[]
                {
                    "AdfPipelineTimeoutException: ADF pipeline 'CopyTradesToBlob' did not complete within the 3600s timeout.",
                    "Caused by: Azure.RequestFailedException: BlobErrorCode=BlobNotFound (HTTP 404) — " +
                    "The specified blob does not exist. ContainerName=trades BlobName=2026-06-05/positions.csv",
                },
                BlobPath      = "trades/2026-06-05/positions.csv",
                BusinessDate  = DateTime.UtcNow.Date.AddDays(-1).ToString("o"),
            }),
            StackTrace:
                "at PositionProcessor.Activities.LoadTradesActivity.Run(String blobPath)\n" +
                "at PositionProcessor.Orchestrators.PositionOrchestrator.RunOrchestrator(TaskOrchestrationContext ctx)"
        ),

        // ── Scenario 1: Storage account firewall 403 ─────────────────────────
        (
            Message: JsonSerializer.Serialize(new
            {
                FailedAt      = DateTime.UtcNow.ToString("o"),
                Stage         = "LoadTradesActivity",
                ExceptionType = "RequestFailedException",
                Message       = "BlobAccessError | Status=403 | ErrorCode=AuthorizationFailure | " +
                                "This request is not authorized to perform this operation using this permission. " +
                                "RequestId=4f2a1b3c-5d6e-7f8a-9b0c-1d2e3f4a5b6c AccountName=stfinancepoc01 " +
                                "ContainerName=trades BlobName=2026-06-05/positions.csv. " +
                                "Storage account public network access is disabled and the private endpoint " +
                                "pe-storage-poc01 may not be resolving correctly from snet-functions.",
                FullChain     = new[]
                {
                    "RequestFailedException: BlobAccessError | Status=403 | ErrorCode=AuthorizationFailure",
                    "Azure.RequestFailedException: The remote server returned an error: (403) Forbidden.",
                    "System.Net.WebException: The remote server returned an error: (403) Forbidden.",
                },
                BlobPath      = "trades/2026-06-05/positions.csv",
                BusinessDate  = DateTime.UtcNow.Date.AddDays(-1).ToString("o"),
            }),
            StackTrace:
                "at Azure.Storage.Blobs.BlobClient.DownloadContentAsync()\n" +
                "at PositionProcessor.Activities.LoadTradesActivity.Run(String blobPath)\n" +
                "at PositionProcessor.Orchestrators.PositionOrchestrator.RunOrchestrator(TaskOrchestrationContext ctx)"
        ),

        // ── Scenario 2: SQL Managed Identity login failure ────────────────────
        (
            Message: JsonSerializer.Serialize(new
            {
                FailedAt      = DateTime.UtcNow.ToString("o"),
                Stage         = "InsertPositionsActivity",
                ExceptionType = "SqlException",
                Message       = "Login failed for user 'id-positionprocessor'. " +
                                "The server principal 'id-positionprocessor' is not able to access the database " +
                                "'FinanceDb' under the current security context. " +
                                "Server=tcp:sql-finance-poc01.database.windows.net,1433 Database=FinanceDb " +
                                "Authentication=Active Directory Default. " +
                                "ErrorNumber=18456 State=1 Class=14. " +
                                "This typically occurs when the Managed Identity SQL user was not re-created " +
                                "after an Azure SQL server recreation.",
                FullChain     = new[]
                {
                    "Microsoft.Data.SqlClient.SqlException (0x80131904): " +
                    "Login failed for user 'id-positionprocessor'.",
                    "System.Data.Common.DbException: Login failed for user 'id-positionprocessor'.",
                },
                BlobPath      = "trades/2026-06-05/positions.csv",
                BusinessDate  = DateTime.UtcNow.Date.AddDays(-1).ToString("o"),
            }),
            StackTrace:
                "at Microsoft.Data.SqlClient.SqlConnection.OpenAsync(CancellationToken)\n" +
                "at PositionProcessor.Activities.InsertPositionsActivity.Run(InsertInput input)\n" +
                "at PositionProcessor.Orchestrators.PositionOrchestrator.RunOrchestrator(TaskOrchestrationContext ctx)"
        ),

        // ── Scenario 3: NSG blocking TCP 1433 to SQL private endpoint ─────────
        (
            Message: JsonSerializer.Serialize(new
            {
                FailedAt      = DateTime.UtcNow.ToString("o"),
                Stage         = "InsertPositionsActivity",
                ExceptionType = "SqlException",
                Message       = "A network-related or instance-specific error occurred while establishing a " +
                                "connection to SQL Server. " +
                                "Server=tcp:sql-finance-poc01.database.windows.net,1433. " +
                                "The server was not found or was not accessible. " +
                                "TCP Provider, error 0 - No connection could be made because the target machine actively refused it. " +
                                "This may indicate an NSG outbound rule on snet-functions is blocking TCP 1433 " +
                                "to the private endpoint address 10.0.2.5 (pe-sql-poc01) in snet-pe. " +
                                "ErrorNumber=53 State=0 Class=20.",
                FullChain     = new[]
                {
                    "Microsoft.Data.SqlClient.SqlException (0x80131904): " +
                    "A network-related or instance-specific error occurred while establishing a connection to SQL Server.",
                    "System.Net.Sockets.SocketException (10061): " +
                    "No connection could be made because the target machine actively refused it. 10.0.2.5:1433",
                },
                BlobPath      = "trades/2026-06-05/positions.csv",
                BusinessDate  = DateTime.UtcNow.Date.AddDays(-1).ToString("o"),
            }),
            StackTrace:
                "at System.Data.SqlClient.SqlInternalConnectionTds.AttemptOneLogin()\n" +
                "at Microsoft.Data.SqlClient.SqlConnection.OpenAsync(CancellationToken)\n" +
                "at PositionProcessor.Activities.InsertPositionsActivity.Run(InsertInput input)\n" +
                "at PositionProcessor.Orchestrators.PositionOrchestrator.RunOrchestrator(TaskOrchestrationContext ctx)"
        ),

        // ── Scenario 4: CSV FormatException on bad price field ────────────────
        (
            Message: JsonSerializer.Serialize(new
            {
                FailedAt      = DateTime.UtcNow.ToString("o"),
                Stage         = "LoadTradesActivity",
                ExceptionType = "FormatException",
                Message       = "Input string was not in a correct format. " +
                                "Failed to parse Price field on row T047 of trades/2026-06-05/positions.csv. " +
                                "Raw value: 'N/A'. Expected a valid decimal number. " +
                                "decimal.Parse(cols[4].Trim()) threw FormatException. " +
                                "The upstream ADF pipeline may have written a placeholder value for a missing market price.",
                FullChain     = new[]
                {
                    "System.FormatException: Input string was not in a correct format.",
                    "at System.Number.ThrowOverflowOrFormatException(ParsingStatus status, ReadOnlySpan`1 value, TypeCode type)",
                    "at PositionProcessor.Activities.LoadTradesActivity.ParseCsv(String csv) line 47",
                },
                BlobPath      = "trades/2026-06-05/positions.csv",
                BusinessDate  = DateTime.UtcNow.Date.AddDays(-1).ToString("o"),
            }),
            StackTrace:
                "at PositionProcessor.Activities.LoadTradesActivity.ParseCsv(String csv)\n" +
                "at PositionProcessor.Activities.LoadTradesActivity.Run(String blobPath)\n" +
                "at PositionProcessor.Orchestrators.PositionOrchestrator.RunOrchestrator(TaskOrchestrationContext ctx)"
        ),
    ];

    public LoadTradesActivity(
        BlobServiceClient blobClient,
        IConfiguration config,
        ILogger<LoadTradesActivity> logger)
    {
        _blobClient     = blobClient;
        _logger         = logger;
        _connectionString = config["SqlConnectionString"]!;
        _injectDevError = string.Equals(
            config["InjectDevError"], "true",
            StringComparison.OrdinalIgnoreCase);
    }

    [Function(nameof(LoadTradesActivity))]
    public async Task Run([ActivityTrigger] object? input = null)
    {
        // ── Dev error injection ───────────────────────────────────────────────
        // When InjectDevError=true, skip the real blob download and write a
        // random error scenario directly to dbo.ApplicationErrors so the full
        // AIOps pipeline (RAG → Claude → Teams) exercises a realistic failure.
       
            await InjectRandomErrorAsync();
            // Throw so the orchestrator records FAILED in ProcessingRunLog too.
            throw new InvalidOperationException(
                "Dev error injection active (InjectDevError=true). " +
                "A synthetic error has been written to dbo.ApplicationErrors. " +
                "Set InjectDevError=false to restore normal operation.");
        
    }

    // ── Dev helpers ───────────────────────────────────────────────────────────

    /// <summary>
    /// Picks a random error scenario from DevErrors and writes it to
    /// dbo.ApplicationErrors so the AIOps ErrorPoller picks it up on its
    /// next 2-minute poll.
    /// </summary>
    private async Task InjectRandomErrorAsync()
    {
        var idx      = Random.Shared.Next(DevErrors.Count);
        var scenario = DevErrors[idx];

        _logger.LogWarning(
            "DEV ERROR INJECTION: inserting scenario {Index} into dbo.ApplicationErrors. " +
            "Set InjectDevError=false to disable.",
            idx);

        const string sql = """
            INSERT INTO dbo.ApplicationErrors
                (ServiceName, ErrorMessage, StackTrace, RunId, CreatedAt)
            VALUES
                (@ServiceName, @ErrorMessage, @StackTrace, @RunId, @CreatedAt)
            """;

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();

        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@ServiceName",  "PositionProcessor");
        cmd.Parameters.AddWithValue("@ErrorMessage", scenario.Message);
        cmd.Parameters.AddWithValue("@StackTrace",   scenario.StackTrace);
        cmd.Parameters.AddWithValue("@RunId",        $"DEV-{DateTime.UtcNow:yyyyMMdd}-INJECT{idx:D2}");
        cmd.Parameters.AddWithValue("@CreatedAt",    DateTime.UtcNow);

        await cmd.ExecuteNonQueryAsync();

        _logger.LogWarning(
            "DEV ERROR INJECTION: scenario {Index} written to dbo.ApplicationErrors.", idx);
    }
}
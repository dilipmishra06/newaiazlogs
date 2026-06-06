using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using PositionProcessor.Models;
using System.Text.Json;

namespace PositionProcessor.Activities;

public class LoadTradesActivity
{
    private readonly ILogger<LoadTradesActivity> _logger;

    private static readonly IReadOnlyList<(string Message, string StackTrace)> FabricatedErrors =
    [
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
                BlobPath     = "trades/2026-06-05/positions.csv",
                BusinessDate = DateTime.UtcNow.Date.AddDays(-1).ToString("o"),
            }),
            StackTrace:
                "at PositionProcessor.Activities.LoadTradesActivity.Run(String blobPath)\n" +
                "at PositionProcessor.Orchestrators.PositionOrchestrator.RunOrchestrator(TaskOrchestrationContext ctx)"
        ),
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
                BlobPath     = "trades/2026-06-05/positions.csv",
                BusinessDate = DateTime.UtcNow.Date.AddDays(-1).ToString("o"),
            }),
            StackTrace:
                "at Azure.Storage.Blobs.BlobClient.DownloadContentAsync()\n" +
                "at PositionProcessor.Activities.LoadTradesActivity.Run(String blobPath)\n" +
                "at PositionProcessor.Orchestrators.PositionOrchestrator.RunOrchestrator(TaskOrchestrationContext ctx)"
        ),
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
                BlobPath     = "trades/2026-06-05/positions.csv",
                BusinessDate = DateTime.UtcNow.Date.AddDays(-1).ToString("o"),
            }),
            StackTrace:
                "at Microsoft.Data.SqlClient.SqlConnection.OpenAsync(CancellationToken)\n" +
                "at PositionProcessor.Activities.InsertPositionsActivity.Run(InsertInput input)\n" +
                "at PositionProcessor.Orchestrators.PositionOrchestrator.RunOrchestrator(TaskOrchestrationContext ctx)"
        ),
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
                BlobPath     = "trades/2026-06-05/positions.csv",
                BusinessDate = DateTime.UtcNow.Date.AddDays(-1).ToString("o"),
            }),
            StackTrace:
                "at System.Data.SqlClient.SqlInternalConnectionTds.AttemptOneLogin()\n" +
                "at Microsoft.Data.SqlClient.SqlConnection.OpenAsync(CancellationToken)\n" +
                "at PositionProcessor.Activities.InsertPositionsActivity.Run(InsertInput input)\n" +
                "at PositionProcessor.Orchestrators.PositionOrchestrator.RunOrchestrator(TaskOrchestrationContext ctx)"
        ),
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
                BlobPath     = "trades/2026-06-05/positions.csv",
                BusinessDate = DateTime.UtcNow.Date.AddDays(-1).ToString("o"),
            }),
            StackTrace:
                "at PositionProcessor.Activities.LoadTradesActivity.ParseCsv(String csv)\n" +
                "at PositionProcessor.Activities.LoadTradesActivity.Run(String blobPath)\n" +
                "at PositionProcessor.Orchestrators.PositionOrchestrator.RunOrchestrator(TaskOrchestrationContext ctx)"
        ),
    ];

    public LoadTradesActivity(ILogger<LoadTradesActivity> logger)
    {
        _logger = logger;
    }

    [Function(nameof(LoadTradesActivity))]
    public Task<List<TradeRecord>> Run([ActivityTrigger] ProcessingRequest input)
    {
        if (input.SimulateFailure)
        {
            var idx      = Random.Shared.Next(FabricatedErrors.Count);
            var scenario = FabricatedErrors[idx];

            _logger.LogWarning(
                "LoadTradesActivity: simulating failure scenario {Index}.", idx);

            throw new InvalidOperationException(scenario.Message);
        }

        _logger.LogInformation("LoadTradesActivity: returning fabricated trades.");

        var trades = new List<TradeRecord>
        {
            new("T001", "PORT-A", "AAPL",  100, 189.50m, "BUY",  DateTime.UtcNow),
            new("T002", "PORT-A", "AAPL",   40, 191.00m, "SELL", DateTime.UtcNow),
            new("T003", "PORT-A", "MSFT",  200, 415.25m, "BUY",  DateTime.UtcNow),
            new("T004", "PORT-B", "GOOGL",  50, 172.10m, "BUY",  DateTime.UtcNow),
            new("T005", "PORT-B", "GOOGL",  10, 174.00m, "SELL", DateTime.UtcNow),
            new("T006", "PORT-B", "NVDA",   75, 875.00m, "BUY",  DateTime.UtcNow),
            new("T007", "PORT-C", "TSLA",  120, 245.30m, "BUY",  DateTime.UtcNow),
            new("T008", "PORT-C", "TSLA",   30, 248.00m, "SELL", DateTime.UtcNow),
        };

        return Task.FromResult(trades);
    }
}
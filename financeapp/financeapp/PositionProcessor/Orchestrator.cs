using Microsoft.Azure.Functions.Worker;
using Microsoft.DurableTask;
using Microsoft.Extensions.Logging;
using PositionProcessor.Activities;
using PositionProcessor.Models;
using System.Text.Json;

namespace PositionProcessor;

/// <summary>
/// Durable orchestrator — coordinates the two activities:
///   1. LoadTradesActivity     — reads CSV from blob, returns List&lt;TradeRecord&gt;
///   2. InsertPositionsActivity — calculates P&amp;L and BulkInserts to SQL
///
/// App Insights automatically tracks:
///   - Each activity as a dependency call with duration
///   - Any exceptions thrown inside activities
///   - The orchestration as a parent operation
///
/// On failure, a structured JSON error message is written to ProcessingRunLog
/// containing: stage, exception type, message, stack summary, blob path, and
/// business date — designed for AI-assisted root cause analysis.
/// </summary>
public class PositionOrchestrator
{
    [Function(nameof(PositionOrchestrator))]
    public static async Task<ProcessingResult> RunOrchestrator(
        [OrchestrationTrigger] TaskOrchestrationContext context)
    {
        var logger = context.CreateReplaySafeLogger(nameof(PositionOrchestrator));
        var request = context.GetInput<ProcessingRequest>()!;

        logger.LogInformation("Orchestrator started. RunId={RunId} BlobPath={BlobPath}",
            request.RunId, request.BlobPath);

        try
        {
            // Activity 1: Load trades from blob storage
            var trades = await context.CallActivityAsync<List<TradeRecord>>(
                nameof(LoadTradesActivity),
                request.BlobPath);

            logger.LogInformation("Loaded {Count} trades for RunId={RunId}",
                trades.Count, request.RunId);

            if (trades.Count == 0)
            {
                await context.CallActivityAsync(nameof(InsertPositionsActivity),
                    new InsertInput(null, request.RunId, "NO_DATA"));

                return new ProcessingResult(0, 0, true, "No trades found");
            }

            // Activity 2: Calculate P&L and bulk insert to SQL
            var inserted = await context.CallActivityAsync<int>(
                nameof(InsertPositionsActivity),
                new InsertInput(trades, request.RunId, "SUCCESS"));

            logger.LogInformation("Inserted {Count} positions. RunId={RunId}",
                inserted, request.RunId);

            return new ProcessingResult(trades.Count, inserted, true, null);
        }
        catch (Exception ex)
        {
            var durableEx = ex as TaskFailedException;

            var realMessage = durableEx != null
                ? ExtractRealMessage(durableEx.Message)
                : ex.Message;

            var stage = durableEx?.TaskName ?? "Orchestrator";

            var errorDetail = new
            {
                FailedAt = DateTime.UtcNow,
                RunId = request.RunId,
                BlobPath = request.BlobPath,
                BusinessDate = request.BusinessDate,
                Stage = stage,
                ExceptionType = ex.GetType().FullName,
                Message = realMessage,
                StackTrace = ex.StackTrace,
                FullChain = GetExceptionChain(ex),
                OrchestrationInstanceId = context.InstanceId
            };

            var errorMessage = JsonSerializer.Serialize(errorDetail);

            logger.LogError(
                ex,
                "Orchestrator failed. RunId={RunId} Stage={Stage} Error={Error}",
                request.RunId,
                stage,
                realMessage);

            try
            {
                await context.CallActivityAsync(
                    nameof(InsertPositionsActivity),
                    new InsertInput(
                        Trades: null,
                        RunId: request.RunId,
                        Status: "FAILED",
                        ErrorMessage: errorMessage));
            }
            catch (Exception logEx)
            {
                logger.LogWarning(
                    logEx,
                    "Failed to write error log to SQL. RunId={RunId}",
                    request.RunId);
            }

            return new ProcessingResult(
            TradesLoaded: 0,
            PositionsInserted: 0,
            Success: false,
            Error: errorMessage);
        }
    }

    // Extracts the real error from Durable's wrapper message:
    // "Task 'LoadTradesActivity' (#0) failed with an unhandled exception: <REAL ERROR HERE>"
    private static string ExtractRealMessage(string durableMessage)
    {
        const string marker = "failed with an unhandled exception: ";
        var idx = durableMessage.IndexOf(marker, StringComparison.OrdinalIgnoreCase);
        return idx >= 0
            ? durableMessage[(idx + marker.Length)..].Trim()
            : durableMessage;
    }

    private static string[] GetExceptionChain(Exception ex)
    {
        var chain = new List<string>();
        var current = ex;
        while (current != null)
        {
            chain.Add($"{current.GetType().Name}: {current.Message}");
            current = current.InnerException;
        }
        return chain.ToArray();
    }
}
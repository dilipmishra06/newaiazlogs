using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.DurableTask.Client;
using Microsoft.Extensions.Logging;
using PositionProcessor.Models;

namespace PositionProcessor;

/// <summary>
/// HTTP starter endpoint that creates a new orchestration instance.
/// Called by the Scheduler function app or manually via PowerShell/curl.
///
/// URL: POST http://localhost:7071/api/orchestrators/{orchestratorName}
///
/// Example body:
/// {
///   "BlobPath":     "trades/2026-05-30/positions.csv",
///   "RunId":        "RUN-20260530-001",
///   "BusinessDate": "2026-05-30T00:00:00Z"
/// }
///
/// Returns the standard Durable management URLs (statusQueryGetUri, etc.)
/// so the caller can poll for completion.
/// </summary>
public class HttpStarter
{
    private readonly ILogger<HttpStarter> _logger;

    public HttpStarter(ILogger<HttpStarter> logger) => _logger = logger;

    [Function("HttpStarter")]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Function, "post",
            Route = "orchestrators/{orchestratorName}")] HttpRequestData req,
        [DurableClient] DurableTaskClient client,
        string orchestratorName)
    {
        var input = await req.ReadFromJsonAsync<ProcessingRequest>();

        _logger.LogInformation(
            "Starting orchestration. Name={Name} RunId={RunId} BlobPath={BlobPath}",
            orchestratorName, input?.RunId, input?.BlobPath);

        string instanceId = await client.ScheduleNewOrchestrationInstanceAsync(
            orchestratorName, input);

        _logger.LogInformation("Scheduled orchestration with InstanceId={InstanceId}", instanceId);

        return await client.CreateCheckStatusResponseAsync(req, instanceId);
    }
}
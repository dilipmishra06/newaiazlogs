using System;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Net.Http;
using System.Net.Http.Json;
using System.Threading.Tasks;

namespace Scheduler;

/// <summary>
/// Two triggers:
///   1. Timer (daily 06:00 UTC) — normal production schedule
///   2. HTTP POST               — manual trigger for reruns / catch-up
///
/// Both call the PositionProcessor orchestrator via its HTTP starter endpoint.
///
/// WHY A SEPARATE SCHEDULER APP?
///   Separation of concerns — the scheduler owns the schedule,
///   the processor owns the business logic.
///   Lets you redeploy, disable, or adjust the schedule independently.
/// </summary>
public class DailyPositionScheduler
{
    private readonly HttpClient _http;
    private readonly IConfiguration _config;
    private readonly ILogger<DailyPositionScheduler> _logger;

    public DailyPositionScheduler(
        IHttpClientFactory httpFactory,
        IConfiguration config,
        ILogger<DailyPositionScheduler> logger)
    {
        _http   = httpFactory.CreateClient("orchestrator");
        _config = config;
        _logger = logger;
    }

    /// <summary>
    /// Fires every day at 06:00 UTC.
    /// Builds the blob path for yesterday's trades and triggers the orchestrator.
    /// </summary>
    [Function("DailyPositionScheduler")]
    public async Task RunTimer([TimerTrigger("0 0 6 * * *")] TimerInfo timer)
    {
        var businessDate = DateTime.UtcNow.Date.AddDays(-1);
        _logger.LogInformation("Timer fired. Triggering positions for BusinessDate={Date}", businessDate);
        await TriggerOrchestrator(businessDate);
    }

    /// <summary>
    /// HTTP POST /api/trigger?date=2026-05-21
    /// Allows manual reruns without redeployment.
    /// </summary>
    [Function("ManualTrigger")]
    public async Task<HttpResponseData> RunHttp(
        [HttpTrigger(AuthorizationLevel.Function, "post", Route = "trigger")] HttpRequestData req)
    {
        // Optional date override in query string
        var dateStr      = req.Query["date"];
        var businessDate = string.IsNullOrEmpty(dateStr)
            ? DateTime.UtcNow.Date.AddDays(-1)
            : DateTime.Parse(dateStr);

        _logger.LogInformation("Manual trigger. BusinessDate={Date}", businessDate);

        var runId = await TriggerOrchestrator(businessDate);

        var response = req.CreateResponse(System.Net.HttpStatusCode.Accepted);
        await response.WriteAsJsonAsync(new { runId, businessDate });
        return response;
    }

    private async Task<string> TriggerOrchestrator(DateTime businessDate)
    {
        var runId    = $"RUN-{businessDate:yyyyMMdd}-{Guid.NewGuid():N[..8]}";
        var blobPath = $"trades/{businessDate:yyyy-MM-dd}/positions.csv";

        var payload = new
        {
            BlobPath     = blobPath,
            RunId        = runId,
            BusinessDate = businessDate
        };

        var starterUrl = _config["OrchestratorStarterUrl"];

        _logger.LogInformation("POSTing to orchestrator. RunId={RunId} Url={Url}", runId, starterUrl);

        var resp = await _http.PostAsJsonAsync(starterUrl, payload);
        resp.EnsureSuccessStatusCode();

        var body = await resp.Content.ReadAsStringAsync();
        _logger.LogInformation("Orchestrator accepted. RunId={RunId} Response={Body}", runId, body);

        return runId;
    }
}

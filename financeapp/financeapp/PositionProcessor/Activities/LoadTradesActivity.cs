using Azure;
using Azure.Storage.Blobs;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using PositionProcessor.Models;

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
/// </summary>
public class LoadTradesActivity
{
    private readonly BlobServiceClient _blobClient;
    private readonly ILogger<LoadTradesActivity> _logger;

    public LoadTradesActivity(
        BlobServiceClient blobClient,
        ILogger<LoadTradesActivity> logger)
    {
        _blobClient = blobClient;
        _logger = logger;
    }

    [Function(nameof(LoadTradesActivity))]
    public async Task<List<TradeRecord>> Run([ActivityTrigger] string blobPath)
    {
        // blobPath format: "containername/folder/file.csv"
        var parts = blobPath.Split('/', 2);

        if (parts.Length != 2)
        {
            throw new ArgumentException(
                $"Invalid blob path format: '{blobPath}'. Expected 'container/blobname'");
        }

        var container = parts[0];
        var blobName = parts[1];

        _logger.LogInformation(
            "Loading trades from blob. Container={Container}, Blob={Blob}",
            container,
            blobName);

        try
        {
            var containerClient = _blobClient.GetBlobContainerClient(container);
            var blobClient = containerClient.GetBlobClient(blobName);

            _logger.LogInformation(
                "Starting blob download. BlobPath={BlobPath}",
                blobPath);

            var response = await blobClient.DownloadContentAsync();

            var content = response.Value.Content.ToString();

            _logger.LogInformation(
                "Blob download completed successfully. BlobPath={BlobPath}",
                blobPath);

            var trades = ParseCsv(content);

            _logger.LogInformation(
                "Parsed {Count} trade records from {BlobPath}",
                trades.Count,
                blobPath);

            return trades;
        }
        catch (RequestFailedException ex)
        {
            throw new Exception(
                $"BlobAccessError | Status={ex.Status} | ErrorCode={ex.ErrorCode} | Message={ex.Message}",
                ex);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Unexpected error while loading blob. BlobPath={BlobPath}",
                blobPath);

            throw;
        }
    }

    private static List<TradeRecord> ParseCsv(string csv)
    {
        var trades = new List<TradeRecord>();

        var lines = csv.Split(
            '\n',
            StringSplitOptions.RemoveEmptyEntries);

        // Skip header row
        foreach (var line in lines.Skip(1))
        {
            var cols = line.Split(',');

            if (cols.Length < 7)
            {
                continue;
            }

            trades.Add(new TradeRecord(
                TradeId: cols[0].Trim(),
                Portfolio: cols[1].Trim(),
                Instrument: cols[2].Trim(),
                Quantity: decimal.Parse(cols[3].Trim()),
                Price: decimal.Parse(cols[4].Trim()),
                Direction: cols[5].Trim(),
                TradeDate: DateTime.Parse(cols[6].Trim())
            ));
        }

        return trades;
    }
}
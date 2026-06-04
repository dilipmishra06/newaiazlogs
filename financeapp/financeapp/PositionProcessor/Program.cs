using Azure.Storage.Blobs;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults()
    .ConfigureServices((ctx, services) =>
    {
        var config = ctx.Configuration;

        // App Insights — automatically instruments:
        //   - HttpClient outbound calls
        //   - SqlClient queries, BulkCopy timing, SQL errors
        //   - Azure SDK blob operations
        //   - Durable Function orchestration spans
        services.AddApplicationInsightsTelemetryWorkerService();
        services.ConfigureFunctionsApplicationInsights();

        // BlobServiceClient — uses connection string locally, Managed Identity in production.
        // Set StorageConnectionString in local.settings.json for local dev.
        // Leave it empty/absent in production and use StorageAccountUrl + Managed Identity.
        services.AddSingleton(_ =>
        {
            var connectionString = config["StorageConnectionString"];

            if (!string.IsNullOrEmpty(connectionString))
            {
                // Local dev — account key authentication
                return new BlobServiceClient(connectionString);
            }

            // Production — Managed Identity (Function App must have
            // Storage Blob Data Reader role on the storage account)
            var storageUrl = config["StorageAccountUrl"]!;
            return new BlobServiceClient(
                new Uri(storageUrl),
                new Azure.Identity.DefaultAzureCredential());
        });

        // Activities registered for DI
        services.AddScoped<PositionProcessor.Activities.LoadTradesActivity>();
        services.AddScoped<PositionProcessor.Activities.InsertPositionsActivity>();
    })
    .Build();

await host.RunAsync();
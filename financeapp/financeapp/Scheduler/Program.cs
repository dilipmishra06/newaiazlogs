using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults()
    .ConfigureServices((ctx, services) =>
    {
        // Named HttpClient pointing at the PositionProcessor starter endpoint
        // Timeout 30s — orchestrator HTTP starter responds quickly
        services.AddHttpClient("orchestrator", client =>
        {
            client.Timeout = System.TimeSpan.FromSeconds(30);
        });
    })
    .Build();

await host.RunAsync();

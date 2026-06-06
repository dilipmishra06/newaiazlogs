$body = @{
    BlobPath        = "trades/30-05-2026/positions.csv"
    RunId           = "RUN-019"
    BusinessDate    = "2026-05-30T00:00:00Z"
    SimulateFailure = $true
} | ConvertTo-Json

Invoke-RestMethod -Method POST `
    -Uri "http://localhost:7071/api/orchestrators/PositionOrchestrator" `
    -ContentType "application/json" `
    -Body $body
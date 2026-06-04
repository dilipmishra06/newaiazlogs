$body = @{
    BlobPath     = "trades/30-05-2026/positions.csv"
    RunId        = "RUN-015"
    BusinessDate = "2026-05-30T00:00:00Z"
} | ConvertTo-Json

Invoke-RestMethod -Method POST `
    -Uri "http://localhost:8090/api/orchestrators/PositionOrchestrator" `
    -ContentType "application/json" `
    -Body $body
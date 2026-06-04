# Finance Position Processor — System Knowledge Base

service: PositionProcessor
doc_type: architecture
last_updated: 2026-06-02

---

## 1. System Overview

### 1.1 What it does

The Finance Position Processor is a daily batch system that loads trade CSV files from Azure Blob Storage, calculates net P&L positions per portfolio and instrument, and bulk-inserts results into Azure SQL Database (FinanceDb). It runs every business day at 06:00 UTC. All processing outcomes — success, failure, row counts, duration, and structured error details — are written to SQL tables that the AIOps monitoring pipeline reads every 2 minutes.

### 1.2 Two Function Apps

Two Function Apps work together:

- **func-positionprocessor-poc01** — Durable Functions orchestrator. Loads trades, calculates positions, bulk-inserts to SQL. Triggered by HTTP POST from the Scheduler.
- **func-scheduler-poc01** — Timer-triggered. Fires at 06:00 UTC, builds the blob path for yesterday's trades, and POSTs to the PositionProcessor HttpStarter endpoint.

Both apps run on the same App Service Plan (plan-finance-poc01, EP1 Premium, Windows). Both are VNet-integrated via snet-functions (10.0.1.0/24) with `vnet_route_all_enabled = true`, meaning all outbound traffic — including DNS — routes through the VNet and uses private endpoints.

### 1.3 Daily processing flow

Step 1: Scheduler timer fires at 06:00 UTC. Generates RunId format `RUN-YYYYMMDD-XXXXXXXX`. Builds blob path `trades/YYYY-MM-DD/positions.csv` for yesterday's date. POSTs `{ BlobPath, RunId, BusinessDate }` to PositionProcessor HttpStarter.

Step 2: HttpStarter receives POST, calls `DurableTaskClient.ScheduleNewOrchestrationInstanceAsync`, returns 202 with management URLs.

Step 3: PositionOrchestrator starts. Calls LoadTradesActivity with the blob path.

Step 4: LoadTradesActivity downloads the CSV from Blob Storage container `trades` using DefaultAzureCredential (Managed Identity). Parses rows into `List<TradeRecord>`. Returns the list to the orchestrator.

Step 5: Orchestrator receives trades. If count is zero, calls InsertPositionsActivity with `Status=NO_DATA`. Otherwise proceeds to Step 6.

Step 6: Orchestrator calls InsertPositionsActivity with the full trade list and `Status=SUCCESS`.

Step 7: InsertPositionsActivity calculates net positions (groups by Portfolio+Instrument, nets BUY minus SELL). Bulk-inserts to `dbo.Positions` via SqlBulkCopy. Writes run outcome to `dbo.ProcessingRunLog`. On any exception the orchestrator catches it, serialises a structured JSON error, and writes `Status=FAILED` to ProcessingRunLog.

---

## 2. PositionProcessor Function App

### 2.1 Runtime and identity

- Runtime: .NET 8 isolated worker, Windows, EP1 Premium plan
- VNet integration: snet-functions (10.0.1.0/24)
- User-Assigned Managed Identity: `id-positionprocessor`
- App setting `SqlConnectionString` references Key Vault secret via `@Microsoft.KeyVault(SecretUri=...)` — no password in config

### 2.2 LoadTradesActivity

Durable activity. Receives blob path string (format: `container/folder/file.csv`). Downloads CSV using `BlobServiceClient` authenticated via `DefaultAzureCredential`. Parses CSV into `List<TradeRecord>` — skips rows with fewer than 7 columns silently. Returns the list to the orchestrator.

**Common failure modes:**
- `RequestFailedException` with `BlobErrorCode.BlobNotFound` (HTTP 404) — trade file not yet uploaded. See RCA-FIN-002.
- `RequestFailedException` with DNS error / no such host — private endpoint DNS misconfiguration. See RCA-FIN-004.
- `FormatException` on `decimal.Parse` — corrupted CSV field (e.g. Price = "N/A"). See RCA-FIN-005.

### 2.3 InsertPositionsActivity

Durable activity. Receives `InsertInput` (trades list, RunId, Status, optional ErrorMessage). Calculates net positions grouped by Portfolio+Instrument. Creates a `DataTable` and calls `SqlBulkCopy.WriteToServerAsync` to `dbo.Positions`. Always writes a row to `dbo.ProcessingRunLog` via MERGE (upsert — safe for Durable retries).

Configuration: `BatchSize = 500`, `BulkCopyTimeout = 300` seconds.

**Common failure modes:**
- `SqlException: Login failed for user 'id-positionprocessor'` — Managed Identity not added as SQL user. See RCA-FIN-001.
- `SqlException: Timeout expired` — DTU exhaustion under high trade volume. See RCA-FIN-003.
- `SqlException: Deadlock` — concurrent run writing same RunId (should not happen; MERGE prevents it).

### 2.4 HttpStarter

HTTP trigger. Accepts POST from Scheduler with JSON body `{ BlobPath, RunId, BusinessDate }`. Deserialises to `ProcessingRequest`. Calls `DurableTaskClient.ScheduleNewOrchestrationInstanceAsync(nameof(PositionOrchestrator), request)`. Returns 202 Accepted with management URLs including `statusQueryGetUri`.

If the POST body cannot be deserialised, an unhandled exception is written to `dbo.ApplicationErrors` (bypasses the orchestrator's catch block).

### 2.5 PositionOrchestrator

Durable orchestrator. Wraps all activity calls in a top-level try/catch. On any exception it:
1. Extracts the real message from `TaskFailedException` wrapper (Durable embeds the activity error inside its own message string — InnerException is null by design).
2. Serialises a structured JSON error containing: `FailedAt`, `RunId`, `BlobPath`, `BusinessDate`, `Stage`, `ExceptionType`, `Message`, `FullChain[]`.
3. Calls InsertPositionsActivity with `Status=FAILED` and the JSON error string.
4. Returns `ProcessingResult(Success=false)`.

The JSON error lands in `dbo.ProcessingRunLog.ErrorMessage` — this is the primary data source for AIOps analysis.

---

## 3. Scheduler Function App

### 3.1 DailyPositionScheduler

Timer trigger. CRON: `0 0 6 * * *` (06:00 UTC daily). Calculates business date as `DateTime.UtcNow.Date.AddDays(-1)`. Builds blob path `trades/YYYY-MM-DD/positions.csv`. POSTs `ProcessingRequest` JSON to PositionProcessor HttpStarter URL (read from app setting `PositionProcessorUrl`).

If the Scheduler itself crashes (DI error, config missing), an unhandled exception is written to `dbo.ApplicationErrors` with `ServiceName = 'Scheduler'`.

### 3.2 ManualTrigger

HTTP POST `/api/trigger?date=YYYY-MM-DD`. Allows reruns for a specific business date without redeployment. Accepts optional `date` query parameter — defaults to yesterday if omitted. Same POSTing logic as DailyPositionScheduler. Used after blob file arrives late or after a manual fix.

---

## 4. Database Schema

### 4.1 dbo.Positions

Target of `SqlBulkCopy` from InsertPositionsActivity. Stores calculated net positions per run.

| Column | Type | Notes |
|---|---|---|
| Id | INT IDENTITY PK | |
| RunId | NVARCHAR(64) | Links to ProcessingRunLog |
| Portfolio | NVARCHAR(50) | e.g. EQUITY-US |
| Instrument | NVARCHAR(50) | e.g. AAPL |
| NetQuantity | DECIMAL(18,4) | Net of BUY minus SELL |
| MarketValue | DECIMAL(18,2) | NetQuantity × AvgPrice |
| PnL | DECIMAL(18,2) | Simplified: NetQty × AvgPx × 0.02 |
| CalculatedAt | DATETIME2 | Default GETUTCDATE() |

Indexes: `IX_Positions_RunId`, `IX_Positions_Portfolio` (Portfolio, Instrument), `IX_Positions_Calculated` (CalculatedAt DESC).

### 4.2 dbo.ProcessingRunLog

Audit trail written by InsertPositionsActivity after every run regardless of outcome. **This is the primary table the AIOps ErrorPoller monitors.** New rows with `Status = 'FAILED'` trigger RAG search and Claude analysis.

| Column | Type | Notes |
|---|---|---|
| Id | INT IDENTITY PK | Watermark key for AIOps polling |
| RunId | NVARCHAR(64) UNIQUE | Format: RUN-YYYYMMDD-XXXXXXXX |
| Status | NVARCHAR(20) | SUCCESS, FAILED, NO_DATA |
| RowsInserted | INT | 0 on failure |
| DurationMs | INT | Total orchestration duration |
| CompletedAt | DATETIME2 | Default GETUTCDATE() |
| ErrorMessage | NVARCHAR(MAX) NULL | Structured JSON on failure (see Section 8.2) |

Written via MERGE (upsert) keyed on RunId — safe for Durable Function replays and retries. Indexes: `IX_RunLog_Status`, `IX_RunLog_Completed`.

### 4.3 dbo.ApplicationErrors

Written by both Function Apps on unhandled exceptions that escape the orchestrator's try/catch. Secondary monitoring source for AIOps.

| Column | Type | Notes |
|---|---|---|
| ErrorId | INT IDENTITY PK | Watermark key for AIOps polling |
| ServiceName | NVARCHAR(100) | PositionProcessor or Scheduler |
| ErrorMessage | NVARCHAR(MAX) | Plain exception message |
| StackTrace | NVARCHAR(MAX) NULL | Full stack if available |
| RunId | NVARCHAR(64) NULL | Links to ProcessingRunLog if known |
| CreatedAt | DATETIME2 | Default GETUTCDATE() |

Indexes: `IX_AppErrors_Service`, `IX_AppErrors_Created`, `IX_AppErrors_RunId`.

---

## 5. Infrastructure

### 5.1 Networking and VNet

VNet: `vnet-finance-poc01`, address space `10.0.0.0/16`, region East US 2.

- **snet-functions** (`10.0.1.0/24`): hosts both Function Apps via VNet Integration. Delegation to `Microsoft.Web/serverFarms`. Both apps have `vnet_route_all_enabled = true` — all outbound traffic including DNS goes through the VNet.
- **snet-pe** (`10.0.2.0/24`): hosts all private endpoints. `private_endpoint_network_policies_enabled = false`.

**DNS requirement:** Because all traffic routes through the VNet, private endpoint DNS zones must be linked to the VNet. Required zones: `privatelink.database.windows.net` and `privatelink.blob.core.windows.net`. If DNS breaks, Function Apps cannot resolve SQL or Blob hostnames and will throw `No such host is known`. See RCA-FIN-004.

### 5.2 Azure SQL

- Server: `sql-finance-poc01.database.windows.net`
- Database: `FinanceDb`, SKU S1 (20 DTUs). Scale to S3 (100 DTUs) for end-of-quarter runs.
- Public network access: **disabled**. Private endpoint `pe-sql-poc01` at snet-pe.
- Authentication: Active Directory Default (Managed Identity). No SQL password at runtime.
- Connection string format: `Server=tcp:sql-finance-poc01.database.windows.net,1433;Database=FinanceDb;Authentication=Active Directory Default;Encrypt=True`

`id-positionprocessor` must exist as a SQL user with `db_datawriter` and `db_datareader` roles. This is a **manual step** — not in Terraform. After any SQL server recreate, re-run: `CREATE USER [id-positionprocessor] FROM EXTERNAL PROVIDER; ALTER ROLE db_datawriter ADD MEMBER [id-positionprocessor]; ALTER ROLE db_datareader ADD MEMBER [id-positionprocessor];`

### 5.3 Blob Storage

- Account: `stfinancepoc01`. Public network access: **disabled**.
- Private endpoint `pe-storage-poc01` at snet-pe. DNS zone `privatelink.blob.core.windows.net` linked to VNet.
- Container: `trades`. Trade files uploaded before 06:00 UTC each business day.
- File path convention: `trades/YYYY-MM-DD/positions.csv`
- `id-positionprocessor` has **Storage Blob Data Reader** role on `stfinancepoc01`.
- `id-positionprocessor` and `id-scheduler` both have **Storage Blob Data Contributor** on `AzureWebJobsStorage` (required for Durable task hub operation).

### 5.4 Key Vault

- Name: `kv-finance-poc01`. RBAC authorization enabled.
- Secret `SqlConnectionString` holds the SQL connection string.
- Referenced in Function App settings as `@Microsoft.KeyVault(SecretUri=...)`.
- `id-positionprocessor` has **Key Vault Secrets User** role.
- If Key Vault is unreachable at startup, the Function App fails to start and logs to `dbo.ApplicationErrors`.

### 5.5 App Service Plan and Application Insights

- Plan: `plan-finance-poc01`, SKU EP1 (Premium Elastic), Windows. Shared by both Function Apps. EP1 is required for VNet Integration.
- App Insights: `appi-finance-poc01`, workspace-based (Log Analytics: `law-finance-poc01`). Connected via `APPLICATIONINSIGHTS_CONNECTION_STRING`.
- SqlClient telemetry enabled automatically — no extra code needed. SQL commands, BulkCopy duration, and SQL errors appear as dependencies.

---

## 6. Managed Identity RBAC

### 6.1 id-positionprocessor roles

| Role | Scope | Purpose |
|---|---|---|
| Storage Blob Data Reader | stfinancepoc01 | LoadTradesActivity downloads trade CSV |
| Storage Blob Data Contributor | AzureWebJobsStorage | Durable task hub operation |
| Key Vault Secrets User | kv-finance-poc01 | Read SqlConnectionString secret |
| SQL user: db_datawriter + db_datareader | FinanceDb | INSERT to Positions and ProcessingRunLog; SELECT for queries |

The SQL user is created manually after deployment (not in Terraform). This is a known gap — see RCA-FIN-001 for what happens when it is missing.

### 6.2 id-scheduler roles

| Role | Scope | Purpose |
|---|---|---|
| Storage Blob Data Contributor | AzureWebJobsStorage | Function App internal storage for timer state |

The Scheduler does not connect to SQL or Blob Storage directly — it only POSTs HTTP to the PositionProcessor HttpStarter endpoint.

---

## 7. Application Insights Telemetry

### 7.1 What is tracked automatically

`Microsoft.Data.SqlClient` emits `DiagnosticSource` events. The App Insights SDK captures these via `SqlClientDiagnosticListener` as SQL dependencies — no extra code required.

What you see in App Insights:
- **BulkInsertAsync dependency** — duration of `SqlBulkCopy.WriteToServerAsync` to `dbo.Positions`. Appears with destination table name. Duration > 5000ms signals DTU pressure.
- **Individual SQL commands** — the MERGE into `dbo.ProcessingRunLog`, duration, success/failure.
- **SQL exceptions** — `SqlException` error number, full message, and stack trace appear in the Exceptions blade and as failed dependencies.
- **Blob download dependency** — Azure SDK auto-instruments blob download as an outbound HTTP dependency. `BlobNotFound` appears as a failed dependency.
- **Durable activity traces** — each activity (LoadTradesActivity, InsertPositionsActivity) has its own operation ID linked to the parent orchestration via `operation_ParentId`.

### 7.2 KQL queries for investigation

Find all failed SQL dependencies in the last hour:
```kusto
dependencies
| where success == false and type == "SQL"
| order by timestamp desc
| take 20
```

Find slow BulkInsert (duration > 5 seconds):
```kusto
dependencies
| where name contains "Positions" or name contains "BulkInsert"
| where duration > 5000
| project timestamp, name, duration, resultCode, operation_Id
```

Find all traces for a specific RunId:
```kusto
traces
| where message contains "RUN-20260521"
| order by timestamp desc
| take 50
```

Find orchestrator failures and their activity errors:
```kusto
traces
| where operation_Name contains "PositionOrchestrator"
    and severityLevel >= 3
| order by timestamp desc
```

Find blob download failures:
```kusto
dependencies
| where type == "Azure blob" and success == false
| order by timestamp desc
| take 20
```

---

## 8. Error Handling and AIOps Integration

### 8.1 Orchestrator error flow

All activity exceptions are caught by `PositionOrchestrator`'s top-level try/catch. `TaskFailedException` wraps the real activity error — the actual message is embedded in the string after `"failed with an unhandled exception: "`. The orchestrator extracts it, builds a structured error object, and calls `InsertPositionsActivity` with `Status=FAILED`.

The structured error is written to `dbo.ProcessingRunLog.ErrorMessage` as JSON. This is the primary data source for AIOps — richer than a plain exception message because it includes the failed stage, blob path, business date, and full exception chain.

Exceptions that **escape** the orchestrator (e.g. HttpStarter deserialization failure, Scheduler crash) are written to `dbo.ApplicationErrors` with a plain message and stack trace.

### 8.2 ErrorMessage JSON schema

When a run fails, `dbo.ProcessingRunLog.ErrorMessage` contains:

```json
{
  "FailedAt":      "2026-05-21T06:02:11Z",
  "RunId":         "RUN-20260521-ABCD1234",
  "BlobPath":      "trades/2026-05-21/positions.csv",
  "BusinessDate":  "2026-05-20T00:00:00Z",
  "Stage":         "LoadTradesActivity",
  "ExceptionType": "RequestFailedException",
  "Message":       "The specified blob does not exist. BlobErrorCode: BlobNotFound",
  "FullChain":     ["RequestFailedException: The specified blob does not exist..."]
}
```

`Stage` is the Durable activity name where the failure occurred (`LoadTradesActivity`, `InsertPositionsActivity`, or `Orchestrator` for failures before any activity call). `FullChain` is the full exception hierarchy — useful when the root cause is wrapped in multiple exception layers.

### 8.3 AIOps ErrorPoller polling logic

The ErrorPoller (Python Azure Function, 2-minute timer) polls two sources:

- **Primary:** `dbo.ProcessingRunLog WHERE Status = 'FAILED' AND Id > {watermark}` — catches all orchestrator-caught failures. ErrorMessage JSON is parsed to extract structured fields before embedding and Claude analysis.
- **Secondary:** `dbo.ApplicationErrors WHERE ErrorId > {watermark}` — catches escaped exceptions from HttpStarter and Scheduler.

Watermarks for both sources are stored as JSON in Azure Blob Storage (`aiops-watermarks/finance-error-poller-watermark.json`). For each new failure: RAG search → Claude analysis → Teams notification. After resolving an incident, add an RCA JSON file to `aiops-knowledge/rcas/` and re-run `python pipeline/index-all.py --incremental` to improve future suggestions.

---

## 9. Blob File Format

### 9.1 CSV format and path convention

Files must be uploaded to `stfinancepoc01` container `trades` before 06:00 UTC each business day.

Path format: `trades/YYYY-MM-DD/positions.csv` where the date is the **business date** (yesterday from the Scheduler's perspective).

Required CSV format:
```
TradeId,Portfolio,Instrument,Quantity,Price,Direction,TradeDate
T001,EQUITY-US,AAPL,100,185.50,BUY,2026-05-21
T002,EQUITY-US,AAPL,50,186.00,SELL,2026-05-21
```

Column rules:
- Header row is required and skipped during parsing
- `Direction` must be `BUY` or `SELL` (case-sensitive)
- `Quantity` and `Price` must be valid decimal numbers — `decimal.Parse` is called directly; non-numeric values throw `FormatException` and fail the entire run
- `TradeDate` must be parseable as `DateTime`
- Rows with fewer than 7 columns are **skipped silently** — they do not fail the run

### 9.2 Validation and error behaviour

| Condition | Behaviour | Status in RunLog |
|---|---|---|
| File missing at 06:00 | `BlobNotFound` exception, run fails immediately | FAILED |
| File present, zero data rows | Parsed as empty list, NO_DATA written | NO_DATA |
| One row has non-numeric Price | `FormatException`, entire run fails | FAILED |
| File has < 7 columns per row | Row silently skipped, run continues | SUCCESS or NO_DATA |
| File arrives late (after 06:00) | Use ManualTrigger: POST `/api/trigger?date=YYYY-MM-DD` | — |

If more than a handful of rows fail parsing, the run will still succeed (for column-count skips) or fail hard (for FormatException on numeric fields). There is no partial-success status — a run is SUCCESS, FAILED, or NO_DATA.

---

## 10. Data Models

### 10.1 TradeRecord and PositionResult

`TradeRecord` — parsed from CSV by LoadTradesActivity:
- `TradeId` (string), `Portfolio` (string), `Instrument` (string)
- `Quantity` (decimal), `Price` (decimal), `Direction` (string: BUY or SELL)
- `TradeDate` (DateTime)

`PositionResult` — calculated by InsertPositionsActivity, written to `dbo.Positions`:
- `Portfolio`, `Instrument`, `NetQuantity` (decimal), `MarketValue` (decimal)
- `PnL` (decimal — simplified: NetQuantity × AvgPrice × 0.02)
- `CalculatedAt` (DateTime), `RunId` (string)

### 10.2 ProcessingRequest and InsertInput

`ProcessingRequest` — JSON payload POSTed from Scheduler to HttpStarter:
- `BlobPath` (string): e.g. `trades/2026-05-21/positions.csv`
- `RunId` (string): e.g. `RUN-20260521-ABCD1234`
- `BusinessDate` (DateTime): the date trades were recorded for

`InsertInput` — passed from orchestrator to InsertPositionsActivity:
- `Trades` (List\<TradeRecord\> nullable): null on FAILED or NO_DATA paths
- `RunId` (string), `Status` (string: SUCCESS / FAILED / NO_DATA)
- `ErrorMessage` (string nullable): structured JSON on FAILED path, null otherwise

`ProcessingResult` — returned by orchestrator to HttpStarter:
- `TradesLoaded` (int), `PositionsInserted` (int), `Success` (bool)
- `Error` (string nullable): the full JSON error string on failure
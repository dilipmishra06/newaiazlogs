-- ============================================================
-- Finance POC Database Schema
-- Azure SQL Database: FinanceDb
-- Run this once after provisioning SQL 
-- ============================================================

-- Positions table: BulkInserted by InsertPositionsActivity
-- SqlBulkCopy targets dbo.Positions
CREATE TABLE dbo.Positions
(
    Id           INT            IDENTITY(1,1) PRIMARY KEY,
    RunId        NVARCHAR(64)   NOT NULL,
    Portfolio    NVARCHAR(50)   NOT NULL,
    Instrument   NVARCHAR(50)   NOT NULL,
    NetQuantity  DECIMAL(18,4)  NOT NULL,
    MarketValue  DECIMAL(18,2)  NOT NULL,
    PnL          DECIMAL(18,2)  NOT NULL,
    CalculatedAt DATETIME2      NOT NULL DEFAULT GETUTCDATE()
);

CREATE INDEX IX_Positions_RunId       ON dbo.Positions (RunId);
CREATE INDEX IX_Positions_Portfolio   ON dbo.Positions (Portfolio, Instrument);
CREATE INDEX IX_Positions_Calculated  ON dbo.Positions (CalculatedAt DESC);

-- ProcessingRunLog: written by WriteRunLogAsync after every run (success or failure)
-- This is the table AIOps will monitor for failures and slow runs
CREATE TABLE dbo.ProcessingRunLog
(
    Id           INT            IDENTITY(1,1) PRIMARY KEY,
    RunId        NVARCHAR(64)   NOT NULL UNIQUE,
    Status       NVARCHAR(20)   NOT NULL,    -- SUCCESS, FAILED, NO_DATA
    RowsInserted INT            NOT NULL DEFAULT 0,
    DurationMs   INT            NOT NULL DEFAULT 0,
    CompletedAt  DATETIME2      NOT NULL DEFAULT GETUTCDATE(),
    ErrorMessage NVARCHAR(MAX)  NULL
);

CREATE INDEX IX_RunLog_Status      ON dbo.ProcessingRunLog (Status);
CREATE INDEX IX_RunLog_Completed   ON dbo.ProcessingRunLog (CompletedAt DESC);

-- ApplicationErrors: written by both Function Apps on unhandled exceptions
-- This is the table the AIOps ErrorPoller reads from
CREATE TABLE dbo.ApplicationErrors
(
    ErrorId      INT            IDENTITY(1,1) PRIMARY KEY,
    ServiceName  NVARCHAR(100)  NOT NULL,
    ErrorMessage NVARCHAR(MAX)  NOT NULL,
    StackTrace   NVARCHAR(MAX)  NULL,
    RunId        NVARCHAR(64)   NULL,        -- links error to a processing run
    CreatedAt    DATETIME2      NOT NULL DEFAULT GETUTCDATE()
);

CREATE INDEX IX_AppErrors_Service   ON dbo.ApplicationErrors (ServiceName);
CREATE INDEX IX_AppErrors_Created   ON dbo.ApplicationErrors (CreatedAt DESC);
CREATE INDEX IX_AppErrors_RunId     ON dbo.ApplicationErrors (RunId);

GO

-- ============================================================
-- Sample data to test the pipeline locally
-- ============================================================
-- Upload this CSV to Storage Account blob: trades/2026-05-21/positions.csv
-- TradeId,Portfolio,Instrument,Quantity,Price,Direction,TradeDate
-- T001,EQUITY-US,AAPL,100,185.50,BUY,2026-05-21
-- T002,EQUITY-US,AAPL,50,186.00,SELL,2026-05-21
-- T003,EQUITY-US,MSFT,200,420.00,BUY,2026-05-21
-- T004,FIXED-INC,US10Y,1000000,98.50,BUY,2026-05-21
-- T005,EQUITY-EU,SAP,300,195.00,BUY,2026-05-21
-- T006,EQUITY-EU,SAP,100,196.00,SELL,2026-05-21

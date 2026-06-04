"""
ErrorPoller/sql_reader.py
──────────────────────────
Reads failures from TWO tables in FinanceDb:

  PRIMARY — dbo.ProcessingRunLog WHERE Status = 'FAILED'
    Written by InsertPositionsActivity.WriteRunLogAsync after every failed run.
    ErrorMessage is a structured JSON blob:
      { FailedAt, RunId, BlobPath, BusinessDate, Stage, ExceptionType,
        Message, FullChain[] }
    This is the main source — covers all orchestrator-caught failures.
    Watermark key: "runlog" → highest Id seen.

  SECONDARY — dbo.ApplicationErrors
    Written by both Function Apps on unhandled exceptions that escape the
    orchestrator's try/catch (e.g. HttpStarter parse failure, DI errors).
    Columns: ErrorId, ServiceName, ErrorMessage, StackTrace, RunId, CreatedAt
    Watermark key: "apperrors" → highest ErrorId seen.

Both watermarks are stored as a single JSON blob in Azure Blob Storage.
Returns a unified list of dicts, each tagged with source="runlog"|"apperrors".
"""

import json
import logging
import os
import struct

import pyodbc

logger = logging.getLogger(__name__)

MAX_ROWS_PER_SOURCE = 20


def _get_connection() -> pyodbc.Connection:
    conn_str = os.environ["SQL_CONNECTION_STRING"]

    if "Active Directory Default" in conn_str or "ActiveDirectoryMsi" in conn_str:
        from azure.identity import DefaultAzureCredential
        credential   = DefaultAzureCredential()
        token        = credential.get_token("https://database.windows.net/.default")
        token_bytes  = token.token.encode("utf-16-le")
        token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)

        bare = (
            f"Driver={{ODBC Driver 18 for SQL Server}};"
            f"Server=tcp:{_extract_server(conn_str)},1433;"
            f"Database={_extract_db(conn_str)};"
            f"Encrypt=yes;TrustServerCertificate=no;"
        )
        conn = pyodbc.connect(bare, attrs_before={1256: token_struct})
    else:
        conn = pyodbc.connect(conn_str)

    conn.timeout = 30
    return conn


def fetch_new_errors(since: dict) -> list[dict]:
    """
    Fetch new failures from both tables.

    Parameters
    ----------
    since : dict
        { "runlog": <last_id>, "apperrors": <last_id> }
        Both keys default to 0 (first run).

    Returns
    -------
    List of unified error dicts, sorted by timestamp ascending.
    Each dict always contains:
        source          "runlog" | "apperrors"
        error_id        int   — used to advance the correct watermark
        service         str
        run_id          str | None
        error_message   str   — raw text or structured JSON
        stack_trace     str   — empty for runlog rows (stack is inside JSON)
        created_at      str
    """
    since_runlog    = since.get("runlog",    0)
    since_apperrors = since.get("apperrors", 0)

    try:
        conn = _get_connection()
    except Exception as e:
        logger.error("SQL connection failed: %s", e)
        raise

    results = []

    with conn:
        cursor = conn.cursor()

        # ── Primary: ProcessingRunLog ─────────────────────────────────────────
        if since_runlog == 0:
            sql = f"""
                SELECT TOP {MAX_ROWS_PER_SOURCE}
                    Id, RunId, Status, ErrorMessage, CompletedAt
                FROM dbo.ProcessingRunLog
                WHERE Status = 'FAILED'
                ORDER BY Id DESC
            """
            cursor.execute(sql)
            columns  = [c[0] for c in cursor.description]
            runlog_rows = list(reversed([dict(zip(columns, r)) for r in cursor.fetchall()]))
        else:
            sql = f"""
                SELECT TOP {MAX_ROWS_PER_SOURCE}
                    Id, RunId, Status, ErrorMessage, CompletedAt
                FROM dbo.ProcessingRunLog
                WHERE Id > ? AND Status = 'FAILED'
                ORDER BY Id ASC
            """
            cursor.execute(sql, since_runlog)
            columns     = [c[0] for c in cursor.description]
            runlog_rows = [dict(zip(columns, r)) for r in cursor.fetchall()]

        for row in runlog_rows:
            results.append({
                "source":        "runlog",
                "error_id":      row["Id"],
                "service":       "PositionProcessor",   # runlog is only written by PositionProcessor
                "run_id":        row.get("RunId") or "",
                "error_message": row.get("ErrorMessage") or "No error message",
                "stack_trace":   "",                     # stack is inside the JSON ErrorMessage blob
                "created_at":    str(row.get("CompletedAt", "")),
            })

        logger.info("ProcessingRunLog: %d new FAILED row(s)", len(runlog_rows))

        # ── Secondary: ApplicationErrors ──────────────────────────────────────
        if since_apperrors == 0:
            sql = f"""
                SELECT TOP {MAX_ROWS_PER_SOURCE}
                    ErrorId, ServiceName, ErrorMessage, StackTrace, RunId, CreatedAt
                FROM dbo.ApplicationErrors
                ORDER BY ErrorId DESC
            """
            cursor.execute(sql)
            columns   = [c[0] for c in cursor.description]
            app_rows  = list(reversed([dict(zip(columns, r)) for r in cursor.fetchall()]))
        else:
            sql = f"""
                SELECT TOP {MAX_ROWS_PER_SOURCE}
                    ErrorId, ServiceName, ErrorMessage, StackTrace, RunId, CreatedAt
                FROM dbo.ApplicationErrors
                WHERE ErrorId > ?
                ORDER BY ErrorId ASC
            """
            cursor.execute(sql, since_apperrors)
            columns  = [c[0] for c in cursor.description]
            app_rows = [dict(zip(columns, r)) for r in cursor.fetchall()]

        for row in app_rows:
            results.append({
                "source":        "apperrors",
                "error_id":      row["ErrorId"],
                "service":       row.get("ServiceName") or "Unknown",
                "run_id":        row.get("RunId") or "",
                "error_message": row.get("ErrorMessage") or "No error message",
                "stack_trace":   row.get("StackTrace") or "",
                "created_at":    str(row.get("CreatedAt", "")),
            })

        logger.info("ApplicationErrors: %d new row(s)", len(app_rows))

    # Sort unified list by timestamp so watermarks advance in order
    results.sort(key=lambda r: r["created_at"])
    logger.info("Total new errors across both sources: %d", len(results))
    return results


def _extract_server(conn_str: str) -> str:
    for part in conn_str.split(";"):
        if part.strip().lower().startswith("server="):
            return part.split("=", 1)[1].replace("tcp:", "").split(",")[0].strip()
    return ""


def _extract_db(conn_str: str) -> str:
    for part in conn_str.split(";"):
        if part.strip().lower().startswith("database="):
            return part.split("=", 1)[1].strip()
    return "FinanceDb"
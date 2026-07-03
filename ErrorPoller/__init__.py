"""
ErrorPoller/__init__.py
────────────────────────
Finance AIOps — monitors PositionProcessor and Scheduler Function Apps.

Polls TWO sources every 2 minutes:
  1. dbo.ProcessingRunLog WHERE Status = 'FAILED'  — primary, structured JSON errors
  2. dbo.ApplicationErrors                          — secondary, unhandled exceptions

For each new failure:
  1. Parse structured JSON from ProcessingRunLog (if present)
  2. RAG search across system-knowledge-base and rca-knowledge-base
  3. AI analysis with full context
  4. Teams notification with structured card

Watermarks for both sources stored as JSON in Azure Blob Storage.
"""

import json
import logging
import azure.functions as func

from .sql_reader      import fetch_new_errors
from .watermark       import get_watermark, update_watermark
from .rag_searcher    import search_knowledge_bases
from .openai_analyzer import analyze_with_ai
from .teams_notifier  import send_teams_notification

logger = logging.getLogger(__name__)


def main(timer: func.TimerRequest) -> None:
    logger.info("=== Finance ErrorPoller triggered ===")

    if timer.past_due:
        logger.warning("Timer is past due")

    # 1. Read both watermarks
    watermark = get_watermark()
    logger.info("Watermarks: runlog=%d apperrors=%d",
                watermark["runlog"], watermark["apperrors"])

    # 2. Fetch new failures from both tables
    new_errors = fetch_new_errors(since=watermark)

    if not new_errors:
        logger.info("No new errors — nothing to process")
        return

    logger.info("Found %d new failure(s)", len(new_errors))

    # Track highest IDs seen per source to advance watermarks correctly
    highest = {"runlog": watermark["runlog"], "apperrors": watermark["apperrors"]}

    for error_row in new_errors:
        source     = error_row["source"]          # "runlog" | "apperrors"
        error_id   = error_row["error_id"]
        service    = error_row["service"]
        run_id     = error_row["run_id"]
        created_at = error_row["created_at"]
        raw_msg    = error_row["error_message"]
        stack      = error_row["stack_trace"]

        logger.info("Processing source=%s error_id=%d service=%s run_id=%s",
                    source, error_id, service, run_id)

        # 3. For runlog rows, the ErrorMessage is a structured JSON blob from the
        #    orchestrator's catch block. Parse it to get richer diagnostic text.
        #    For apperrors rows it's a plain exception message.
        parsed_error = _parse_runlog_error(raw_msg) if source == "runlog" else None

        search_text  = _build_search_text(parsed_error, raw_msg, stack)
        full_context = _build_full_context(error_row, parsed_error)

        # 4. RAG
        try:
            rag_context = search_knowledge_bases(
                error_text=search_text,
                service=service,
                stack_trace=stack,
                top_k=3,
            )
            logger.info("RAG: %d RCA(s), %d system doc(s)",
                        len(rag_context.get("rcas", [])),
                        len(rag_context.get("system_docs", [])))
        except Exception as e:
            logger.warning("RAG failed (non-blocking): %s", e)
            rag_context = {"rcas": [], "system_docs": []}

        
        # 5. AI Analysis
        try:
            ai_suggestion = analyze_with_ai(
                error_details=full_context,
                service=service,
                run_id=run_id,
                rag_context=rag_context,
            )
            logger.info("AI analysis done — Priority=%s Confidence=%s",
                        ai_suggestion.get("priority"),
                        ai_suggestion.get("confidence"))
        except Exception as e:
            logger.error("AI analysis failed for error_id=%d: %s", error_id, e)
            ai_suggestion = _fallback_suggestion(run_id)
        # 6. Teams
        # Use the parsed message as the snippet if available — it's cleaner
        snippet = (parsed_error.get("Message") or raw_msg)[:300] if parsed_error else raw_msg[:300]
        try:
            send_teams_notification(
                error_id=error_id,
                service=service,
                run_id=run_id,
                created_at=created_at,
                error_snippet=snippet,
                stack_snippet=stack[:200],
                ai_suggestion=ai_suggestion,
                rag_context=rag_context,
            )
            logger.info("Teams notified for source=%s error_id=%d", source, error_id)
        except Exception as e:
            logger.error("Teams failed for error_id=%d: %s", error_id, e)

        # Advance the correct watermark
        if error_id > highest[source]:
            highest[source] = error_id

    # 7. Persist watermarks (only advances, never goes backwards)
    update_watermark(highest)
    logger.info("=== ErrorPoller complete — processed %d failure(s) ===", len(new_errors))


def _parse_runlog_error(raw_msg: str) -> dict | None:
    """
    ProcessingRunLog.ErrorMessage is JSON serialized by the orchestrator:
      { FailedAt, RunId, BlobPath, BusinessDate, Stage, ExceptionType,
        Message, FullChain[] }
    Returns the parsed dict, or None if it isn't valid JSON.
    """
    if not raw_msg or not raw_msg.strip().startswith("{"):
        return None
    try:
        return json.loads(raw_msg)
    except json.JSONDecodeError:
        return None


def _build_search_text(parsed: dict | None, raw_msg: str, stack: str) -> str:
    """
    Text used as the RAG embedding query.
    Prioritises the structured exception message and type — the most
    semantically meaningful signal for matching past RCAs.
    Excludes RunId, timestamps, row counts — noise for embeddings.
    """
    parts = []

    if parsed:
        # Structured runlog error — use ExceptionType + Message
        if parsed.get("ExceptionType"):
            parts.append(f"ExceptionType: {parsed['ExceptionType']}")
        if parsed.get("Message"):
            parts.append(f"Error: {parsed['Message']}")
        if parsed.get("Stage"):
            parts.append(f"Stage: {parsed['Stage']}")
        # FullChain gives the full exception hierarchy — good semantic signal
        chain = parsed.get("FullChain", [])
        if chain:
            parts.append("ExceptionChain: " + " | ".join(chain[:3]))
    else:
        # Plain message (ApplicationErrors or non-JSON runlog)
        if raw_msg:
            parts.append(f"Error: {raw_msg[:600]}")
        if stack:
            # Top of stack is the most distinctive — skip framework frames
            top_stack = "\n".join(
                line for line in stack.split("\n")[:15]
                if not any(noise in line for noise in
                           ["at System.", "at Microsoft.", "at Azure."])
            )
            parts.append(f"Stack: {top_stack}")

    return "\n".join(parts)


def _build_full_context(row: dict, parsed: dict | None) -> str:
    """
    Full context passed to AI for structured analysis.
    For runlog rows, unpacks the JSON to give AI named fields.
    """
    parts = [
        f"Source: {row['source']}",
        f"Service: {row['service']}",
        f"RunId: {row['run_id'] or 'Unknown'}",
        f"Timestamp: {row['created_at']}",
    ]

    if parsed:
        # Structured orchestrator error — give AI the parsed fields
        parts += [
            f"FailedStage: {parsed.get('Stage', 'Unknown')}",
            f"ExceptionType: {parsed.get('ExceptionType', 'Unknown')}",
            f"ErrorMessage: {parsed.get('Message', 'Unknown')}",
        ]
        if parsed.get("BlobPath"):
            parts.append(f"BlobPath: {parsed['BlobPath']}")
        if parsed.get("BusinessDate"):
            parts.append(f"BusinessDate: {parsed['BusinessDate']}")
        chain = parsed.get("FullChain", [])
        if chain:
            parts.append("ExceptionChain:\n  " + "\n  ".join(chain))
    else:
        parts.append(f"ErrorMessage: {row['error_message']}")
        if row.get("stack_trace"):
            parts.append(f"StackTrace:\n{row['stack_trace'][:2000]}")

    return "\n".join(parts)


def _fallback_suggestion(run_id: str) -> dict:
    run_clause = f"WHERE RunId = '{run_id}'" if run_id else "ORDER BY CompletedAt DESC"
    return {
        "what_happened":    "AI analysis unavailable — investigate manually.",
        "root_cause":       "Could not determine automatically.",
        "impact":           "Position processing may be incomplete for this run.",
        "immediate_action": (
            f"1. SELECT * FROM dbo.ProcessingRunLog {run_clause}\n"
            f"2. SELECT TOP 10 * FROM dbo.ApplicationErrors ORDER BY ErrorId DESC\n"
            f"3. Check App Insights exceptions blade for the same RunId"
        ),
        "priority":         "P2",
        "confidence":       "none",
        "cascade_risk":     "unknown",
        "rca_reference":    None,
        "suggested_sql":    (
            f"SELECT * FROM dbo.ProcessingRunLog {run_clause};\n"
            f"SELECT * FROM dbo.ApplicationErrors WHERE RunId = '{run_id}';"
        ),
        "suggested_kql":    (
            f"traces | where message contains '{run_id}' | order by timestamp desc | take 50"
            if run_id else
            "exceptions | where timestamp > ago(1h) | order by timestamp desc | take 20"
        ),
    }
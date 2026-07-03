"""
ErrorPoller/openai_analyzer.py
────────────────────────────────
Sends error + RAG context to GPT-5.4 Pro via Azure OpenAI / Azure AI Foundry.
Finance-domain aware — understands Durable Functions, SqlBulkCopy, RunId, orchestrations.

"""

import json
import logging
import os

from openai import OpenAI

logger = logging.getLogger(__name__)

PROMPT_WITH_CONTEXT = """
You are a senior Azure SRE analyzing a live error in a Finance Position Processing system.
This system uses Azure Durable Functions (C#) to load trade CSV files from Blob Storage,
calculate P&L positions, and BulkInsert results to Azure SQL Database. Try to be broad viewed and use azure docs to list all possible misconfigurations,
Your analysis is a SUGGESTION. A human engineer must verify and document the RCA.

CURRENT ERROR
Service : {service}
RunId   : {run_id}
Details :
{error_details}

{rca_section}
{system_section}

Respond ONLY with this JSON — no markdown, no preamble:
{{
    "what_happened"    : "One sentence. What went wrong in this processing run.",
    "root_cause"       : "Most likely root cause. Reference RCA ID if matched.",
    "impact"           : "Which runs or portfolios are affected. Are positions for this date missing?",
    "immediate_action" : "Numbered steps. Include specific SQL queries to check dbo.ProcessingRunLog and dbo.ApplicationErrors.",
    "priority"         : "P1 or P2 or P3",
    "confidence"       : "high or medium or low",
    "cascade_risk"     : "yes or no — will downstream consumers have stale/missing position data?",
    "rca_reference"    : "RCA ID and how it relates, or null.",
    "suggested_sql"    : "A T-SQL query to run against FinanceDb (Azure SQL) to investigate further.",
    "suggested_kql"    : "A KQL query to run in Application Insights Log Analytics to correlate telemetry."
}}

Priority: P1=all position processing failed/data missing for business date.
P2=partial failure or delayed. P3=non-critical warning.

For suggested_sql: target dbo.ApplicationErrors, dbo.ProcessingRunLog, or dbo.Positions.
For suggested_kql: target the dependencies, traces, or exceptions tables in App Insights.
"""

PROMPT_NO_CONTEXT = """
You are a senior Azure SRE analyzing a live error in a Finance Position Processing system.
This system uses Azure Durable Functions (C#) to load trade CSV files from Blob Storage,
calculate P&L positions, and BulkInsert results to Azure SQL Database.
Key tables: dbo.Positions (bulk inserted), dbo.ProcessingRunLog (run audit), dbo.ApplicationErrors (error log).
Your analysis is a SUGGESTION. Human investigation required.

CURRENT ERROR
Service : {service}
RunId   : {run_id}
Details :
{error_details}

No matching past RCAs or system docs found. Use your knowledge of Azure Durable Functions,
SqlBulkCopy, Azure SQL private endpoints, and Managed Identity authentication.

Respond ONLY with this JSON:
{{
    "what_happened"    : "One sentence.",
    "root_cause"       : "Most likely root cause.",
    "impact"           : "Which runs or portfolios are affected.",
    "immediate_action" : "Numbered steps with specific SQL queries.",
    "priority"         : "P1 or P2 or P3",
    "confidence"       : "high or medium or low",
    "cascade_risk"     : "yes or no",
    "rca_reference"    : null,
    "suggested_sql"    : "A T-SQL query to run against FinanceDb (Azure SQL) to investigate further.",
    "suggested_kql"    : "A KQL query to run in Application Insights Log Analytics to correlate telemetry."
}}
"""


def analyze_with_ai(
    error_details: str,
    service: str,
    run_id: str,
    rag_context: dict,
) -> dict:
    client = _get_client()

    rcas        = rag_context.get("rcas", [])
    system_docs = rag_context.get("system_docs", [])

    if rcas or system_docs:
        rca_section    = _fmt_rcas(rcas)
        system_section = _fmt_system(system_docs)
        prompt = PROMPT_WITH_CONTEXT.format(
            service=service,
            run_id=run_id or "N/A",
            error_details=error_details[:3000],
            rca_section=rca_section,
            system_section=system_section,
        )
        logger.info("Calling GPT-5.4 Pro with %d RCA(s) + %d system doc(s)", len(rcas), len(system_docs))
    else:
        prompt = PROMPT_NO_CONTEXT.format(
            service=service,
            run_id=run_id or "N/A",
            error_details=error_details[:3000],
        )
        logger.info("Calling GPT-5.4 Pro — no RAG context, using base knowledge")

    response = client.responses.create(
        model=os.environ.get("OPENAI_MODEL_DEPLOYMENT", "gpt-5.4-pro"),
        max_output_tokens=8192,   # reasoning tokens share this budget with visible output —
                                  # 4096 was getting fully consumed by internal reasoning on
                                  # "high" effort, leaving 0 chars for the actual JSON answer
        text={"format": {"type": "json_object"}},   # Responses API: JSON mode lives under text.format, not response_format
        reasoning={"effort": "medium"},   # gpt-5.4-pro supports medium/high/xhigh only (no "low").
                                           # medium is enough for structured triage output and
                                           # leaves more of the token budget for the visible answer
        input=[
            {"role": "system", "content": "You respond only with valid JSON. No markdown, no preamble, no commentary."},
            {"role": "user", "content": prompt},
        ],
    )

    raw = response.output_text
    logger.info("GPT-5.4 Pro responded — %d chars", len(raw))
    if not raw:
        logger.warning(
            "GPT-5.4 Pro returned empty output_text — likely reasoning tokens "
            "consumed the full max_output_tokens budget. status=%s incomplete_details=%s",
            getattr(response, "status", None),
            getattr(response, "incomplete_details", None),
        )
    return _parse(raw)


def _fmt_rcas(rcas: list) -> str:
    if not rcas:
        return ""
    lines = ["SIMILAR PAST INCIDENTS (verified RCAs)"]
    for i, r in enumerate(rcas, 1):
        lines.append(
            f"\nPast Incident {i} — {r.get('rca_id')} "
            f"(Match: {r.get('similarity')} · {r.get('duration_minutes')} min)\n"
            f"  Service     : {r.get('service')}\n"
            f"  What happened: {r.get('error_summary')}\n"
            f"  Root cause  : {r.get('root_cause')}\n"
            f"  Resolution  : {r.get('resolution')}\n"
            f"  Prevention  : {r.get('prevention')}\n"
            f"  Resolved by : {r.get('resolved_by')} on {r.get('date_resolved')}"
        )
    return "\n".join(lines)


def _fmt_system(docs: list) -> str:
    if not docs:
        return ""
    lines = ["RELEVANT SYSTEM KNOWLEDGE (architecture and infrastructure docs)"]
    for i, d in enumerate(docs, 1):
        lines.append(
            f"\nDoc {i} — {d.get('title')} (Match: {d.get('similarity')})\n"
            f"  Source: {d.get('source_file')}\n"
            f"  {d.get('content', '')[:600]}"
        )
    return "\n".join(lines)


def _parse(raw: str) -> dict:
    try:
        # json_object mode returns clean JSON directly, but we still strip
        # markdown fences defensively in case the deployment/model ignores
        # response_format (some proxy/gateway setups can drop it).
        cleaned = raw.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.split("```")[1]
            if cleaned.startswith("json"):
                cleaned = cleaned[4:]
        return json.loads(cleaned.strip())
    except json.JSONDecodeError as e:
        logger.error("JSON parse failed: %s — raw: %s", e, raw[:200])
        return {
            "what_happened":    raw[:200],
            "root_cause":       "AI response parsing failed.",
            "impact":           "Unknown — check dbo.ProcessingRunLog immediately.",
            "immediate_action": (
                "1. SELECT TOP 10 * FROM dbo.ApplicationErrors ORDER BY ErrorId DESC\n"
                "2. SELECT TOP 10 * FROM dbo.ProcessingRunLog ORDER BY CompletedAt DESC\n"
                "3. Check App Insights exceptions blade"
            ),
            "priority":         "P2",
            "confidence":       "low",
            "cascade_risk":     "unknown",
            "rca_reference":    None,
            "suggested_sql":    "SELECT TOP 10 * FROM dbo.ApplicationErrors ORDER BY ErrorId DESC",
            "suggested_kql":    "exceptions | where timestamp > ago(1h) | order by timestamp desc | take 20",
        }


def _get_client() -> OpenAI:
    """
    Azure OpenAI v1 client — Microsoft's current recommended pattern (GA since
    Aug 2025). Uses the plain OpenAI() client pointed at the /openai/v1/ base
    path instead of the legacy AzureOpenAI(api_version=...) client. This
    removes the dated api-version guessing game entirely and is required for
    the Responses API parameter shapes (text.format, reasoning, etc.) to work
    reliably across model/version updates.

    Required env vars:
        AZURE_OPENAI_KEY
        AZURE_OPENAI_RESOURCE_NAME   e.g. "my-resource"  (just the resource name,
                                      not the full URL — used to build the v1 base_url)

    If you have a literal endpoint URL instead of a resource name, set
    AZURE_OPENAI_ENDPOINT to the full https://<resource>.openai.azure.com/ value
    and this function will derive the v1 path from it.
    """
    endpoint = os.environ.get("AZURE_OPENAI_ENDPOINT")
    if not endpoint:
        resource_name = os.environ["AZURE_OPENAI_RESOURCE_NAME"]
        endpoint = f"https://{resource_name}.openai.azure.com"

    base_url = endpoint.rstrip("/") + "/openai/v1/"

    return OpenAI(
        api_key=os.environ["AZURE_OPENAI_KEY"],
        base_url=base_url,
    )
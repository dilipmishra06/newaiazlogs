"""
ErrorPoller/teams_notifier.py
──────────────────────────────
Sends rich Teams alert card with full AI analysis and RAG context.

Card sections:
  1. Header       — service, priority, error type, RAG context summary
  2. AI Analysis  — what happened, root cause, impact, immediate actions
  3. Knowledge Base — RCAs matched, system docs used
  4. Error snippet  — raw error message for engineer reference
  5. SQL Query    — run against FinanceDb (Azure SQL)
  6. KQL Query    — run in Application Insights
  7. Disclaimer + upload RCA CTA

Error-type classification
─────────────────────────
Uses the same OpenAI() v1 client pattern as
ai_analyzer.py (no api_version, base_url ends in /openai/v1/), and the
Responses API (input=..., output_text) instead of Chat Completions. Since
this is just a one-line classification (not deep diagnostic reasoning),
reasoning effort is kept low to minimize latency on the notification path.

Fallback: if the model call fails for any reason (timeout, API error, empty
response) the function falls back to "Application Error" so the Teams card
is never blocked.
"""

import json
import logging
import os

from openai import OpenAI
import requests

logger = logging.getLogger(__name__)

# ── Error-type classifier via GPT-5.4 Pro ────────────────────────────────────

_CLASSIFY_PROMPT = """\
You are an Azure SRE. Given the error snippet below, return ONLY a short
human-readable error-type label (2-5 words, title case, no punctuation).

Examples of good labels:
  SQL Login Failure
  Blob Not Found
  DNS Resolution Failure
  Managed Identity Auth Error
  ADF Pipeline Timeout
  Key Vault Access Denied
  NSG Blocked Outbound
  DTU Exhaustion
  ARM Throttling
  Durable Function Replay Error
  CSV Parse Failure
  Private Endpoint Misconfiguration
  Service Bus Dead Letter
  Redis Connection Refused

Return ONLY the label — no explanation, no quotes, no JSON.

ERROR SNIPPET:
{snippet}
"""

_classifier_client = None  # lazily created, reused across invocations (cold-start friendly)


def _get_classifier_client() -> OpenAI:
    """
    Azure OpenAI v1 client for the error-type classifier.
    Same pattern as ai_analyzer.py — plain OpenAI() client pointed at the
    /openai/v1/ base path, no api_version required.

    Required env vars:
        AZURE_OPENAI_KEY
        AZURE_OPENAI_ENDPOINT          e.g. https://<resource>.openai.azure.com/
                                        (or AZURE_OPENAI_RESOURCE_NAME — just the
                                        resource name — if you prefer to derive it)
    """
    global _classifier_client
    if _classifier_client is not None:
        return _classifier_client

    endpoint = os.environ.get("AZURE_OPENAI_ENDPOINT")
    if not endpoint:
        resource_name = os.environ["AZURE_OPENAI_RESOURCE_NAME"]
        endpoint = f"https://{resource_name}.openai.azure.com"

    base_url = endpoint.rstrip("/") + "/openai/v1/"

    _classifier_client = OpenAI(
        api_key=os.environ["AZURE_OPENAI_KEY"],
        base_url=base_url,
    )
    return _classifier_client


def _detect_error_type(snippet: str) -> str:
    """
    Classify the error snippet into a short human-readable label.

    Uses GPT-5.4 Pro (max_output_tokens=60, low reasoning effort for
    latency) so any error type — including novel ones never seen before —
    gets a meaningful label rather than the generic "Application Error"
    fallback.

    Falls back to "Application Error" on any API failure so the
    Teams notification is never blocked.
    """
    if not snippet or not snippet.strip():
        return "Application Error"

    try:
        client = _get_classifier_client()
        response = client.responses.create(
            model=os.environ.get("OPENAI_MODEL_DEPLOYMENT", "gpt-5.4-pro"),
            max_output_tokens=500,   # short label, but reasoning tokens share this budget —
                                     # too low risks the same 0-char issue as the main analyzer
            reasoning={"effort": "medium"},  # gpt-5.4-pro only supports medium/high/xhigh — "low" 400s
            input=[{
                "role": "user",
                "content": _CLASSIFY_PROMPT.format(snippet=snippet[:800]),
            }],
        )
        label = (response.output_text or "").strip()
        # Safety guard: if the model returns something unexpectedly long or
        # multi-line, truncate to the first line.
        label = label.splitlines()[0].strip() if label else ""
        if label:
            logger.debug("GPT-5.4 Pro error-type classification: %r", label)
            return label
    except Exception as exc:
        logger.warning("Error-type classification via GPT-5.4 Pro failed: %s — using fallback", exc)

    return "Application Error"


# ── Main notifier ─────────────────────────────────────────────────────────────

def send_teams_notification(
    error_id:      int,
    service:       str,
    run_id:        str,
    created_at:    str,
    error_snippet: str,
    stack_snippet: str,
    ai_suggestion: dict,
    rag_context:   dict,
) -> None:
    webhook_url = os.environ["TEAMS_WEBHOOK_URL"]

    priority           = ai_suggestion.get("priority", "P2")
    color, emoji       = _priority_style(priority)
    confidence         = ai_suggestion.get("confidence", "medium")
    cascade            = ai_suggestion.get("cascade_risk", "unknown")

    # Dynamic classification — no hardcoded keywords
    error_type = _detect_error_type(error_snippet)

    confidence_display = {
        "high":   "🟢 High",
        "medium": "🟡 Medium",
        "low":    "🔴 Low",
        "none":   "⚫ Unavailable",
    }.get(confidence, confidence)

    cascade_display = (
        "⚠️ Yes — check dependent services" if cascade == "yes"
        else "✅ No cascade detected"
    )

    action = ai_suggestion.get("immediate_action", "Investigate immediately.")
    if isinstance(action, list):
        action = "\n".join(f"{i+1}. {s}" for i, s in enumerate(action))

    suggested_sql = ai_suggestion.get(
        "suggested_sql",
        "SELECT TOP 10 * FROM dbo.ApplicationErrors ORDER BY ErrorId DESC",
    )
    suggested_kql = ai_suggestion.get(
        "suggested_kql",
        "exceptions | where timestamp > ago(1h) | order by timestamp desc | take 20",
    )

    rcas        = rag_context.get("rcas", [])
    system_docs = rag_context.get("system_docs", [])
    context_display = f"{len(rcas)} RCA(s) · {len(system_docs)} system doc(s)"

    card = {
        "@type":    "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": color,
        "summary": f"{emoji} [{priority}] {error_type} — {service} — ErrorId: {error_id}",
        "sections": [

            # ── Header ─────────────────────────────────────────────────────────
            {
                "activityTitle":    f"{emoji} **{error_type} — {service}**",
                "activitySubtitle": f"ErrorId: `{error_id}` · {created_at[:19].replace('T', ' ')} UTC",
                "markdown": True,
                "facts": [
                    {"name": "Priority",       "value": f"**{priority}**"},
                    {"name": "Service",        "value": service},
                    {"name": "RunId",          "value": run_id or "N/A"},
                    {"name": "Error Type",     "value": error_type},
                    {"name": "Cascade Risk",   "value": cascade_display},
                    {"name": "AI Confidence",  "value": confidence_display},
                    {"name": "Knowledge Used", "value": context_display},
                ],
            },

            # ── AI Analysis ────────────────────────────────────────────────────
            {
                "title":    "🤖 AI Analysis *(suggestion only — not verified)*",
                "markdown": True,
                "text": (
                    f"**What happened:**\n{ai_suggestion.get('what_happened', 'Unknown')}\n\n"
                    f"**Root cause:**\n{ai_suggestion.get('root_cause', 'Unknown')}\n\n"
                    f"**Impact:**\n{ai_suggestion.get('impact', 'Unknown')}\n\n"
                    f"**Immediate actions:**\n{action}"
                ),
            },

            # ── Knowledge Base context ─────────────────────────────────────────
            _build_knowledge_section(ai_suggestion, rcas, system_docs),

            # ── Error snippet ──────────────────────────────────────────────────
            {
                "title":    "📋 Error Details",
                "markdown": True,
                "text": f"```\n{error_snippet}\n```"
                + (f"\n**Stack trace:**\n```\n{stack_snippet}\n```" if stack_snippet else ""),
            },

            # ── SQL query (Azure SQL / FinanceDb) ──────────────────────────────
            {
                "title":    "🗄️ Suggested SQL Query",
                "markdown": True,
                "text": (
                    "Run against **FinanceDb** (Azure SQL):\n"
                    f"```sql\n{suggested_sql}\n```"
                ),
            },

            # ── KQL query (Application Insights) ──────────────────────────────
            {
                "title":    "🔍 Suggested KQL Query",
                "markdown": True,
                "text": (
                    "Run in **Application Insights → Logs**:\n"
                    f"```\n{suggested_kql}\n```"
                ),
            },

            # ── Footer ─────────────────────────────────────────────────────────
            {
                "markdown": True,
                "text": (
                    "---\n"
                    "⚠️ *AI suggestion only. Human investigation required. "
                    "After resolving, add an RCA to `aiops-knowledge/rcas/` and re-run the indexer "
                    "so future incidents get better suggestions.*"
                ),
            },
        ],

        "potentialAction": [
            {
                "@type": "OpenUri",
                "name": "📊 Application Insights",
                "targets": [{"os": "default", "uri": "https://portal.azure.com/#blade/Microsoft_Azure_Monitoring/AzureMonitoringBrowseBlade/logs"}],
            },
            {
                "@type": "OpenUri",
                "name": "🗄️ Azure SQL Portal",
                "targets": [{"os": "default", "uri": "https://portal.azure.com/#blade/HubsExtension/BrowseResource/resourceType/Microsoft.Sql%2Fservers%2Fdatabases"}],
            },
            {
                "@type": "OpenUri",
                "name": "📝 Add RCA to Knowledge Base",
                "targets": [{"os": "default", "uri": os.environ.get("STORAGE_PORTAL_URL", "https://portal.azure.com")}],
            },
        ],
    }

    response = requests.post(
        webhook_url,
        data=json.dumps(card),
        headers={"Content-Type": "application/json"},
        timeout=10,
    )
    response.raise_for_status()


# ── Knowledge section builder ─────────────────────────────────────────────────

def _build_knowledge_section(
    ai_suggestion: dict,
    rcas:          list,
    system_docs:   list,
) -> dict:
    rca_reference = ai_suggestion.get("rca_reference")

    if not rcas and not system_docs:
        return {
            "title":    "📚 Knowledge Base",
            "markdown": True,
            "text": (
                "*No matching context found in knowledge bases.*\n"
                "AI used general Azure SRE expertise.\n"
                "Add RCAs and re-run `python pipeline/index-all.py` to improve future suggestions."
            ),
        }

    lines = []

    if rcas:
        lines.append("**Past incidents from verified RCAs:**\n")
        for i, rca in enumerate(rcas, 1):
            lines.append(
                f"**{i}. {rca.get('rca_id', 'N/A')}** "
                f"(Match: {rca.get('similarity', '?')} · {rca.get('duration_minutes', '?')} min)\n"
                f"- Service: {rca.get('service', 'N/A')}\n"
                f"- What happened: {rca.get('error_summary', 'N/A')[:120]}\n"
                f"- Resolution: {rca.get('resolution', 'N/A')[:150]}\n"
                f"- Resolved by: {rca.get('resolved_by', 'N/A')} · {str(rca.get('date_resolved', ''))[:10]}\n"
            )

    if system_docs:
        lines.append("\n**Architecture / runbook sections used:**\n")
        for doc in system_docs:
            lines.append(
                f"- [{doc.get('title', 'N/A')}]"
                f"({doc.get('source_file', '')}) "
                f"(Match: {doc.get('similarity', '?')})\n"
            )

    if rca_reference:
        lines.append(f"\n🤖 *AI note: {rca_reference}*")

    total = len(rcas) + len(system_docs)
    return {
        "title":    f"📚 Knowledge Base ({total} source(s) used)",
        "markdown": True,
        "text":     "\n".join(lines),
    }


# ── Priority helpers ──────────────────────────────────────────────────────────

def _priority_style(priority: str) -> tuple:
    return {
        "P1": ("FF0000", "🔴"),
        "P2": ("FF6600", "🟠"),
        "P3": ("FFAA00", "🟡"),
    }.get(priority, ("808080", "⚪"))
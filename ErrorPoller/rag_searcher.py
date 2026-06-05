"""
ErrorPoller/rag_searcher.py
────────────────────────────
Searches system-knowledge-base and rca-knowledge-base in Azure AI Search.
Returns typed results so Claude knows what kind of context it received.

Signal Classification — v4 (fully open vocabulary)
────────────────────────────────────────────────────
v2 used four hardcoded keyword sets: _BLOB_SIGNALS, _SQL_SIGNALS,
_DNS_SIGNALS, _AUTH_SIGNALS. That worked for the five known failure modes.

v3 replaced keyword sets with a Claude call returning four fixed booleans
{blob, auth, dns, sql}. Better — but still a closed vocabulary. An ADF
pipeline timeout, an NSG rule block, a Key Vault soft-delete, an ARM
throttling event, or a custom application exception would either map
badly to one of four buckets or be missed entirely.

v4 removes the fixed category list entirely. A single Claude call now
returns TWO things:

  1. signals — a free-text list of affected technology domains, e.g.
     ["Azure Data Factory", "Blob Storage", "Managed Identity"]
     Claude infers these from the error context, not from any preset list.

  2. query_plan — for each signal, the chunk_type to search and a short
     targeted query string optimised for that domain. Claude writes the
     query strings itself, so infrastructure chunks are retrieved using
     the vocabulary that actually appears in those chunks, not the
     vocabulary that appears in the error.

The RAG layer executes exactly the queries Claude specifies, plus two
fixed queries (failure_modes, resolution_steps) that always run.

Example — NSG-blocked SQL connection:
  signals:    ["Azure SQL", "VNet NSG", "Private Endpoint"]
  query_plan: [
    { "chunk_type": "infrastructure",
      "query": "NSG outbound rule TCP 1433 SQL private endpoint snet-functions pe-sql-poc01" },
    { "chunk_type": "schema",
      "query": "dbo.ProcessingRunLog dbo.Positions dbo.ApplicationErrors" }
  ]

Example — ADF timeout, blob never arrived:
  signals:    ["Azure Data Factory", "Blob Storage"]
  query_plan: [
    { "chunk_type": "infrastructure",
      "query": "ADF pipeline blob storage copy activity trades container upload schedule" }
  ]

Fallback
────────
If the Claude call fails, _build_query_plan() falls back to a single
infrastructure query using the raw error text. The two fixed queries
(failure_modes, resolution_steps) still run, so the pipeline is never
fully blind.

Threshold guidance
──────────────────
With a small corpus (< 20 RCAs) the semantic reranker rarely exceeds 2.5
even on strong matches. Safe starting point: RCA >= 1.2, system docs >= 1.0.
Set AIOPS_LOG_ALL_RAG_SCORES=true to log every pre-threshold candidate.
"""

import json
import logging
import os
import re
from typing import Optional

import anthropic
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
from azure.search.documents.models import VectorizedQuery
from openai import AzureOpenAI

logger = logging.getLogger(__name__)

_search_clients: dict[str, SearchClient] = {}
_openai_client: Optional[AzureOpenAI]   = None

# ── Thresholds ────────────────────────────────────────────────────────────────
RCA_SEMANTIC_THRESHOLD    = float(os.environ.get("RAG_RCA_SEMANTIC_THRESHOLD",    "1.2"))
RCA_VECTOR_THRESHOLD      = float(os.environ.get("RAG_RCA_VECTOR_THRESHOLD",      "0.70"))
SYSTEM_SEMANTIC_THRESHOLD = float(os.environ.get("RAG_SYSTEM_SEMANTIC_THRESHOLD", "1.0"))
SYSTEM_RRF_THRESHOLD      = float(os.environ.get("RAG_SYSTEM_RRF_THRESHOLD",      "0.01"))

_LOG_ALL_SCORES = os.environ.get("AIOPS_LOG_ALL_RAG_SCORES", "false").lower() == "true"

# ── Query plan via Claude ─────────────────────────────────────────────────────

_PLAN_PROMPT = """\
You are an Azure SRE building a retrieval query plan for an error in a
Finance Position Processing system running on Azure Durable Functions.

The knowledge base contains chunked architecture documentation indexed
under these chunk_type values:
  failure_modes    — known exception patterns per activity
  infrastructure   — VNet, subnets, private endpoints, NSG, DNS, RBAC, MSI, Key Vault, storage firewall
  resolution_steps — manual fix procedures, retrigger commands, SQL user creation scripts
  schema           — dbo.Positions, dbo.ProcessingRunLog, dbo.ApplicationErrors column definitions
  kql_queries      — pre-built KQL queries for Application Insights
  general          — unclassified architecture content

Given the error text below, return ONLY a JSON object with this exact
structure — no markdown, no explanation:

{{
  "signals": [<list of affected Azure service or technology domains as plain strings>],
  "query_plan": [
    {{
      "chunk_type": <one of the chunk_type values above>,
      "query": <a short targeted search string using vocabulary that would appear IN the matching documentation chunk, not vocabulary from the error itself>
    }}
  ]
}}

Rules for signals:
  - Use specific Azure service names: "Azure SQL", "Azure Blob Storage",
    "Azure Data Factory", "VNet NSG", "Private Endpoint", "DNS",
    "Managed Identity", "Key Vault", "App Service Plan", "Durable Functions",
    "SqlBulkCopy", "Azure Service Bus", "Azure Cache for Redis", etc.
  - Include all domains you can infer from context, not just explicit keywords.
    "No connection to 10.0.2.5:1433" -> ["Azure SQL", "VNet NSG", "Private Endpoint"]
    "ADF pipeline CopyTradesToBlob did not complete" -> ["Azure Data Factory", "Azure Blob Storage"]
    "Login failed for user id-positionprocessor" -> ["Azure SQL", "Managed Identity"]

Rules for query_plan:
  - Include an infrastructure entry whenever the error could involve
    networking, identity, firewall, private endpoints, DNS, or RBAC --
    even if those words do not appear in the error. Write the query using
    the vocabulary that appears in infrastructure documentation.
  - Include a schema entry only if the error involves SQL tables or data.
  - Do NOT include failure_modes or resolution_steps -- those always run
    separately and do not need to appear here.
  - Maximum 4 entries in query_plan.
  - Each query string should be 10-25 words of domain-specific vocabulary.

ERROR TEXT:
{error_text}
"""


def _build_query_plan(error_text: str) -> dict:
    """
    Calls Claude to produce a dynamic query plan for this specific error.

    Returns:
      {
        "signals":    ["Azure SQL", "Managed Identity", ...],
        "query_plan": [
          {"chunk_type": "infrastructure", "query": "..."},
          ...
        ]
      }

    Falls back to a single generic infrastructure query if the call fails.

    Fixes vs v4 original
    ─────────────────────
    1. max_tokens raised 400 → 800. The JSON plan for a 3-signal, 4-entry
       error was regularly hitting the token ceiling mid-object, producing
       truncated JSON and a JSONDecodeError whose str() started with the
       fragment that was next in the stream (e.g. '\\n  "signals"').
    2. stop_reason check added immediately after the API call. A truncated
       response now raises with a clear message before json.loads is reached.
    3. Exception logging now includes type(exc).__name__ so the log line
       distinguishes JSONDecodeError / ValueError / APIError / etc.
    """
    try:
        client = anthropic.Anthropic(
            api_key=os.environ["CLAUDE_FOUNDRY_KEY"],
            base_url=os.environ["CLAUDE_FOUNDRY_ENDPOINT"],
        )
        response = client.messages.create(
            model=os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-5"),
            max_tokens=800,   # raised from 400 — plan JSON can be 300-600 tokens
            temperature=0,
            messages=[{
                "role": "user",
                "content": _PLAN_PROMPT.format(error_text=error_text[:2000]),
            }],
        )

        # Detect truncation before attempting to parse.  A truncated JSON
        # object causes json.loads to raise JSONDecodeError; the str() of
        # that exception starts with the fragment that was next in the
        # stream, which looks like a misleading value in the log.
        if response.stop_reason == "max_tokens":
            tail = response.content[0].text[-120:] if response.content else ""
            raise ValueError(
                f"Claude response truncated at max_tokens limit; "
                f"increase budget or shorten prompt. tail={tail!r}"
            )

        raw = response.content[0].text

        # Extract the first {...} block — handles any combination of:
        #   - leading/trailing whitespace or newlines
        #   - ```json...``` or ```...``` fences
        #   - preamble text before the JSON ("Here is the plan:\n{...")
        # re.DOTALL so . matches newlines inside the JSON object.
        match = re.search(r'\{.*\}', raw, re.DOTALL)
        if not match:
            logger.warning(
                "Claude query plan: no JSON object found in response. "
                "stop_reason=%s raw=%r",
                response.stop_reason, raw[:300],
            )
            raise ValueError("No JSON object in Claude response")

        plan = json.loads(match.group())
        signals    = plan.get("signals", [])
        query_plan = plan.get("query_plan", [])

        logger.info(
            "Claude query plan: signals=%s  queries=%d",
            signals, len(query_plan),
        )
        return {"signals": signals, "query_plan": query_plan}

    except Exception as exc:
        # Include the exception *type* in the log so the message
        # distinguishes JSONDecodeError / ValueError / APIError / etc.
        logger.warning(
            "Claude query plan failed (%s: %s) — using fallback infrastructure query",
            type(exc).__name__, str(exc)[:120],
        )
        return {
            "signals": [],
            "query_plan": [
                {
                    "chunk_type": "infrastructure",
                    "query": error_text[:300],
                }
            ],
        }


# ── Public API ────────────────────────────────────────────────────────────────

def search_knowledge_bases(
    error_text:  str,
    service:     str,
    stack_trace: str = "",
    top_k:       int = 3,
) -> dict:
    """
    Search both indexes and return:
      { "rcas": [...], "system_docs": [...], "signals": [...] }

    error_text should contain only diagnostic signal (ErrorMessage +
    ExceptionType + Stage). Do NOT include RunId, timestamps, or business
    dates -- those dilute the embedding and cause the retriever to return
    scheduling/metadata chunks instead of failure-mode and infrastructure
    chunks.
    """
    oai  = _get_openai_client()
    plan = _build_query_plan(error_text)

    embedding   = _embed(oai, error_text)
    rcas        = _search_rcas(embedding, error_text, service, top_k)
    system_docs = _search_system(embedding, error_text, service, plan, top_k=4)

    return {
        "rcas":        rcas,
        "system_docs": system_docs,
        "signals":     plan.get("signals", []),  # surfaced in Teams card
    }


# ── RCA search ────────────────────────────────────────────────────────────────

def _search_rcas(
    embedding:  list[float],
    error_text: str,
    service:    str,
    top_k:      int,
) -> list[dict]:
    client = _get_search_client("rca-knowledge-base")
    vq     = VectorizedQuery(
        vector=embedding,
        k_nearest_neighbors=50,
        fields="embedding",
        exhaustive=False,
    )
    try:
        results = client.search(
            search_text=_keywords(error_text),
            vector_queries=[vq],
            query_type="semantic",
            semantic_configuration_name="rca-semantic",
            select=[
                "rca_id", "service", "severity", "error_summary",
                "root_cause", "resolution", "prevention",
                "date_resolved", "resolved_by", "duration_minutes",
            ],
            top=top_k + 3,
        )
    except Exception as exc:
        logger.warning("RCA semantic search failed, trying vector-only: %s", exc)
        results = client.search(
            search_text=None,
            vector_queries=[vq],
            select=[
                "rca_id", "service", "severity", "error_summary",
                "root_cause", "resolution", "prevention",
                "date_resolved", "resolved_by", "duration_minutes",
            ],
            top=top_k,
        )

    out: list[dict] = []
    for r in results:
        reranker = r.get("@search.reranker_score")
        score    = r.get("@search.score", 0)

        if reranker is not None:
            if _LOG_ALL_SCORES:
                logger.info(
                    "RAG score [RCA pre-filter] rca_id=%s semantic=%.3f threshold=%.2f",
                    r.get("rca_id", "?"), reranker, RCA_SEMANTIC_THRESHOLD,
                )
            if reranker < RCA_SEMANTIC_THRESHOLD:
                continue
            score_display = f"{reranker:.2f}/4.0 (semantic)"
        else:
            if _LOG_ALL_SCORES:
                logger.info(
                    "RAG score [RCA pre-filter] rca_id=%s vector=%.3f threshold=%.2f",
                    r.get("rca_id", "?"), score, RCA_VECTOR_THRESHOLD,
                )
            if score < RCA_VECTOR_THRESHOLD:
                continue
            score_display = f"{score:.0%} (vector)"

        out.append({
            "result_type":      "rca",
            "rca_id":           r.get("rca_id", ""),
            "service":          r.get("service", ""),
            "severity":         r.get("severity", ""),
            "error_summary":    r.get("error_summary", ""),
            "root_cause":       r.get("root_cause", ""),
            "resolution":       r.get("resolution", ""),
            "prevention":       r.get("prevention", ""),
            "date_resolved":    str(r.get("date_resolved", ""))[:10],
            "resolved_by":      r.get("resolved_by", ""),
            "duration_minutes": r.get("duration_minutes", 0),
            "similarity":       score_display,
        })

    logger.info("RCA index: %d result(s) passed threshold", len(out))
    return out[:top_k]


# ── System doc search ─────────────────────────────────────────────────────────

def _search_system(
    embedding:  list[float],
    error_text: str,
    service:    str,
    plan:       dict,
    top_k:      int,
) -> list[dict]:
    """
    Executes the Claude-generated query plan against system-knowledge-base,
    plus two fixed queries that always run regardless of error type:

      Fixed Q1 — failure_modes   (finds the activity + exception pattern)
      Fixed Q2 — resolution_steps (retrieves known fix procedures)

    The dynamic queries from Claude's plan cover everything else:
    infrastructure, schema, kql_queries, or general chunks as needed,
    using query strings Claude crafted to match documentation vocabulary.
    """
    svc_filter = f"service eq '{service}' or service eq 'platform'" if service else None
    seen_ids:  set[str]   = set()
    all_hits:  list[dict] = []

    def _run_query(text: str, chunk_type: str, label: str, n: int = 2) -> None:
        type_filter = f"chunk_type eq '{chunk_type}'"
        combined    = f"({svc_filter}) and ({type_filter})" if svc_filter else type_filter
        vq = VectorizedQuery(
            vector=_embed(_get_openai_client(), text),
            k_nearest_neighbors=20,
            fields="embedding",
            exhaustive=False,
        )
        try:
            results = _get_search_client("system-knowledge-base").search(
                search_text=_keywords(text),
                vector_queries=[vq],
                query_type="semantic",
                semantic_configuration_name="system-semantic",
                select=["id", "title", "content", "doc_type",
                        "chunk_type", "service", "source_file"],
                filter=combined,
                top=n,
            )
        except Exception as exc:
            logger.warning("System query [%s] semantic failed, trying hybrid: %s", label, exc)
            try:
                results = _get_search_client("system-knowledge-base").search(
                    search_text=_keywords(text),
                    vector_queries=[vq],
                    select=["id", "title", "content", "doc_type",
                            "chunk_type", "service", "source_file"],
                    filter=combined,
                    top=n,
                )
            except Exception as exc2:
                logger.warning("System query [%s] hybrid also failed: %s", label, exc2)
                return

        for r in results:
            chunk_id = r.get("id", "")
            if chunk_id in seen_ids:
                continue

            reranker = r.get("@search.reranker_score")
            score    = r.get("@search.score", 0)

            if reranker is not None:
                if _LOG_ALL_SCORES:
                    logger.info(
                        "RAG score [System/%s pre-filter] title=%s semantic=%.3f threshold=%.2f",
                        label, r.get("title", "?")[:40], reranker, SYSTEM_SEMANTIC_THRESHOLD,
                    )
                if reranker < SYSTEM_SEMANTIC_THRESHOLD:
                    continue
                score_display = f"{reranker:.2f}/4.0 (semantic)"
                sort_key      = reranker
            else:
                if _LOG_ALL_SCORES:
                    logger.info(
                        "RAG score [System/%s pre-filter] title=%s hybrid=%.4f threshold=%.4f",
                        label, r.get("title", "?")[:40], score, SYSTEM_RRF_THRESHOLD,
                    )
                if score < SYSTEM_RRF_THRESHOLD:
                    continue
                score_display = f"{score:.4f} (hybrid)"
                sort_key      = score

            seen_ids.add(chunk_id)
            logger.info(
                "System doc accepted [query=%s]: score=%s title=%s",
                label, score_display, r.get("title", "")[:60],
            )
            all_hits.append({
                "result_type": "system_doc",
                "title":       r.get("title", ""),
                "content":     r.get("content", "")[:800],
                "doc_type":    r.get("doc_type", ""),
                "chunk_type":  r.get("chunk_type", ""),
                "service":     r.get("service", "platform"),
                "source_file": r.get("source_file", ""),
                "similarity":  score_display,
                "_sort_key":   sort_key,
                "_query":      label,
            })

    # ── Fixed queries — always run regardless of error type ───────────────────
    _run_query(error_text, chunk_type="failure_modes",    label="failure_mode", n=2)
    _run_query(
        text=f"{error_text} resolution fix manual trigger retrigger",
        chunk_type="resolution_steps",
        label="resolution",
        n=2,
    )

    # ── Dynamic queries — exactly what Claude planned ─────────────────────────
    query_plan = plan.get("query_plan", [])
    for i, entry in enumerate(query_plan[:4]):   # cap at 4 dynamic queries
        chunk_type = entry.get("chunk_type", "general")
        query_text = entry.get("query", "")
        if not query_text:
            continue
        _run_query(
            text=query_text,
            chunk_type=chunk_type,
            label=f"dynamic_{i}_{chunk_type}",
            n=2,
        )

    all_hits.sort(key=lambda h: h["_sort_key"], reverse=True)
    for hit in all_hits:
        hit.pop("_sort_key", None)
        hit.pop("_query",    None)

    logger.info(
        "System index: %d unique result(s) — 2 fixed + %d dynamic queries",
        len(all_hits), len(query_plan),
    )
    return all_hits[:top_k]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _keywords(text: str) -> str:
    text = text[:500]
    for noise in ("at System.", "at Microsoft.", "at DurableTask.",
                  "at PositionProcessor.", "in /home/", "line "):
        text = text.replace(noise, " ")
    return " ".join(text.split())


def _embed(client: AzureOpenAI, text: str) -> list[float]:
    resp = client.embeddings.create(
        model=os.environ.get("EMBEDDING_MODEL", "text-embedding-3-small"),
        input=text[:6000],
    )
    return resp.data[0].embedding


def _get_search_client(index_name: str) -> SearchClient:
    if index_name not in _search_clients:
        _search_clients[index_name] = SearchClient(
            endpoint=os.environ["SEARCH_ENDPOINT"],
            index_name=index_name,
            credential=AzureKeyCredential(os.environ["SEARCH_KEY"]),
        )
    return _search_clients[index_name]


def _get_openai_client() -> AzureOpenAI:
    global _openai_client
    if _openai_client is None:
        _openai_client = AzureOpenAI(
            api_key=os.environ["OPENAI_EMBEDDING_KEY"],
            azure_endpoint=os.environ["OPENAI_EMBEDDING_ENDPOINT"],
            api_version="2024-02-01",
        )
    return _openai_client
"""
ErrorPoller/rag_searcher.py
────────────────────────────
Searches system-knowledge-base and rca-knowledge-base in Azure AI Search.
Returns typed results so AI knows what kind of context it received.

KEY CHANGE from v1
──────────────────
The previous version ran a single generic vector query against system-knowledge-base.
That produced irrelevant chunks (HttpStarter, DailyPositionScheduler) for a 403 blob
error because the generic embedding drifted toward scheduler/orchestrator content.

This version runs MULTIPLE targeted queries with chunk_type filters:

  1. failure_modes   — always. Finds the specific activity + exception pattern.
  2. infrastructure  — always for blob/SQL/auth errors. Forces network + VNet +
                       private endpoint + RBAC chunks into context regardless of
                       what the generic embedding retrieves. This is the fix for
                       the 403 analysis missing the network layer entirely.
  3. resolution_steps — always. Retrieves known fix procedures.
  4. schema          — for SQL errors only.

Results from all queries are deduplicated by chunk id, re-ranked by semantic
score descending, and capped at top_k before being returned to the caller.

Threshold guidance
──────────────────
With a small corpus (< 20 RCAs) the semantic reranker rarely exceeds 2.5 even
on strong matches. Safe starting point: RCA ≥ 1.2, system docs ≥ 1.0.
Set AIOPS_LOG_ALL_RAG_SCORES=true to log every pre-threshold candidate score.
"""

import logging
import os
from typing import Optional

from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
from azure.search.documents.models import VectorizedQuery
from openai import AzureOpenAI

logger = logging.getLogger(__name__)

_search_clients: dict[str, SearchClient] = {}
_openai_client: Optional[AzureOpenAI]   = None

# ── Thresholds ────────────────────────────────────────────────────────────────
# Tune empirically once score distributions are visible in logs.
RCA_SEMANTIC_THRESHOLD    = float(os.environ.get("RAG_RCA_SEMANTIC_THRESHOLD",    "1.2"))
RCA_VECTOR_THRESHOLD      = float(os.environ.get("RAG_RCA_VECTOR_THRESHOLD",      "0.70"))
SYSTEM_SEMANTIC_THRESHOLD = float(os.environ.get("RAG_SYSTEM_SEMANTIC_THRESHOLD", "1.0"))
SYSTEM_RRF_THRESHOLD      = float(os.environ.get("RAG_SYSTEM_RRF_THRESHOLD",      "0.01"))

_LOG_ALL_SCORES = os.environ.get("AIOPS_LOG_ALL_RAG_SCORES", "false").lower() == "true"

# ── Error classification signals ──────────────────────────────────────────────
# Used to decide which supplementary chunk_type queries to fire.
_BLOB_SIGNALS   = {"blob", "storage", "403", "401", "authorization", "authorizationfailure",
                   "blobnotfound", "404", "requestfailedexception"}
_SQL_SIGNALS    = {"sql", "login failed", "timeout", "deadlock", "bulkcopy",
                   "sqlexception", "dtu"}
_DNS_SIGNALS    = {"dns", "no such host", "nxdomain", "host"}
_AUTH_SIGNALS   = {"403", "401", "authorization", "authorizationfailure",
                   "login failed", "managed identity"}


def _classify(message: str) -> set[str]:
    """Return a set of signal categories present in the error message."""
    lower  = message.lower()
    active = set()
    if any(s in lower for s in _BLOB_SIGNALS):   active.add("blob")
    if any(s in lower for s in _SQL_SIGNALS):    active.add("sql")
    if any(s in lower for s in _DNS_SIGNALS):    active.add("dns")
    if any(s in lower for s in _AUTH_SIGNALS):   active.add("auth")
    return active


# ── Public API ────────────────────────────────────────────────────────────────

def search_knowledge_bases(
    error_text:  str,
    service:     str,
    stack_trace: str = "",
    top_k:       int = 3,
) -> dict:
    """
    Search both indexes and return:
      { "rcas": [...], "system_docs": [...] }

    error_text should contain only diagnostic signal (ErrorMessage + ExceptionType
    + Stage). Do NOT include RunId, timestamps, or business dates — those dilute
    the embedding and cause the retriever to return scheduling/metadata chunks
    instead of failure-mode and infrastructure chunks.
    """
    oai       = _get_openai_client()
    signals   = _classify(error_text)
    embedding = _embed(oai, error_text)

    rcas        = _search_rcas(embedding, error_text, service, top_k)
    system_docs = _search_system(embedding, error_text, service, signals, top_k=4)

    return {"rcas": rcas, "system_docs": system_docs}


# ── RCA search (unchanged from v1 — was working correctly) ────────────────────

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


# ── System doc search — REWRITTEN ─────────────────────────────────────────────

def _search_system(
    embedding:  list[float],
    error_text: str,
    service:    str,
    signals:    set[str],
    top_k:      int,
) -> list[dict]:
    """
    Multi-query targeted retrieval against system-knowledge-base.

    Why multi-query instead of a single generic search:
      A single vector query embeds the full error text and finds the chunk whose
      embedding is closest in cosine space. For a 403 blob error the top result
      tends to be LoadTradesActivity (correct) but positions 2-3 drift toward
      scheduler/orchestrator content because those chunks share vocabulary with
      the error context (RunId, BlobPath, ActivityName). The network/VNet chunk
      never surfaces because "private endpoint" and "DNS zone" are not in the
      error text at all — they are the *missing* context, not present context.

      Running separate targeted queries with chunk_type filters guarantees that
      infrastructure and resolution_steps chunks are always retrieved for the
      error categories that need them, regardless of what the generic embedding
      finds.

    Query plan:
      Q1  failure_modes   — always — finds the activity+exception match
      Q2  infrastructure  — blob|auth|dns errors — forces VNet/endpoint/RBAC chunks
      Q3  resolution_steps — always — retrieves known fix procedures
      Q4  schema          — SQL errors only — pulls table definitions if relevant

    Results are deduplicated by chunk id, sorted by semantic score descending,
    capped at top_k.
    """
    svc_filter = f"service eq '{service}' or service eq 'platform'" if service else None
    seen_ids:  set[str]   = set()
    all_hits:  list[dict] = []

    def _run_query(
        text:       str,
        chunk_type: str,
        label:      str,
        n:          int = 2,
    ) -> None:
        """Execute one targeted query and accumulate results into all_hits."""
        type_filter = f"chunk_type eq '{chunk_type}'"
        combined    = (
            f"({svc_filter}) and ({type_filter})"
            if svc_filter else type_filter
        )
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

    # ── Q1: failure mode — always ─────────────────────────────────────────────
    _run_query(
        text=error_text,
        chunk_type="failure_modes",
        label="failure_mode",
        n=2,
    )

    # ── Q2: infrastructure — blob, auth, or DNS errors ────────────────────────
    # This is the critical query that was missing in v1.
    # A 403 on Azure Blob Storage with public access disabled can be caused by:
    #   - Private endpoint misconfiguration (network layer)
    #   - Missing RBAC role assignment (identity layer)
    # Both are documented in infrastructure chunks (sections 5.1, 5.3, 6.1).
    # A generic embedding of the 403 error text never retrieves these chunks
    # because "VNet", "private endpoint", "DNS zone" are not in the error.
    # The targeted query with explicit infrastructure vocabulary forces retrieval.
    if signals & {"blob", "auth", "dns"}:
        _run_query(
            text=(
                "VNet private endpoint DNS zone blob storage network access "
                "snet-functions vnet_route_all_enabled managed identity RBAC "
                "Storage Blob Data Reader id-positionprocessor pe-storage-poc01"
            ),
            chunk_type="infrastructure",
            label="network_and_rbac",
            n=3,
        )

    # ── Q3: resolution steps — always ────────────────────────────────────────
    _run_query(
        text=f"{error_text} resolution fix manual trigger retrigger",
        chunk_type="resolution_steps",
        label="resolution",
        n=2,
    )

    # ── Q4: schema — SQL errors only ──────────────────────────────────────────
    if "sql" in signals:
        _run_query(
            text="dbo.ProcessingRunLog dbo.Positions dbo.ApplicationErrors schema columns",
            chunk_type="schema",
            label="schema",
            n=1,
        )

    # ── Deduplicate, sort by score, cap at top_k ──────────────────────────────
    all_hits.sort(key=lambda h: h["_sort_key"], reverse=True)
    for hit in all_hits:
        hit.pop("_sort_key", None)
        hit.pop("_query",    None)

    logger.info(
        "System index: %d unique result(s) from %d targeted queries",
        len(all_hits), sum(1 for s in ("blob", "auth", "dns", "sql") if s in signals) + 2,
    )
    return all_hits[:top_k]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _keywords(text: str) -> str:
    """
    Strip stack-frame noise before BM25 to avoid irrelevant keyword hits on
    common method names (CallActivityAsync, RunOrchestrator, etc.).
    """
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
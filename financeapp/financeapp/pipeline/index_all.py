"""
pipeline/index_all.py
─────────────────────────────────────────────────────────────────────────────
Enterprise Knowledge Base Indexer — Finance Position Processor AIOps
─────────────────────────────────────────────────────────────────────────────

Architecture
────────────
Two Azure AI Search indexes are maintained:

  system-knowledge-base
      Chunked architecture / infrastructure / runbook markdown docs.
      Hybrid search: BM25 keyword + HNSW vector + semantic reranker.
      Filterable by: doc_type, service, chunk_type.
      chunk_type discriminates failure-mode chunks from schema, KQL, and
      resolution chunks so the ErrorPoller can bias retrieval at query time.

  rca-knowledge-base
      One document per past incident RCA (JSON).
      Vector + semantic + recency scoring (P180D freshness boost).
      Filterable by: service, severity.

Operational guarantees
──────────────────────
  Idempotent      — upsert on every upload; safe to rerun.
  Incremental     -- SHA-256 hash per file; unchanged files are skipped.
  Retry           — exponential backoff on transient Azure / OpenAI errors;
                    hard-fail on 400 / 401 / 403 / 404.
  Concurrency     — configurable ThreadPoolExecutor for embedding generation;
                    backpressure-aware batch upload with configurable size.
  Frontmatter     — YAML frontmatter parsed from markdown files;
                    last_updated sourced from doc, not indexing timestamp.
  Validation      — schema check before upload; connectivity smoke-test at
                    startup; mandatory required-field check for RCAs.
  Observability   — structured JSON log lines at DEBUG; human-readable at INFO.
  Dry-run         — full pipeline simulation with no API calls.
  State           — per-file hash stored in .index-state.json for incremental.

Usage
─────
  pip install -r requirements.txt

  # First run — create indexes and index everything
  python pipeline/index_all.py --full

  # After editing a doc or adding a new RCA
  python pipeline/index_all.py --incremental

  # Preview without calling APIs
  python pipeline/index_all.py --dry-run

  # Rebuild only one index
  python pipeline/index_all.py --full --only rcas
  python pipeline/index_all.py --full --only docs

  # Debug verbosity
  python pipeline/index_all.py --incremental --verbose

Environment variables  (set in .env or shell)
─────────────────────────────────────────────
  SEARCH_ENDPOINT             Azure AI Search endpoint URL
  SEARCH_KEY                  Azure AI Search admin key
  OPENAI_EMBEDDING_ENDPOINT   Azure OpenAI endpoint URL
  OPENAI_EMBEDDING_KEY        Azure OpenAI API key
  EMBEDDING_MODEL             Deployment name (e.g. text-embedding-3-small)

Optional tuning (safe defaults provided)
─────────────────────────────────────────
  CHUNK_SIZE          int   characters per chunk          default 1800
  CHUNK_OVERLAP       int   overlap characters            default 150
  BATCH_SIZE          int   documents per upload batch    default 10
  EMBED_CONCURRENCY   int   parallel embedding threads    default 4
  MAX_RETRIES         int   transient retry limit         default 4
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import logging.config
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Optional

from azure.core.credentials import AzureKeyCredential
from azure.core.exceptions import HttpResponseError, ServiceRequestError
from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import (
    FreshnessScoringFunction,
    FreshnessScoringParameters,
    HnswAlgorithmConfiguration,
    ScoringFunctionInterpolation,
    ScoringProfile,
    SearchField,
    SearchFieldDataType,
    SearchIndex,
    SearchableField,
    SemanticConfiguration,
    SemanticField,
    SemanticPrioritizedFields,
    SemanticSearch,
    SimpleField,
    VectorSearch,
    VectorSearchProfile,
)
from dotenv import load_dotenv
from openai import AzureOpenAI

load_dotenv()

# ── Logging ───────────────────────────────────────────────────────────────────

def _configure_logging(verbose: bool) -> logging.Logger:
    """
    Two handlers:
      - Console  → INFO (or DEBUG with --verbose), human-readable.
      - File     → DEBUG always, JSON-structured for log aggregators.
    """
    level = logging.DEBUG if verbose else logging.INFO

    log_dir = Path(__file__).parent / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"indexer-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}.log"

    class _JsonFormatter(logging.Formatter):
        def format(self, record: logging.LogRecord) -> str:
            payload = {
                "ts":      self.formatTime(record, datefmt="%Y-%m-%dT%H:%M:%S"),
                "level":   record.levelname,
                "logger":  record.name,
                "msg":     record.getMessage(),
            }
            if record.exc_info:
                payload["exc"] = self.formatException(record.exc_info)
            return json.dumps(payload)

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(level)
    console.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(_JsonFormatter())

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.addHandler(console)
    root.addHandler(fh)

    return logging.getLogger("indexer")


log: logging.Logger = logging.getLogger("indexer")  # replaced in main()


# ── Configuration ─────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Config:
    search_endpoint: str
    search_key: str
    openai_endpoint: str
    openai_key: str
    embedding_model: str

    embedding_dims: int   = 1536
    chunk_size: int       = 1800
    chunk_overlap: int    = 150
    batch_size: int       = 10
    embed_concurrency: int = 4
    max_retries: int      = 4
    retry_base_secs: float = 2.0

    docs_dir: Path        = field(default_factory=lambda: Path(__file__).parent.parent / "docs")
    rcas_dir: Path        = field(default_factory=lambda: Path(__file__).parent.parent / "rcas")
    state_file: Path      = field(default_factory=lambda: Path(__file__).parent / ".index-state.json")

    @classmethod
    def from_env(cls) -> "Config":
        def _req(name: str) -> str:
            val = os.environ.get(name, "").strip()
            if not val:
                log.error("Missing required environment variable: %s", name)
                sys.exit(1)
            return val

        def _int(name: str, default: int) -> int:
            raw = os.environ.get(name, "").strip()
            if not raw:
                return default
            try:
                return int(raw)
            except ValueError:
                log.warning("Invalid int for %s=%r — using default %d", name, raw, default)
                return default

        return cls(
            search_endpoint     = _req("SEARCH_ENDPOINT"),
            search_key          = _req("SEARCH_KEY"),
            openai_endpoint     = _req("OPENAI_EMBEDDING_ENDPOINT"),
            openai_key          = _req("OPENAI_EMBEDDING_KEY"),
            embedding_model     = _req("EMBEDDING_MODEL"),
            chunk_size          = _int("CHUNK_SIZE", 1800),
            chunk_overlap       = _int("CHUNK_OVERLAP", 150),
            batch_size          = _int("BATCH_SIZE", 10),
            embed_concurrency   = _int("EMBED_CONCURRENCY", 4),
            max_retries         = _int("MAX_RETRIES", 4),
        )


# ── Retry ─────────────────────────────────────────────────────────────────────

def with_retry(cfg: Config, fn, *args, label: str = "", **kwargs) -> Any:
    """
    Exponential backoff wrapper.
    Retries:   ServiceRequestError, HttpResponseError 429 / 503 / 504.
    Raises:    Immediately on 400 / 401 / 403 / 404 (non-retryable).
    """
    for attempt in range(1, cfg.max_retries + 1):
        try:
            return fn(*args, **kwargs)
        except HttpResponseError as exc:
            if exc.status_code in (400, 401, 403, 404):
                raise
            if attempt == cfg.max_retries:
                log.error("Exhausted retries for %s after %d attempts", label, attempt)
                raise
            wait = cfg.retry_base_secs ** attempt
            log.warning(
                "HTTP %s on '%s' — retry %d/%d in %.1fs",
                exc.status_code, label, attempt, cfg.max_retries, wait,
            )
            time.sleep(wait)
        except ServiceRequestError:
            if attempt == cfg.max_retries:
                raise
            wait = cfg.retry_base_secs ** attempt
            log.warning(
                "Network error on '%s' — retry %d/%d in %.1fs",
                label, attempt, cfg.max_retries, wait,
            )
            time.sleep(wait)

    # Unreachable but satisfies type checker
    raise RuntimeError(f"Retry loop exited unexpectedly for {label}")


# ══════════════════════════════════════════════════════════════════════════════
# INDEX DEFINITIONS
# ══════════════════════════════════════════════════════════════════════════════

def _hnsw_vector_search(dims: int) -> VectorSearch:
    return VectorSearch(
        algorithms=[
            HnswAlgorithmConfiguration(
                name="hnsw",
                parameters={
                    "m": 4,
                    "efConstruction": 400,
                    "efSearch": 500,
                    "metric": "cosine",
                },
            )
        ],
        profiles=[VectorSearchProfile(name="hnsw-profile", algorithm_configuration_name="hnsw")],
    )


def _vector_field(dims: int) -> SearchField:
    return SearchField(
        name="embedding",
        type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
        searchable=True,
        vector_search_dimensions=dims,
        vector_search_profile_name="hnsw-profile",
    )


def system_index_definition(cfg: Config) -> SearchIndex:
    """
    system-knowledge-base

    Key addition vs original: chunk_type field (filterable, facetable).
    Values: failure_modes | schema | kql_queries | resolution_steps | general

    This lets the ErrorPoller boost failure_modes chunks when it has a
    structured ExceptionType, or resolution_steps chunks when it needs
    a remediation path.
    """
    fields = [
        SimpleField(name="id",           type=SearchFieldDataType.String,         key=True,  filterable=True),
        SimpleField(name="doc_id",       type=SearchFieldDataType.String,                    filterable=True),
        SimpleField(name="chunk_index",  type=SearchFieldDataType.Int32,          sortable=True, filterable=True),
        SimpleField(name="doc_type",     type=SearchFieldDataType.String,         filterable=True, facetable=True),
        SimpleField(name="chunk_type",   type=SearchFieldDataType.String,         filterable=True, facetable=True),
        SimpleField(name="service",      type=SearchFieldDataType.String,         filterable=True, facetable=True),
        SimpleField(name="source_file",  type=SearchFieldDataType.String,         filterable=True),
        # last_updated sourced from doc frontmatter, not indexing timestamp
        SimpleField(name="last_updated", type=SearchFieldDataType.DateTimeOffset, filterable=True, sortable=True),
        SimpleField(name="content_hash", type=SearchFieldDataType.String,         filterable=True),
        SearchableField(name="title",    type=SearchFieldDataType.String,         analyzer_name="en.microsoft"),
        SearchableField(name="content",  type=SearchFieldDataType.String,         analyzer_name="en.microsoft"),
        _vector_field(cfg.embedding_dims),
    ]

    semantic = SemanticSearch(
        configurations=[
            SemanticConfiguration(
                name="system-semantic",
                prioritized_fields=SemanticPrioritizedFields(
                    title_field=SemanticField(field_name="title"),
                    content_fields=[SemanticField(field_name="content")],
                    keywords_fields=[
                        SemanticField(field_name="service"),
                        SemanticField(field_name="doc_type"),
                        SemanticField(field_name="chunk_type"),
                    ],
                ),
            )
        ],
        default_configuration_name="system-semantic",
    )

    return SearchIndex(
        name="system-knowledge-base",
        fields=fields,
        vector_search=_hnsw_vector_search(cfg.embedding_dims),
        semantic_search=semantic,
    )


def rca_index_definition(cfg: Config) -> SearchIndex:
    """
    rca-knowledge-base

    One document per incident. Recency boost: P180D freshness on date_resolved.
    Additional field vs original: tags (searchable collection) for free-form
    labelling (e.g. "blob-storage", "managed-identity", "dns") to improve
    recall on novel error patterns that don't yet have an RCA.
    """
    fields = [
        SimpleField(name="id",               type=SearchFieldDataType.String,         key=True,  filterable=True),
        SimpleField(name="rca_id",           type=SearchFieldDataType.String,         filterable=True, sortable=True),
        SimpleField(name="severity",         type=SearchFieldDataType.String,         filterable=True, facetable=True),
        SimpleField(name="date_resolved",    type=SearchFieldDataType.DateTimeOffset, filterable=True, sortable=True),
        SimpleField(name="resolved_by",      type=SearchFieldDataType.String,         filterable=True),
        SimpleField(name="duration_minutes", type=SearchFieldDataType.Int32,          filterable=True, sortable=True),
        SimpleField(name="content_hash",     type=SearchFieldDataType.String,         filterable=True),
        SearchableField(name="service",      type=SearchFieldDataType.String,         filterable=True, facetable=True,
                        analyzer_name="en.microsoft"),
        SearchableField(name="error_summary", type=SearchFieldDataType.String,        analyzer_name="en.microsoft"),
        SearchableField(name="root_cause",    type=SearchFieldDataType.String,        analyzer_name="en.microsoft"),
        SearchableField(name="resolution",    type=SearchFieldDataType.String,        analyzer_name="en.microsoft"),
        SearchableField(name="prevention",    type=SearchFieldDataType.String,        analyzer_name="en.microsoft"),
        SearchField(
            name="tags",
            type=SearchFieldDataType.Collection(SearchFieldDataType.String),
            searchable=True,
            filterable=True,
            facetable=True,
            analyzer_name="en.microsoft",
        ),
        _vector_field(cfg.embedding_dims),
    ]

    semantic = SemanticSearch(
        configurations=[
            SemanticConfiguration(
                name="rca-semantic",
                prioritized_fields=SemanticPrioritizedFields(
                    title_field=SemanticField(field_name="error_summary"),
                    content_fields=[
                        SemanticField(field_name="root_cause"),
                        SemanticField(field_name="resolution"),
                    ],
                    keywords_fields=[
                        SemanticField(field_name="service"),
                        SemanticField(field_name="prevention"),
                    ],
                ),
            )
        ],
        default_configuration_name="rca-semantic",
    )

    scoring_profiles = [
        ScoringProfile(
            name="recency-boost",
            functions=[
                FreshnessScoringFunction(
                    field_name="date_resolved",
                    boost=2.0,
                    parameters=FreshnessScoringParameters(boosting_duration="P180D"),
                    interpolation=ScoringFunctionInterpolation.LINEAR,
                )
            ],
        )
    ]

    return SearchIndex(
        name="rca-knowledge-base",
        fields=fields,
        vector_search=_hnsw_vector_search(cfg.embedding_dims),
        semantic_search=semantic,
        scoring_profiles=scoring_profiles,
        default_scoring_profile="recency-boost",
    )


# ══════════════════════════════════════════════════════════════════════════════
# STATE TRACKING
# ══════════════════════════════════════════════════════════════════════════════

def load_state(cfg: Config) -> dict[str, str]:
    if cfg.state_file.exists():
        try:
            return json.loads(cfg.state_file.read_text(encoding="utf-8"))
        except Exception as exc:
            log.warning("Could not read state file (%s) — starting fresh", exc)
    return {}


def save_state(cfg: Config, state: dict[str, str]) -> None:
    cfg.state_file.write_text(json.dumps(state, indent=2), encoding="utf-8")
    log.debug("State written to %s", cfg.state_file)


def file_sha256(path: Path) -> str:
    """First 16 hex chars of SHA-256 — sufficient for change detection."""
    return hashlib.sha256(path.read_bytes()).hexdigest()[:16]


def is_changed(state: dict[str, str], key: str, current_hash: str) -> bool:
    return state.get(key) != current_hash


# ══════════════════════════════════════════════════════════════════════════════
# INDEX MANAGEMENT
# ══════════════════════════════════════════════════════════════════════════════

def create_or_update_indexes(
    index_client: SearchIndexClient,
    cfg: Config,
    full: bool,
) -> None:
    for defn in [system_index_definition(cfg), rca_index_definition(cfg)]:
        existing: Optional[SearchIndex] = None
        try:
            existing = index_client.get_index(defn.name)
        except HttpResponseError as exc:
            if exc.status_code != 404:
                raise

        if existing and not full:
            log.info("Index exists — skipping recreate: %s", defn.name)
            continue

        if existing and full:
            log.info("Full mode — deleting index: %s", defn.name)
            with_retry(cfg, index_client.delete_index, defn.name, label=f"delete:{defn.name}")
            time.sleep(2)

        result = with_retry(cfg, index_client.create_index, defn, label=f"create:{defn.name}")
        log.info("Created index: %s  (%d fields)", result.name, len(result.fields))


# ══════════════════════════════════════════════════════════════════════════════
# FRONTMATTER PARSING
# ══════════════════════════════════════════════════════════════════════════════

_FRONTMATTER_RE  = re.compile(r"^---\s*\n(.*?)\n---\s*\n", re.DOTALL)
# Matches simple scalar lines:  key: value  or  key: "value"
_FM_SCALAR_RE    = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)\s*:\s*(.+)$")


def _parse_frontmatter_block(block: str) -> dict:
    """
    Minimal YAML-subset parser (stdlib only, no PyYAML required).

    Handles the only frontmatter fields this codebase uses:
        service, doc_type, last_updated   (all simple scalar strings)

    Skips list items, nested blocks, and blank lines silently.
    Strips surrounding quotes from values.
    """
    meta: dict = {}
    for line in block.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = _FM_SCALAR_RE.match(line)
        if not m:
            continue  # list item or nested key -- not needed
        key, raw_val = m.group(1), m.group(2).strip()
        # Strip surrounding single or double quotes
        if len(raw_val) >= 2 and raw_val[0] in ('"', "'") and raw_val[-1] == raw_val[0]:
            raw_val = raw_val[1:-1]
        meta[key] = raw_val
    return meta


def parse_frontmatter(text: str) -> tuple[dict, str]:
    """
    Extract frontmatter block if present (stdlib only -- no PyYAML required).
    Returns (metadata_dict, body_without_frontmatter).
    Gracefully returns ({}, text) if frontmatter is absent or malformed.
    """
    match = _FRONTMATTER_RE.match(text)
    if not match:
        return {}, text
    try:
        meta = _parse_frontmatter_block(match.group(1))
        body = text[match.end():]
        return meta, body
    except Exception as exc:
        log.warning("Frontmatter parse error: %s -- ignoring", exc)
        return {}, text


def frontmatter_last_updated(meta: dict) -> str:
    """
    Return ISO-8601 UTC string from frontmatter last_updated field.
    Falls back to current UTC time if absent or unparseable.
    """
    raw = meta.get("last_updated")
    if raw is None:
        return datetime.now(timezone.utc).isoformat()

    if isinstance(raw, datetime):
        if raw.tzinfo is None:
            raw = raw.replace(tzinfo=timezone.utc)
        return raw.isoformat()

    if isinstance(raw, str):
        for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S"):
            try:
                parsed = datetime.strptime(raw, fmt)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                return parsed.isoformat()
            except ValueError:
                continue
        log.warning("Unparseable last_updated value %r — using now()", raw)

    return datetime.now(timezone.utc).isoformat()


# ══════════════════════════════════════════════════════════════════════════════
# CHUNK TYPE DETECTION
# ══════════════════════════════════════════════════════════════════════════════

# Ordered: more specific patterns first.
_CHUNK_TYPE_RULES: list[tuple[str, list[str]]] = [
    ("failure_modes",    ["failure mode", "blobnotfound", "formatexception", "sqlexception",
                          "requestfailedexception", "common failure", "bloberrorcode",
                          "rca-fin-", "login failed", "timeout expired"]),
    ("schema",           ["| column |", "| type |", "| notes |", "int identity", "nvarchar",
                          "datetime2", "decimal(", "datatype"]),
    ("kql_queries",      ["kusto", "| where ", "| project ", "| order by", "dependencies\n",
                          "traces\n", "| take "]),
    ("resolution_steps", ["resolution", "step 1:", "step 2:", "step 3:", "manually triggered",
                          "post /api/trigger", "create user [", "alter role "]),
    ("error_handling",   ["errorpoller", "processingrunlog", "applicationerrors",
                          "watermark", "structured json", "fullchain"]),
    ("infrastructure",   ["vnet", "subnet", "private endpoint", "dns", "managed identity",
                          "app service plan", "key vault", "blob storage", "azure sql"]),
]


def detect_chunk_type(title: str, content: str) -> str:
    """
    Classify a chunk into one of the discriminative types for filtered retrieval.
    Matching is case-insensitive against combined title+content.
    Returns the first matching type, or 'general'.
    """
    combined = (title + "\n" + content).lower()
    for chunk_type, signals in _CHUNK_TYPE_RULES:
        if any(sig in combined for sig in signals):
            return chunk_type
    return "general"


# ══════════════════════════════════════════════════════════════════════════════
# METADATA INFERENCE
# ══════════════════════════════════════════════════════════════════════════════

def infer_metadata(file_path: Path, frontmatter: dict) -> dict:
    """
    Merge frontmatter fields with path-based heuristics.
    Frontmatter wins when present; path heuristics are the fallback.
    """
    path_lower = str(file_path).lower()

    # doc_type: frontmatter > path heuristic
    doc_type = frontmatter.get("doc_type")
    if not doc_type:
        if "runbook" in path_lower:
            doc_type = "runbook"
        elif "infra" in path_lower:
            doc_type = "infrastructure"
        elif any(k in path_lower for k in ("system", "architecture", "knowledge")):
            doc_type = "architecture"
        else:
            doc_type = "general"

    # service: frontmatter > path heuristic
    service = frontmatter.get("service")
    if not service:
        if "scheduler" in path_lower:
            service = "Scheduler"
        elif any(k in path_lower for k in ("position", "processor")):
            service = "PositionProcessor"
        else:
            service = "platform"

    return {"doc_type": doc_type, "service": service}


# ══════════════════════════════════════════════════════════════════════════════
# MARKDOWN CHUNKING
# ══════════════════════════════════════════════════════════════════════════════

def chunk_markdown(text: str, source_file: str, cfg: Config) -> Iterator[dict]:
    """
    Section-aware, table-safe chunker.

    Algorithm:
      1. Split body at ## / ### headings — each section becomes a candidate chunk.
      2. Sections under CHUNK_SIZE are emitted as-is.
      3. Oversized sections are split line-by-line with CHUNK_OVERLAP carry-forward,
         never splitting inside a Markdown table (lines starting with '|').
      4. Chunks under 60 chars are discarded (headings with no meaningful body).
      5. Each chunk dict carries title, content, and detected chunk_type.
    """
    heading_re = re.compile(r"^(#{1,3}\s+.+)$", re.MULTILINE)
    parts = heading_re.split(text)

    current_title   = source_file
    current_content = ""

    def _flush(title: str, content: str) -> Iterator[dict]:
        content = content.strip()
        if len(content) < 60:
            return

        if len(content) <= cfg.chunk_size:
            yield {
                "title":      title,
                "content":    content,
                "chunk_type": detect_chunk_type(title, content),
            }
            return

        # Oversized section — split by line, preserving tables intact.
        lines    = content.split("\n")
        buf: list[str] = []
        buf_len  = 0
        sub_idx  = 0
        in_table = False

        for line in lines:
            is_table_row = line.strip().startswith("|")
            in_table     = is_table_row or (in_table and line.strip().startswith("|"))

            buf.append(line)
            buf_len += len(line) + 1

            if buf_len >= cfg.chunk_size and not in_table:
                chunk_text = "\n".join(buf).strip()
                if chunk_text:
                    label = title if sub_idx == 0 else f"{title} (part {sub_idx + 1})"
                    yield {
                        "title":      label,
                        "content":    chunk_text,
                        "chunk_type": detect_chunk_type(label, chunk_text),
                    }
                    sub_idx += 1
                    # Carry overlap — keep last N lines
                    keep    = max(1, cfg.chunk_overlap // 80)
                    buf     = buf[-keep:]
                    buf_len = sum(len(l) + 1 for l in buf)

        remaining = "\n".join(buf).strip()
        if len(remaining) >= 60:
            label = title if sub_idx == 0 else f"{title} (part {sub_idx + 1})"
            yield {
                "title":      label,
                "content":    remaining,
                "chunk_type": detect_chunk_type(label, remaining),
            }

    for part in parts:
        if heading_re.match(part):
            yield from _flush(current_title, current_content)
            current_title   = part.strip().lstrip("#").strip()
            current_content = ""
        else:
            current_content += part

    yield from _flush(current_title, current_content)


# ══════════════════════════════════════════════════════════════════════════════
# EMBEDDING — concurrent with backpressure
# ══════════════════════════════════════════════════════════════════════════════

def get_embedding(cfg: Config, openai_client: AzureOpenAI, text: str) -> list[float]:
    """Generate one embedding. Truncates to 6 000 chars (≈ 1 500 tokens)."""
    result = with_retry(
        cfg,
        openai_client.embeddings.create,
        model=cfg.embedding_model,
        input=text[:6000],
        label="embedding",
    )
    return result.data[0].embedding


def embed_batch_concurrent(
    cfg: Config,
    openai_client: AzureOpenAI,
    items: list[dict],
    text_fn,
) -> list[list[float]]:
    """
    Embed a list of items concurrently using a thread pool.
    text_fn(item) → str   extracts the text to embed from each item.
    Returns embeddings in the same order as items.
    """
    embeddings: list[Optional[list[float]]] = [None] * len(items)

    with ThreadPoolExecutor(max_workers=cfg.embed_concurrency) as pool:
        future_to_idx = {
            pool.submit(get_embedding, cfg, openai_client, text_fn(item)): idx
            for idx, item in enumerate(items)
        }
        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            try:
                embeddings[idx] = future.result()
            except Exception as exc:
                log.error("Embedding failed for item %d: %s", idx, exc)
                raise

    return embeddings  # type: ignore[return-value]


# ══════════════════════════════════════════════════════════════════════════════
# BATCH UPLOAD
# ══════════════════════════════════════════════════════════════════════════════

def upload_batch(
    cfg: Config,
    client: SearchClient,
    documents: list[dict],
    index_name: str,
) -> tuple[int, int]:
    results = with_retry(
        cfg,
        client.upload_documents,
        documents=documents,
        label=f"upload:{index_name}",
    )
    succeeded = sum(1 for r in results if r.succeeded)
    failed    = sum(1 for r in results if not r.succeeded)
    for r in results:
        if not r.succeeded:
            log.error("Upload failed key=%s error=%s", r.key, r.error_message)
    return succeeded, failed


def upload_all(
    cfg: Config,
    client: SearchClient,
    documents: list[dict],
    index_name: str,
) -> tuple[int, int]:
    total_ok, total_fail = 0, 0
    for start in range(0, len(documents), cfg.batch_size):
        batch        = documents[start : start + cfg.batch_size]
        ok, fail     = upload_batch(cfg, client, batch, index_name)
        total_ok    += ok
        total_fail  += fail
        log.debug(
            "Batch %d-%d → %d ok, %d fail",
            start, start + len(batch) - 1, ok, fail,
        )
    return total_ok, total_fail


# ══════════════════════════════════════════════════════════════════════════════
# SYSTEM DOCS INDEXER
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class IndexStats:
    files_found:   int = 0
    files_skipped: int = 0
    files_indexed: int = 0
    chunks_total:  int = 0
    docs_uploaded: int = 0
    docs_failed:   int = 0
    elapsed_secs:  float = 0.0

    def __str__(self) -> str:
        return (
            f"files={self.files_found} "
            f"skipped={self.files_skipped} "
            f"indexed={self.files_indexed} "
            f"chunks={self.chunks_total} "
            f"uploaded={self.docs_uploaded} "
            f"failed={self.docs_failed} "
            f"elapsed={self.elapsed_secs:.1f}s"
        )


def index_system_docs(
    cfg: Config,
    search_client: SearchClient,
    openai_client: AzureOpenAI,
    state: dict[str, str],
    incremental: bool,
    dry_run: bool,
) -> IndexStats:
    stats = IndexStats()
    t0    = time.time()

    md_files = sorted(cfg.docs_dir.rglob("*.md"))
    stats.files_found = len(md_files)

    if not md_files:
        log.warning("No markdown files found in %s", cfg.docs_dir)
        stats.elapsed_secs = time.time() - t0
        return stats

    log.info("Found %d markdown file(s) in %s", len(md_files), cfg.docs_dir)

    all_docs: list[dict] = []

    for file_path in md_files:
        fhash  = file_sha256(file_path)
        sk     = f"sys:{file_path.name}"
        doc_id = file_path.stem.lower().replace(" ", "-").replace("_", "-")

        if incremental and not is_changed(state, sk, fhash):
            log.info("  SKIP (unchanged): %s", file_path.name)
            stats.files_skipped += 1
            continue

        raw_text              = file_path.read_text(encoding="utf-8")
        frontmatter, body     = parse_frontmatter(raw_text)
        meta                  = infer_metadata(file_path, frontmatter)
        last_updated          = frontmatter_last_updated(frontmatter)
        chunks                = list(chunk_markdown(body, file_path.name, cfg))

        log.info(
            "  Chunking: %s  type=%s service=%s chunks=%d last_updated=%s",
            file_path.name, meta["doc_type"], meta["service"],
            len(chunks), last_updated,
        )

        # Log chunk_type distribution for this file
        type_dist: dict[str, int] = {}
        for c in chunks:
            type_dist[c["chunk_type"]] = type_dist.get(c["chunk_type"], 0) + 1
        log.debug("    chunk_type distribution: %s", type_dist)

        stats.files_indexed += 1
        stats.chunks_total  += len(chunks)

        if dry_run:
            for i, chunk in enumerate(chunks):
                log.info(
                    "    [DRY RUN] chunk %d  type=%-20s  title=%s",
                    i, chunk["chunk_type"], chunk["title"][:70],
                )
            state[sk] = fhash
            continue

        # Embed all chunks for this file concurrently
        try:
            embeddings = embed_batch_concurrent(
                cfg,
                openai_client,
                chunks,
                text_fn=lambda c: f"{c['title']}\n\n{c['content']}",
            )
        except Exception as exc:
            log.error("Embedding failed for %s: %s — skipping file", file_path.name, exc)
            continue

        for i, (chunk, embedding) in enumerate(zip(chunks, embeddings)):
            all_docs.append({
                "id":           f"{doc_id}-{i:04d}",
                "doc_id":       doc_id,
                "chunk_index":  i,
                "title":        chunk["title"],
                "content":      chunk["content"],
                "doc_type":     meta["doc_type"],
                "chunk_type":   chunk["chunk_type"],
                "service":      meta["service"],
                "source_file":  file_path.name,
                "last_updated": last_updated,
                "content_hash": fhash,
                "embedding":    embedding,
            })

        state[sk] = fhash

    if not dry_run and all_docs:
        log.info("Uploading %d chunks to system-knowledge-base...", len(all_docs))
        ok, fail           = upload_all(cfg, search_client, all_docs, "system-knowledge-base")
        stats.docs_uploaded = ok
        stats.docs_failed   = fail
    elif dry_run:
        log.info("[DRY RUN] Would upload %d document chunks", len(all_docs) if all_docs else stats.chunks_total)

    stats.elapsed_secs = time.time() - t0
    log.info("system-knowledge-base: %s", stats)
    return stats


# ══════════════════════════════════════════════════════════════════════════════
# RCA INDEXER
# ══════════════════════════════════════════════════════════════════════════════

_RCA_REQUIRED_FIELDS = frozenset(
    {"rca_id", "service", "error_summary", "root_cause", "resolution", "prevention"}
)

_RCA_AUTO_TAGS: list[tuple[str, list[str]]] = [
    ("blob-storage",     ["blobnotfound", "blob storage", "blobserviceclient", "requestfailedexception"]),
    ("managed-identity", ["managed identity", "id-positionprocessor", "login failed", "external provider"]),
    ("dns",              ["dns", "nxdomain", "no such host", "privatelink", "conditional forwarding"]),
    ("sql-timeout",      ["timeout expired", "bulkcopytimeout", "dtu", "sqlbulkcopy"]),
    ("csv-parsing",      ["formatexception", "decimal.parse", "csv", "corrupted row", "n/a"]),
    ("sql-auth",         ["login failed", "db_datawriter", "db_datareader", "create user"]),
    ("scheduler",        ["scheduler", "timer trigger", "06:00 utc", "cronexpression"]),
    ("durable-functions",["durable", "orchestrat", "activity", "taskfailedexception"]),
]


def derive_rca_tags(rca: dict) -> list[str]:
    """
    Auto-derive technology tags from RCA content for improved recall on
    novel error patterns that semantically resemble a past incident.
    Merges with any explicit tags already in the RCA JSON.
    """
    combined = " ".join([
        rca.get("error_summary", ""),
        rca.get("root_cause", ""),
        rca.get("resolution", ""),
        rca.get("prevention", ""),
    ]).lower()

    derived = [tag for tag, signals in _RCA_AUTO_TAGS if any(s in combined for s in signals)]
    explicit = rca.get("tags", [])
    if isinstance(explicit, str):
        explicit = [explicit]

    # Deduplicate while preserving explicit-first order
    seen: set[str] = set()
    tags: list[str] = []
    for t in (explicit + derived):
        if t not in seen:
            seen.add(t)
            tags.append(t)
    return tags


def validate_rca(rca: dict, source_file: str) -> list[str]:
    """Return list of validation errors; empty list means the RCA is valid."""
    errors: list[str] = []

    missing = _RCA_REQUIRED_FIELDS - set(rca.keys())
    if missing:
        errors.append(f"Missing required fields: {sorted(missing)}")

    for field_name in ("rca_id", "service", "error_summary", "root_cause",
                       "resolution", "prevention"):
        val = rca.get(field_name)
        if val is not None and not str(val).strip():
            errors.append(f"Field '{field_name}' is empty")

    dur = rca.get("duration_minutes")
    if dur is not None and (not isinstance(dur, int) or dur < 0):
        errors.append(f"'duration_minutes' must be a non-negative int, got {dur!r}")

    sev = rca.get("severity")
    if sev is not None and sev not in ("P1", "P2", "P3", "P4"):
        errors.append(f"'severity' must be P1-P4, got {sev!r}")

    return errors


def build_rca_embed_text(rca: dict) -> str:
    """
    Build the embedding string from the fields an engineer would search for.
    Tags are appended to boost recall for pattern-matching on novel errors.
    """
    tags_line = f"Tags: {', '.join(rca.get('_tags', []))}" if rca.get("_tags") else ""
    parts = [
        f"Service: {rca.get('service', '')}",
        f"Incident: {rca.get('error_summary', '')}",
        f"Root cause: {rca.get('root_cause', '')}",
        f"Resolution: {rca.get('resolution', '')}",
        f"Prevention: {rca.get('prevention', '')}",
    ]
    if tags_line:
        parts.append(tags_line)
    return "\n".join(parts)[:6000]


def index_rcas(
    cfg: Config,
    search_client: SearchClient,
    openai_client: AzureOpenAI,
    state: dict[str, str],
    incremental: bool,
    dry_run: bool,
) -> IndexStats:
    stats = IndexStats()
    t0    = time.time()

    json_files = sorted(cfg.rcas_dir.glob("*.json"))
    if not json_files:
        log.warning("No JSON files found in %s", cfg.rcas_dir)
        stats.elapsed_secs = time.time() - t0
        return stats

    all_rcas: list[dict] = []
    validation_errors    = 0

    for jf in json_files:
        fhash = file_sha256(jf)
        sk    = f"rca:{jf.name}"
        stats.files_found += 1

        if incremental and not is_changed(state, sk, fhash):
            log.info("  SKIP (unchanged): %s", jf.name)
            stats.files_skipped += 1
            continue

        try:
            data = json.loads(jf.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            log.error("Invalid JSON in %s: %s — skipping", jf.name, exc)
            continue

        items = data if isinstance(data, list) else [data]

        for rca in items:
            errs = validate_rca(rca, jf.name)
            if errs:
                log.warning(
                    "RCA %s in %s failed validation: %s — skipping",
                    rca.get("rca_id", "?"), jf.name, "; ".join(errs),
                )
                validation_errors += 1
                continue

            rca["_source_file"] = jf.name
            rca["_hash"]        = fhash
            rca["_tags"]        = derive_rca_tags(rca)
            all_rcas.append(rca)

        state[sk] = fhash

    if validation_errors:
        log.warning("Skipped %d RCA(s) due to validation errors", validation_errors)

    stats.files_indexed = len(all_rcas)

    if not all_rcas:
        log.info("No RCAs to index")
        stats.elapsed_secs = time.time() - t0
        return stats

    log.info("Embedding %d RCA document(s)...", len(all_rcas))

    if dry_run:
        for rca in all_rcas:
            log.info(
                "  [DRY RUN] Would embed: %s  tags=%s",
                rca["rca_id"], rca["_tags"],
            )
        stats.elapsed_secs = time.time() - t0
        return stats

    # Embed all RCAs concurrently
    try:
        embeddings = embed_batch_concurrent(
            cfg,
            openai_client,
            all_rcas,
            text_fn=build_rca_embed_text,
        )
    except Exception as exc:
        log.error("RCA embedding failed: %s", exc)
        stats.elapsed_secs = time.time() - t0
        return stats

    documents: list[dict] = []
    for rca, embedding in zip(all_rcas, embeddings):
        documents.append({
            "id":               rca["rca_id"].lower().replace("-", ""),
            "rca_id":           rca["rca_id"],
            "service":          rca["service"],
            "severity":         rca.get("severity", "P2"),
            "error_summary":    rca["error_summary"],
            "root_cause":       rca["root_cause"],
            "resolution":       rca["resolution"],
            "prevention":       rca["prevention"],
            "tags":             rca["_tags"],
            "date_resolved":    rca.get("date_resolved", "2026-01-01T00:00:00Z"),
            "resolved_by":      rca.get("resolved_by", ""),
            "duration_minutes": rca.get("duration_minutes", 0),
            "content_hash":     rca["_hash"],
            "embedding":        embedding,
        })

    log.info("Uploading %d RCA documents to rca-knowledge-base...", len(documents))
    ok, fail           = upload_all(cfg, search_client, documents, "rca-knowledge-base")
    stats.docs_uploaded = ok
    stats.docs_failed   = fail
    stats.elapsed_secs  = time.time() - t0
    log.info("rca-knowledge-base: %s", stats)
    return stats


# ══════════════════════════════════════════════════════════════════════════════
# CONNECTIVITY VALIDATION
# ══════════════════════════════════════════════════════════════════════════════

def validate_connectivity(
    cfg: Config,
    index_client: SearchIndexClient,
    openai_client: AzureOpenAI,
) -> None:
    """Smoke-test both services at startup. Exit cleanly on failure."""
    log.info("Validating connectivity...")

    try:
        list(index_client.list_index_names())
        log.info("  ✓ Azure AI Search reachable (%s)", cfg.search_endpoint)
    except Exception as exc:
        log.error("  ✗ Azure AI Search connectivity failed: %s", exc)
        sys.exit(1)

    try:
        openai_client.embeddings.create(model=cfg.embedding_model, input="connectivity-test")
        log.info("  ✓ OpenAI Embeddings reachable (model=%s)", cfg.embedding_model)
    except Exception as exc:
        log.error("  ✗ OpenAI Embeddings failed: %s", exc)
        sys.exit(1)

    log.info("  ✓ All services healthy")


# ══════════════════════════════════════════════════════════════════════════════
# ARGUMENT PARSING
# ══════════════════════════════════════════════════════════════════════════════

def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="index_all",
        description="Finance AIOps Knowledge Base Indexer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--full",
        action="store_true",
        help="Drop and recreate indexes, then index all files.",
    )
    mode.add_argument(
        "--incremental",
        action="store_true",
        help="Skip files unchanged since last run (uses .index-state.json).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate the full pipeline without calling any Azure or OpenAI APIs.",
    )
    parser.add_argument(
        "--only",
        choices=["docs", "rcas"],
        help="Index only one source type.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable DEBUG-level console logging.",
    )
    return parser


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = build_arg_parser()
    args   = parser.parse_args()

    global log
    log = _configure_logging(args.verbose)

    cfg = Config.from_env()

    # Azure clients
    credential   = AzureKeyCredential(cfg.search_key)
    index_client = SearchIndexClient(endpoint=cfg.search_endpoint, credential=credential)
    sys_client   = SearchClient(
        endpoint=cfg.search_endpoint,
        index_name="system-knowledge-base",
        credential=credential,
    )
    rca_client   = SearchClient(
        endpoint=cfg.search_endpoint,
        index_name="rca-knowledge-base",
        credential=credential,
    )
    openai_client = AzureOpenAI(
        api_key=cfg.openai_key,
        azure_endpoint=cfg.openai_endpoint,
        api_version="2024-02-01",
    )

    # ── Banner ────────────────────────────────────────────────────────────────
    mode_label = (
        "DRY RUN" if args.dry_run
        else "FULL"        if args.full
        else "INCREMENTAL" if args.incremental
        else "DEFAULT"
    )
    print()
    print("=" * 70)
    print("  Finance AIOps Knowledge Base Indexer")
    print(f"  Search   : {cfg.search_endpoint}")
    print(f"  Model    : {cfg.embedding_model}")
    print(f"  Mode     : {mode_label}")
    print(f"  Only     : {args.only or 'all'}")
    print(f"  Docs dir : {cfg.docs_dir}")
    print(f"  RCAs dir : {cfg.rcas_dir}")
    print("=" * 70)
    print()

    # ── Pre-flight ────────────────────────────────────────────────────────────
    if not args.dry_run:
        validate_connectivity(cfg, index_client, openai_client)
        create_or_update_indexes(index_client, cfg, full=args.full)
        if args.full:
            time.sleep(3)  # let Azure propagate index creation

    state = load_state(cfg) if args.incremental else {}
    run_start   = time.time()
    all_results: dict[str, IndexStats] = {}

    # ── System docs ───────────────────────────────────────────────────────────
    if not args.only or args.only == "docs":
        print("-" * 70)
        print("[1/2] System knowledge  →  system-knowledge-base")
        print("-" * 70)
        all_results["docs"] = index_system_docs(
            cfg, sys_client, openai_client,
            state, args.incremental, args.dry_run,
        )

    # ── RCAs ──────────────────────────────────────────────────────────────────
    if not args.only or args.only == "rcas":
        print("-" * 70)
        print("[2/2] RCA documents     →  rca-knowledge-base")
        print("-" * 70)
        all_results["rcas"] = index_rcas(
            cfg, rca_client, openai_client,
            state, args.incremental, args.dry_run,
        )

    # ── Persist state ─────────────────────────────────────────────────────────
    if args.incremental and not args.dry_run:
        save_state(cfg, state)

    # ── Summary ───────────────────────────────────────────────────────────────
    total_secs = round(time.time() - run_start, 1)
    print()
    print("=" * 70)
    print("  COMPLETE")
    print("=" * 70)
    for name, s in all_results.items():
        print(f"  {name:<6}  files={s.files_found}  skipped={s.files_skipped}"
              f"  chunks={s.chunks_total}  uploaded={s.docs_uploaded}"
              f"  failed={s.docs_failed}  time={s.elapsed_secs:.1f}s")
    print(f"  Total elapsed: {total_secs}s")
    if not args.dry_run:
        any_failed = any(s.docs_failed > 0 for s in all_results.values())
        print()
        if any_failed:
            print("  ⚠  Some documents failed to upload — review log for details")
        else:
            print("  Indexes ready:")
            print("    system-knowledge-base  — architecture and infrastructure docs")
            print("    rca-knowledge-base     — past incident RCAs")
    print("=" * 70)
    print()

    # Exit non-zero if any upload failures occurred
    if any(s.docs_failed > 0 for s in all_results.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()
"""
Central configuration for the Fabric Shortcut Proxy POC.

Settings resolve with this precedence (highest wins):
    1. environment variable
    2. external JSON config file (``config.json``, or ``$CONFIG_FILE``)
    3. built-in default

The JSON file is optional; when absent, behavior is exactly env-var + defaults.
See ``config.example.json`` for the full shape.
"""
from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass


# ---------------------------------------------------------------------------
# External JSON config (optional)
# ---------------------------------------------------------------------------

def _load_config_file() -> dict:
    path = os.environ.get("CONFIG_FILE", "config.json")
    try:
        # utf-8-sig tolerates an optional BOM (Windows editors / PowerShell).
        with open(path, "r", encoding="utf-8-sig") as fh:
            data = json.load(fh)
        if not isinstance(data, dict):
            print(f"[config] {path}: top-level JSON must be an object; ignoring.", file=sys.stderr)
            return {}
        return data
    except FileNotFoundError:
        return {}
    except (OSError, json.JSONDecodeError) as exc:  # pragma: no cover - defensive
        print(f"[config] failed to read {path!r}: {exc}", file=sys.stderr)
        return {}


_FILE_CFG: dict = _load_config_file()


def _raw(env: str | None, key: str, default):
    """Return env var (if set), else JSON value (if present), else default."""
    if env and env in os.environ:
        return os.environ[env]
    if key in _FILE_CFG:
        return _FILE_CFG[key]
    return default


# Auto-registry: every setting read through the _get_* helpers records its json
# key, env var, type and DEFAULT here, so the config builder (/_config) can show
# the complete settings catalog with defaults — no hand-maintained list to drift.
_SETTINGS_REGISTRY: dict[str, dict] = {}


def _register(env: str | None, key: str, typ: str, default) -> None:
    if key:
        _SETTINGS_REGISTRY[key] = {"key": key, "env": env, "type": typ, "default": default}


def _get_str(env: str | None, key: str, default: str) -> str:
    _register(env, key, "str", default)
    v = _raw(env, key, default)
    return default if v is None else str(v)


def _get_int(env: str | None, key: str, default: int) -> int:
    _register(env, key, "int", default)
    v = _raw(env, key, default)
    return int(str(v)) if not isinstance(v, bool) else default


def _get_float(env: str | None, key: str, default: float) -> float:
    _register(env, key, "float", default)
    v = _raw(env, key, default)
    return float(str(v)) if not isinstance(v, bool) else default


def _get_bool(env: str | None, key: str, default: bool) -> bool:
    _register(env, key, "bool", default)
    v = _raw(env, key, None)
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() in ("1", "true", "yes", "on")


# ---------------------------------------------------------------------------
# S3 / bucket settings
# ---------------------------------------------------------------------------

BUCKET_NAME: str = _get_str("S3_BUCKET", "bucket", "fabric-iceberg-poc")
WAREHOUSE_PREFIX: str = _get_str(None, "warehouse_prefix", "db")  # path inside the bucket
TABLE_NAME: str = _get_str("TABLE_NAME", "table_name", "sales")

# Phase 1: virtual object path layout.
#   legacy    -> db/<table>
#   canonical -> db/<server>/<database>/<schema>/<object>
# Default is canonical with legacy aliases disabled for immediate cutover.
OBJECT_PATH_LAYOUT: str = _get_str("OBJECT_PATH_LAYOUT", "object_path_layout", "canonical").strip().lower()
ENABLE_LEGACY_PATH_ALIASES: bool = _get_bool(
    "ENABLE_LEGACY_PATH_ALIASES", "enable_legacy_path_aliases", False
)

# S3 credentials (Fabric uses these when creating the shortcut connection).
# For POC: proxy accepts any credentials that match these values.
ACCESS_KEY_ID: str = _get_str("S3_ACCESS_KEY_ID", "access_key_id", "AKIAIOSFODNN7EXAMPLE")
SECRET_ACCESS_KEY: str = _get_str("S3_SECRET_ACCESS_KEY", "secret_access_key", "poc-secret-not-checked")

# ---------------------------------------------------------------------------
# Source database
# ---------------------------------------------------------------------------

# SQLite for quick local demo.  Swap to your real source:
#   PostgreSQL : postgresql+asyncpg://user:pass@host/db
#   SQL Server : mssql+aioodbc://user:pass@host/db?driver=ODBC+Driver+18+for+SQL+Server
#   SQL Server (Windows auth): mssql+aioodbc://@host/db?driver=ODBC+Driver+18+for+SQL+Server&trusted_connection=yes
DB_URL: str = _get_str("DB_URL", "db_url", "sqlite+aiosqlite:///./poc_source.db")
DB_SOURCE_TABLE: str = _get_str("DB_SOURCE_TABLE", "source_table", "sales")

# Split/partition key column (an integer column used to shard the table into
# NUM_SPLITS Parquet files). When set, the table's Iceberg schema is derived
# automatically from source metadata — no manual TABLE_SCHEMA needed.
KEY_COLUMN: str = _get_str("KEY_COLUMN", "key_column", "")

# Query execution limits
QUERY_TIMEOUT_SECONDS: int = _get_int("QUERY_TIMEOUT", "query_timeout", 30)
QUERY_MAX_ROWS: int = _get_int("QUERY_MAX_ROWS", "query_max_rows", 500_000)

# ---------------------------------------------------------------------------
# Output table format
# ---------------------------------------------------------------------------
# "iceberg" (default): serve an Iceberg table; Fabric's S3 shortcut virtualizes
#   it to Delta (an extra conversion layer with 5s-2min latency + caveats).
# "delta": serve a native Delta table (_delta_log/*.json + parquet). Fabric's
#   shortcut reads it directly — no Iceberg->Delta conversion. Same splits,
#   pinning and freshness model; each published snapshot version = one Delta
#   commit (add new files, remove superseded).
TABLE_FORMAT: str = _get_str("TABLE_FORMAT", "table_format", "iceberg").strip().lower()

# ---------------------------------------------------------------------------
# Iceberg / split settings
# ---------------------------------------------------------------------------

# Number of virtual Parquet split files exposed per snapshot.
NUM_SPLITS: int = _get_int("NUM_SPLITS", "num_splits", 8)

# Phase 4 scale engine — split planning strategy:
#   "modulo" (default, known-good): WHERE (pk % num_splits) = split_index. Even
#     distribution, but every split scans the whole table (no index pruning).
#   "range": WHERE pk >= lo AND pk < hi over contiguous key ranges computed from
#     the source MIN/MAX. Each split reads only its slice via the PK index — the
#     path that scales to 10^8 rows. Requires an integer key column; falls back
#     to modulo (with a warning) when bounds can't be determined.
SPLIT_STRATEGY: str = _get_str("SPLIT_STRATEGY", "split_strategy", "modulo").strip().lower()

# Phase 4 scale engine — streaming Parquet materialization. When on, a split is
# read from the source in batches and written incrementally to the Parquet file,
# so peak memory is ~one batch of rows instead of the whole split materialized in
# RAM (the path for 10^8-row tables). Default off keeps the known-good single-shot
# generator (byte-identical). STREAM_BATCH_ROWS is the fetch/row-group batch size.
STREAMING_PARQUET: bool = _get_bool("STREAMING_PARQUET", "streaming_parquet", False)
STREAM_BATCH_ROWS: int = _get_int("STREAM_BATCH_ROWS", "stream_batch_rows", 50_000)

# Phase 4 scale engine — source backpressure. Caps the number of concurrent SQL
# queries this Agent runs against the SOURCE database (startup materialization
# AND on-demand serving-time regeneration), so a fleet doesn't overwhelm the
# source. 0 = unlimited (default, behavior unchanged). Fleet-wide load is roughly
# agents x SOURCE_MAX_CONCURRENCY; a global cross-Agent limiter is future work.
SOURCE_MAX_CONCURRENCY: int = _get_int("SOURCE_MAX_CONCURRENCY", "source_max_concurrency", 0)

# Iceberg snapshot is pinned to a watermark timestamp (epoch ms).
# At startup the proxy sets this to the current time; it remains stable
# until the process restarts (or an explicit refresh API is called).
SNAPSHOT_REFRESH_ON_START: bool = True

# ---------------------------------------------------------------------------
# Iceberg table schema definition
# ---------------------------------------------------------------------------
# Describes the LOGICAL columns exposed to Fabric.
# Must match the source SQL table columns exactly (names + compatible types).
# Iceberg primitive types: boolean, int, long, float, double, decimal(P,S),
#                          date, time, timestamp, timestamptz, string, uuid,
#                          fixed(L), binary.
@dataclass
class ColumnDef:
    field_id: int
    name: str
    iceberg_type: str          # Iceberg type string
    nullable: bool = True


TABLE_SCHEMA: list[ColumnDef] = [
    ColumnDef(field_id=1,  name="id",          iceberg_type="long",          nullable=False),
    ColumnDef(field_id=2,  name="order_date",  iceberg_type="date",          nullable=True),
    ColumnDef(field_id=3,  name="customer_id", iceberg_type="long",          nullable=True),
    ColumnDef(field_id=4,  name="product",     iceberg_type="string",        nullable=True),
    ColumnDef(field_id=5,  name="quantity",    iceberg_type="int",           nullable=True),
    ColumnDef(field_id=6,  name="unit_price",  iceberg_type="double",        nullable=True),
    ColumnDef(field_id=7,  name="total",       iceberg_type="double",        nullable=True),
    ColumnDef(field_id=8,  name="region",      iceberg_type="string",        nullable=True),
]


# ---------------------------------------------------------------------------
# Table registry (F1 — multi-table support)
# ---------------------------------------------------------------------------
# Each virtual Iceberg table maps a source SQL table/view to an Iceberg schema.
#
# The simplest form is just a name + source table + key column — the column
# schema is then reflected from the source database automatically at startup:
#
#     TableDef(name="sales", source_table="dbo.SalesOrders", key_column="OrderId")
#
# Provide an explicit ``schema`` only when you want to override the reflected
# types (e.g. the built-in SQLite demo, whose date column is stored as text).
# By default there is exactly one table; add entries to expose more under
# ``warehouse/db/<name>``.
@dataclass
class TableDef:
    name: str                                 # Iceberg table name (path segment)
    source_table: str                         # source SQL table/view to query
    schema: list[ColumnDef] | None = None     # columns; None => reflect from source
    num_splits: int = NUM_SPLITS              # virtual Parquet files for this table
    key_column: str | None = None             # integer split key; None => auto-detect PK


# When KEY_COLUMN is set, the default table auto-derives its schema from source
# metadata; otherwise it uses the explicit demo TABLE_SCHEMA (known-good path).
def _tabledef_from_json(d: dict) -> "TableDef":
    """Build a TableDef from a JSON ``tables`` entry.

    Only ``source_table`` is truly required; ``name`` defaults to the source
    table's last segment, ``schema`` is reflected from source when omitted, and
    ``key_column`` is auto-detected from the primary key when omitted.
    """
    source_table = str(d.get("source_table", ""))
    name = d.get("name") or (source_table.rsplit(".", 1)[-1] if source_table else "table")
    raw_schema = d.get("schema")
    schema = None
    if raw_schema:
        schema = [
            ColumnDef(
                field_id=int(c["field_id"]),
                name=c["name"],
                iceberg_type=c.get("type") or c.get("iceberg_type"),
                nullable=bool(c.get("nullable", True)),
            )
            for c in raw_schema
        ]
    return TableDef(
        name=str(name),
        source_table=source_table,
        schema=schema,
        num_splits=int(d.get("num_splits", NUM_SPLITS)),
        key_column=d.get("key_column") or None,
    )


if _FILE_CFG.get("tables"):
    # Fully JSON-driven table registry (no config.py editing needed).
    TABLES: list[TableDef] = [_tabledef_from_json(t) for t in _FILE_CFG["tables"]]
else:
    TABLES = [
        TableDef(
            name=TABLE_NAME,
            source_table=DB_SOURCE_TABLE,
            schema=None if KEY_COLUMN else TABLE_SCHEMA,
            num_splits=NUM_SPLITS,
            key_column=KEY_COLUMN or None,
        ),
    ]

# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

METADATA_CACHE_TTL_SECONDS: int = _get_int("METADATA_CACHE_TTL", "metadata_cache_ttl", 60)
PARQUET_CACHE_TTL_SECONDS: int  = _get_int("PARQUET_CACHE_TTL", "parquet_cache_ttl", 300)
PARQUET_CACHE_MAX_BYTES: int    = _get_int("PARQUET_CACHE_MAX_BYTES", "parquet_cache_max_bytes", 256 * 1024 * 1024)  # 256 MB

# F5 — persistent (disk) Parquet cache. When enabled, generated Parquet bytes
# are written to PARQUET_DISK_CACHE_DIR keyed by a hash of the object key, so a
# warm restart can skip regeneration entirely (snapshot ids are deterministic,
# so keys are stable across restarts). Default OFF (pure in-memory cache).
PARQUET_DISK_CACHE: bool = _get_bool("PARQUET_DISK_CACHE", "parquet_disk_cache", False)
PARQUET_DISK_CACHE_DIR: str = _get_str("PARQUET_DISK_CACHE_DIR", "parquet_disk_cache_dir", "./.parquet_cache")

# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------

HOST: str = _get_str("HOST", "host", "0.0.0.0")
PORT: int  = _get_int("PORT", "port", 9000)

# ---------------------------------------------------------------------------
# Robustness (Phase 2)
# ---------------------------------------------------------------------------

# H4 — resource guards: max simultaneous on-demand Parquet generations.
MAX_CONCURRENT_GENERATIONS: int = _get_int("MAX_CONCURRENT_GENERATIONS", "max_concurrent_generations", 4)

# H5 — retry/resilience for transient source-DB errors.
DB_MAX_RETRIES: int = _get_int("DB_MAX_RETRIES", "db_max_retries", 2)
DB_RETRY_BACKOFF_SECONDS: float = _get_float("DB_RETRY_BACKOFF", "db_retry_backoff", 0.5)

# H6 — validate that the source table exposes every declared column at startup.
VALIDATE_SOURCE_SCHEMA: bool = _get_bool("VALIDATE_SOURCE_SCHEMA", "validate_source_schema", True)

# H3 — enforce AWS SigV4 request signatures (default OFF for POC compatibility).
REQUIRE_SIGV4: bool = _get_bool("REQUIRE_SIGV4", "require_sigv4", False)

# Optional config-builder admin SPA at /_config (accepts DB credentials to
# reflect schemas). OFF by default — run it locally, never expose publicly.
ENABLE_CONFIG_BUILDER: bool = _get_bool("ENABLE_CONFIG_BUILDER", "enable_config_builder", False)

# Optional monitoring dashboard SPA at /_monitor — per-table read/query stats,
# metadata/version info, and Fabric->SQL->Parquet query-lag. Read-only; OFF by
# default. Requires REQUEST_TRACE for the request timeline data.
ENABLE_MONITOR: bool = _get_bool("ENABLE_MONITOR", "enable_monitor", False)


# ---------------------------------------------------------------------------
# Advanced Iceberg + performance (Phase 5)
# ---------------------------------------------------------------------------

# F3 — emit per-column statistics (record/null/value counts + lower/upper
# bounds) in the manifest so Iceberg readers can prune splits. Default OFF:
# the stat maps must be encoded exactly right or strict readers (XTable) reject
# the manifest, so this stays flagged until validated against real Fabric.
ICEBERG_MANIFEST_STATS: bool = _get_bool("ICEBERG_MANIFEST_STATS", "iceberg_manifest_stats", False)

# F2 — retain prior snapshots and expose them via snapshot-log / metadata-log so
# Iceberg time-travel works. Default OFF (single-snapshot metadata is the
# known-good Fabric path). Bounds how many historical snapshots are kept.
ICEBERG_SNAPSHOT_HISTORY: bool = _get_bool("ICEBERG_SNAPSHOT_HISTORY", "iceberg_snapshot_history", False)
SNAPSHOT_HISTORY_LIMIT: int = _get_int("SNAPSHOT_HISTORY_LIMIT", "snapshot_history_limit", 10)

# F4 — materialize splits concurrently at startup (bounded by
# MAX_CONCURRENT_GENERATIONS) instead of one at a time.
CONCURRENT_STARTUP_MATERIALIZATION: bool = _get_bool(
    "CONCURRENT_STARTUP_MATERIALIZATION", "concurrent_startup_materialization", True
)


# ---------------------------------------------------------------------------
# Data freshness (content-addressed snapshots + background poller)
# ---------------------------------------------------------------------------
# When enabled, the proxy periodically re-reads each table, content-addresses
# every chunk (path = hash of its rows), and publishes a NEW Iceberg snapshot
# only when content actually changed — so Fabric re-reads updated data. OFF by
# default: the known-good deterministic path (below) is unchanged.
AUTO_REFRESH: bool = _get_bool("AUTO_REFRESH", "auto_refresh", False)

# Poll interval (10 min default — matches Fabric's metadata-sync cadence).
REFRESH_POLL_SECONDS: int = _get_int("REFRESH_POLL_SECONDS", "refresh_poll_seconds", 600)

# Change-detection strategy:
#   auto          -> dialect_probe first; else wait for manual; full pull only if allowed
#   dialect_probe -> catalog probe only (no full pull)
#   content_hash  -> always re-read + hash + dedupe (uniform, most expensive)
#   ttl           -> re-read once per REFRESH_TTL_SECONDS window
#   manual        -> only POST /_admin/refresh advances
REFRESH_STRATEGY: str = _get_str("REFRESH_STRATEGY", "refresh_strategy", "auto")

# In `auto`, allow the last-resort full content read when the probe is unavailable.
REFRESH_ALLOW_FULL_PULL: bool = _get_bool("REFRESH_ALLOW_FULL_PULL", "refresh_allow_full_pull", False)

# Window for the `ttl` strategy.
REFRESH_TTL_SECONDS: int = _get_int("REFRESH_TTL_SECONDS", "refresh_ttl_seconds", 1200)


# ---------------------------------------------------------------------------
# Request tracing (capture the Fabric read/convert timeline)
# ---------------------------------------------------------------------------
# Records every S3 object request (key, kind, table, status, duration, gap since
# the previous request) in a bounded in-memory ring buffer, exposed via
# /_admin/trace, /_admin/timeline and /_admin/objects. Cheap; on by default.
REQUEST_TRACE: bool = _get_bool("REQUEST_TRACE", "request_trace", True)
TRACE_BUFFER_SIZE: int = _get_int("TRACE_BUFFER_SIZE", "trace_buffer_size", 5000)

# Map SQL naive DATETIME/DATETIME2 columns to Iceberg `timestamptz` (UTC) instead
# of `timestamp`. The Fabric SQL analytics endpoint does NOT support TIMESTAMP_NTZ
# (Iceberg `timestamp`), so the default assumes naive timestamps are UTC to keep
# those columns queryable. Set false to preserve exact no-timezone semantics.
TIMESTAMP_ASSUME_UTC: bool = _get_bool("TIMESTAMP_ASSUME_UTC", "timestamp_assume_utc", True)

# Pin startup-materialized Parquet splits in memory so each snapshot data file is
# served BYTE-IDENTICAL for the life of the snapshot (never TTL-expired or LRU-
# evicted). This prevents on-demand regeneration producing a different-sized file
# than the manifest declared, which breaks Fabric's XTable conversion
# (READ_EXCEPTION / 404 BlobNotFound). Disable only if memory-constrained AND
# PARQUET_DISK_CACHE is on (disk gives byte-identical regeneration instead).
PIN_MATERIALIZED_SPLITS: bool = _get_bool("PIN_MATERIALIZED_SPLITS", "pin_materialized_splits", True)


# ---------------------------------------------------------------------------
# Cluster / scale (SCALE_ARCHITECTURE_PLAN.md) — Phase 0 seam
# ---------------------------------------------------------------------------
# Durable artifact store for materialized Parquet splits + table metadata. The
# runtime reads/writes objects through this layer so serving can later be shared
# across a fleet of stateless Agents. Backends: "local" (filesystem dir; also
# NFS/SMB for multi-node) or "memory" (ephemeral, tests). Not yet on the hot
# serving path in Phase 0 — introduced as the interface + default only.
ARTIFACT_STORE_BACKEND: str = _get_str("ARTIFACT_STORE_BACKEND", "artifact_store_backend", "local").strip().lower()
ARTIFACT_STORE_DIR: str = _get_str("ARTIFACT_STORE_DIR", "artifact_store_dir", "./.artifacts")

# Phase 2: use the artifact store as the DURABLE serving tier for materialized
# Parquet splits. When on, splits are written to the store on materialize and
# read back on a cold miss (before any SQL regeneration), so a restarted or
# stateless Agent serves byte-identically from the store with ZERO regeneration.
# Default off (single-process known-good path unchanged); the Manager turns it on
# for supervised Agents. Correctness note: within a published epoch the store is
# authoritative for a key; changed data must yield a new key (content-addressed
# freshness) or a full rebuild that overwrites the store.
ARTIFACT_STORE_SERVING: bool = _get_bool("ARTIFACT_STORE_SERVING", "artifact_store_serving", False)

# Phase 6: publish a COMPLETE serving image (data splits AND metadata/manifests/
# _delta_log bytes) to the artifact store at startup, so the store dir is a fully
# self-contained, servable table image. A stateless Agent (e.g. the C++ Agent)
# can then serve every S3 object as opaque bytes straight from the store — no SQL,
# Parquet or Iceberg/Delta logic needed. Default off; requires ARTIFACT_STORE_SERVING.
PUBLISH_SERVING_IMAGE: bool = _get_bool("PUBLISH_SERVING_IMAGE", "publish_serving_image", False)

# Phase 3: fleet + gateway. The Manager can supervise N Agents (each on PORT+i)
# behind a built-in round-robin S3 gateway, and shard cold materialization so
# each Agent generates only 1/N of the splits (the rest are read from the shared
# artifact store). Defaults keep single-Agent behavior (the known-good path).
AGENT_COUNT: int = _get_int("AGENT_COUNT", "agent_count", 1)                 # Manager: how many Agents to supervise
AGENT_SHARD_INDEX: int = _get_int("AGENT_SHARD_INDEX", "agent_shard_index", 0)   # this Agent's shard (set by Manager)
AGENT_SHARD_COUNT: int = _get_int("AGENT_SHARD_COUNT", "agent_shard_count", 1)   # total shards (= AGENT_COUNT)
ENABLE_GATEWAY: bool = _get_bool("ENABLE_GATEWAY", "enable_gateway", False)  # Manager: front the fleet with an S3 LB
MATERIALIZE_WAIT_SECONDS: float = _get_float("MATERIALIZE_WAIT_SECONDS", "materialize_wait_seconds", 30.0)

# Phase 4: /_manager operator console. When on, the Manager serves a self-contained
# admin page at /_manager (fleet monitor) plus a small JSON admin API to start /
# stop / restart / drain individual Agents. Default off (the control-plane data
# path is unchanged). ADMIN_TOKEN, when set, is required (X-Admin-Token header or
# ?token=) for every mutating action; reads stay open for convenience.
ENABLE_ADMIN_UI: bool = _get_bool("ENABLE_ADMIN_UI", "enable_admin_ui", False)  # Manager: serve /_manager console
ADMIN_TOKEN: str = _get_str("ADMIN_TOKEN", "admin_token", "").strip()          # guard for mutating /_manager actions

# Manager/Agent control plane (Phase 1). The DATA plane is unchanged; these only
# govern the internal control link. An Agent with an empty MANAGER_URL runs
# **standalone** — byte-identical to the pre-cluster single-process server.
#   Agent side:   MANAGER_URL (where to register/heartbeat), AGENT_ID (auto if blank).
#   Manager side: CONTROL_HOST/CONTROL_PORT (control REST bind), HEARTBEAT_MS,
#                 HEARTBEAT_MISS_LIMIT (dead after N misses), restart backoff/guard.
MANAGER_URL: str = _get_str("MANAGER_URL", "manager_url", "").strip()
AGENT_ID: str = _get_str("AGENT_ID", "agent_id", "").strip()
CONTROL_HOST: str = _get_str("CONTROL_HOST", "control_host", "127.0.0.1")
CONTROL_PORT: int = _get_int("CONTROL_PORT", "control_port", 9200)
HEARTBEAT_MS: int = _get_int("HEARTBEAT_MS", "heartbeat_ms", 2000)
HEARTBEAT_MISS_LIMIT: int = _get_int("HEARTBEAT_MISS_LIMIT", "heartbeat_miss_limit", 3)
AGENT_RESTART_BACKOFF_SECONDS: float = _get_float("AGENT_RESTART_BACKOFF", "agent_restart_backoff", 1.0)
AGENT_MAX_RAPID_RESTARTS: int = _get_int("AGENT_MAX_RAPID_RESTARTS", "agent_max_rapid_restarts", 5)

# Phase 5: robustness & Manager HA. All default off (single-Manager, unchanged).
#   MANAGER_HA          — Manager runs a leader lease over the shared artifact store;
#                         only the primary supervises Agents + serves the gateway,
#                         standbys wait and take over when the primary's lease lapses.
#   RETENTION_GC        — an Agent (shard 0) periodically prunes orphaned Parquet
#                         splits (from snapshot versions aged out of history) from
#                         the shared store, bounding storage.
#   ROLLING_RESTART_*   — the Manager can restart Agents one at a time (health-gated)
#                         so >= N-1 keep serving during an upgrade (no read gap).
MANAGER_HA: bool = _get_bool("MANAGER_HA", "manager_ha", False)
LEADER_LEASE_TTL_MS: int = _get_int("LEADER_LEASE_TTL_MS", "leader_lease_ttl_ms", 10_000)
LEADER_LEASE_RENEW_MS: int = _get_int("LEADER_LEASE_RENEW_MS", "leader_lease_renew_ms", 3_000)
RETENTION_GC: bool = _get_bool("RETENTION_GC", "retention_gc", False)
RETENTION_GC_INTERVAL_SECONDS: float = _get_float("RETENTION_GC_INTERVAL_SECONDS", "retention_gc_interval_seconds", 300.0)
ROLLING_RESTART_HEALTH_TIMEOUT: float = _get_float("ROLLING_RESTART_HEALTH_TIMEOUT", "rolling_restart_health_timeout", 30.0)


# ---------------------------------------------------------------------------
# Config validation & secrets hygiene (Plan item H7)
# ---------------------------------------------------------------------------

import re as _re


def redact_db_url(url: str) -> str:
    """Mask any password embedded in a DB/SQLAlchemy URL for safe logging.

    ``scheme://user:password@host/db`` -> ``scheme://user:***@host/db``.
    URLs without embedded credentials (e.g. SQLite paths) are returned as-is.
    """
    return _re.sub(r"(://[^:/@]+:)[^@/]*(@)", r"\1***\2", url)


def validate_config() -> None:
    """Validate required configuration at startup; raise ``ValueError`` on error.

    Fails fast with a single aggregated, actionable message instead of letting
    misconfiguration surface later as obscure request-time errors.
    """
    problems: list[str] = []

    if not DB_URL:
        problems.append("DB_URL must be set (a SQLAlchemy async URL).")
    if not BUCKET_NAME:
        problems.append("S3_BUCKET must be a non-empty bucket name.")
    if not TABLE_NAME:
        problems.append("TABLE_NAME must be non-empty.")
    if OBJECT_PATH_LAYOUT not in ("legacy", "canonical"):
        problems.append(
            f"OBJECT_PATH_LAYOUT must be 'legacy' or 'canonical' (got {OBJECT_PATH_LAYOUT!r})."
        )
    if NUM_SPLITS < 1:
        problems.append(f"NUM_SPLITS must be >= 1 (got {NUM_SPLITS}).")
    if TABLE_FORMAT not in ("iceberg", "delta"):
        problems.append(f"TABLE_FORMAT must be 'iceberg' or 'delta' (got {TABLE_FORMAT!r}).")
    if SPLIT_STRATEGY not in ("modulo", "range"):
        problems.append(f"SPLIT_STRATEGY must be 'modulo' or 'range' (got {SPLIT_STRATEGY!r}).")
    if STREAM_BATCH_ROWS < 1:
        problems.append(f"STREAM_BATCH_ROWS must be >= 1 (got {STREAM_BATCH_ROWS}).")
    if SOURCE_MAX_CONCURRENCY < 0:
        problems.append(f"SOURCE_MAX_CONCURRENCY must be >= 0 (got {SOURCE_MAX_CONCURRENCY}).")
    if ARTIFACT_STORE_BACKEND not in ("local", "memory"):
        problems.append(f"ARTIFACT_STORE_BACKEND must be 'local' or 'memory' (got {ARTIFACT_STORE_BACKEND!r}).")
    if not (1 <= CONTROL_PORT <= 65535):
        problems.append(f"CONTROL_PORT must be in 1..65535 (got {CONTROL_PORT}).")
    if HEARTBEAT_MS <= 0:
        problems.append(f"HEARTBEAT_MS must be > 0 (got {HEARTBEAT_MS}).")
    if HEARTBEAT_MISS_LIMIT < 1:
        problems.append(f"HEARTBEAT_MISS_LIMIT must be >= 1 (got {HEARTBEAT_MISS_LIMIT}).")
    if AGENT_RESTART_BACKOFF_SECONDS < 0:
        problems.append(f"AGENT_RESTART_BACKOFF must be >= 0 (got {AGENT_RESTART_BACKOFF_SECONDS}).")
    if AGENT_MAX_RAPID_RESTARTS < 1:
        problems.append(f"AGENT_MAX_RAPID_RESTARTS must be >= 1 (got {AGENT_MAX_RAPID_RESTARTS}).")
    if LEADER_LEASE_TTL_MS <= 0:
        problems.append(f"LEADER_LEASE_TTL_MS must be > 0 (got {LEADER_LEASE_TTL_MS}).")
    if not (0 < LEADER_LEASE_RENEW_MS < LEADER_LEASE_TTL_MS):
        problems.append(f"LEADER_LEASE_RENEW_MS must be in 1..TTL-1 (got {LEADER_LEASE_RENEW_MS}/{LEADER_LEASE_TTL_MS}).")
    if RETENTION_GC_INTERVAL_SECONDS <= 0:
        problems.append(f"RETENTION_GC_INTERVAL_SECONDS must be > 0 (got {RETENTION_GC_INTERVAL_SECONDS}).")
    if ROLLING_RESTART_HEALTH_TIMEOUT <= 0:
        problems.append(f"ROLLING_RESTART_HEALTH_TIMEOUT must be > 0 (got {ROLLING_RESTART_HEALTH_TIMEOUT}).")
    if AGENT_COUNT < 1:
        problems.append(f"AGENT_COUNT must be >= 1 (got {AGENT_COUNT}).")
    if AGENT_SHARD_COUNT < 1:
        problems.append(f"AGENT_SHARD_COUNT must be >= 1 (got {AGENT_SHARD_COUNT}).")
    if not (0 <= AGENT_SHARD_INDEX < AGENT_SHARD_COUNT):
        problems.append(f"AGENT_SHARD_INDEX must be in 0..AGENT_SHARD_COUNT-1 (got {AGENT_SHARD_INDEX}/{AGENT_SHARD_COUNT}).")
    if QUERY_TIMEOUT_SECONDS <= 0:
        problems.append(f"QUERY_TIMEOUT must be > 0 (got {QUERY_TIMEOUT_SECONDS}).")
    if QUERY_MAX_ROWS <= 0:
        problems.append(f"QUERY_MAX_ROWS must be > 0 (got {QUERY_MAX_ROWS}).")
    if not (1 <= PORT <= 65535):
        problems.append(f"PORT must be between 1 and 65535 (got {PORT}).")

    if not TABLE_SCHEMA:
        problems.append("TABLE_SCHEMA must define at least one column.")
    else:
        field_ids = [c.field_id for c in TABLE_SCHEMA]
        if any(i <= 0 for i in field_ids):
            problems.append("TABLE_SCHEMA field_ids must be positive integers.")
        if len(set(field_ids)) != len(field_ids):
            problems.append("TABLE_SCHEMA field_ids must be unique.")
        names = [c.name for c in TABLE_SCHEMA]
        if len(set(names)) != len(names):
            problems.append("TABLE_SCHEMA column names must be unique.")

    for name, value in (
        ("METADATA_CACHE_TTL", METADATA_CACHE_TTL_SECONDS),
        ("PARQUET_CACHE_TTL", PARQUET_CACHE_TTL_SECONDS),
        ("PARQUET_CACHE_MAX_BYTES", PARQUET_CACHE_MAX_BYTES),
    ):
        if value <= 0:
            problems.append(f"{name} must be > 0 (got {value}).")

    if MAX_CONCURRENT_GENERATIONS < 1:
        problems.append(
            f"MAX_CONCURRENT_GENERATIONS must be >= 1 (got {MAX_CONCURRENT_GENERATIONS})."
        )
    if DB_MAX_RETRIES < 0:
        problems.append(f"DB_MAX_RETRIES must be >= 0 (got {DB_MAX_RETRIES}).")
    if DB_RETRY_BACKOFF_SECONDS < 0:
        problems.append(
            f"DB_RETRY_BACKOFF must be >= 0 (got {DB_RETRY_BACKOFF_SECONDS})."
        )

    if not TABLES:
        problems.append("TABLES must define at least one table.")
    else:
        table_names = [t.name for t in TABLES]
        if len(set(table_names)) != len(table_names):
            problems.append("TABLES entries must have unique names.")
        for t in TABLES:
            if not t.source_table:
                problems.append(f"Table {t.name!r}: source_table must be non-empty.")
            if t.num_splits < 1:
                problems.append(f"Table {t.name!r}: num_splits must be >= 1.")
            if t.schema is None:
                # Schema is reflected from source metadata at startup.
                continue
            if not t.schema:
                problems.append(f"Table {t.name!r}: schema must have at least one column.")
            else:
                fids = [c.field_id for c in t.schema]
                if any(i <= 0 for i in fids):
                    problems.append(f"Table {t.name!r}: field_ids must be positive.")
                if len(set(fids)) != len(fids):
                    problems.append(f"Table {t.name!r}: field_ids must be unique.")

    if problems:
        raise ValueError("Invalid configuration:\n  - " + "\n  - ".join(problems))


# ---------------------------------------------------------------------------
# Settings catalog (drives the /_config "All settings" view)
# ---------------------------------------------------------------------------
# Category + one-line help per json key. Any key missing here falls back to the
# "Other" category. `secret` keys are shown but never pre-filled by the builder.
SETTINGS_META: dict[str, dict] = {
    # Source & connection
    "db_url":        {"cat": "Source", "help": "SQLAlchemy async connection string.", "secret": True},
    "source_table":  {"cat": "Source", "help": "Source SQL table/view to expose."},
    "key_column":    {"cat": "Source", "help": "Integer split/partition key column (blank = auto-detect PK)."},
    "table_name":    {"cat": "Source", "help": "Virtual Iceberg table name (single-table mode)."},
    # S3 / bucket
    "bucket":            {"cat": "S3 endpoint", "help": "Virtual S3 bucket name Fabric connects to."},
    "warehouse_prefix":  {"cat": "S3 endpoint", "help": "Path prefix inside the bucket (warehouse root)."},
    "object_path_layout": {"cat": "S3 endpoint", "help": "Virtual object layout: legacy (db/<table>) or canonical (db/<server>/<database>/<schema>/<object>)."},
    "enable_legacy_path_aliases": {"cat": "S3 endpoint", "help": "Serve legacy path aliases while canonical layout is enabled (migration safety)."},
    "access_key_id":     {"cat": "S3 endpoint", "help": "S3 access key id Fabric presents."},
    "secret_access_key": {"cat": "S3 endpoint", "help": "S3 secret key (only checked when require_sigv4).", "secret": True},
    "require_sigv4":     {"cat": "S3 endpoint", "help": "Enforce AWS SigV4 request signatures."},
    # Server
    "host": {"cat": "Server", "help": "Bind address."},
    "port": {"cat": "Server", "help": "Listen port."},
    # Splits & query
    "num_splits":     {"cat": "Splits & query", "help": "Virtual Parquet files per table."},
    "split_strategy": {"cat": "Splits & query", "help": "'modulo' (even, full-scan per split) or 'range' (contiguous key ranges via the PK index — scales to 10^8 rows)."},
    "streaming_parquet": {"cat": "Splits & query", "help": "Materialize each split in row batches (bounded memory) instead of loading the whole split into RAM."},
    "stream_batch_rows": {"cat": "Splits & query", "help": "Batch/row-group size for streaming Parquet materialization."},
    "source_max_concurrency": {"cat": "Splits & query", "help": "Cap concurrent SQL queries against the source DB (backpressure). 0 = unlimited."},
    "query_timeout":  {"cat": "Splits & query", "help": "Per-query timeout (seconds)."},
    "query_max_rows": {"cat": "Splits & query", "help": "Max rows per split query."},
    "table_format":   {"cat": "Splits & query", "help": "Output format: 'iceberg' (Fabric virtualizes to Delta) or 'delta' (native, no conversion — lower lag)."},
    # Caching / correctness
    "pin_materialized_splits": {"cat": "Caching", "help": "Serve snapshot data files byte-identical (prevents size drift). Keep on."},
    "parquet_disk_cache":      {"cat": "Caching", "help": "Persist generated Parquet to disk for warm restarts."},
    "parquet_disk_cache_dir":  {"cat": "Caching", "help": "Directory for the disk Parquet cache."},
    "parquet_cache_max_bytes": {"cat": "Caching", "help": "In-memory Parquet LRU cap (bytes)."},
    "parquet_cache_ttl":       {"cat": "Caching", "help": "In-memory Parquet entry TTL (seconds)."},
    "metadata_cache_ttl":      {"cat": "Caching", "help": "Metadata cache TTL (seconds)."},
    # Robustness
    "max_concurrent_generations": {"cat": "Robustness", "help": "Max simultaneous on-demand Parquet builds."},
    "db_max_retries":             {"cat": "Robustness", "help": "Retries on transient source-DB errors."},
    "db_retry_backoff":           {"cat": "Robustness", "help": "Linear backoff between retries (seconds)."},
    "validate_source_schema":     {"cat": "Robustness", "help": "Fail fast if a declared column is missing at startup."},
    "timestamp_assume_utc":       {"cat": "Robustness", "help": "Map naive datetimes to timestamptz (Fabric SQL endpoint rejects TIMESTAMP_NTZ)."},
    # Admin UIs / observability
    "enable_config_builder": {"cat": "Admin & observability", "help": "Serve this config builder at /_config."},
    "enable_monitor":        {"cat": "Admin & observability", "help": "Serve the monitoring dashboard at /_monitor."},
    "request_trace":         {"cat": "Admin & observability", "help": "Record the Fabric request timeline."},
    "trace_buffer_size":     {"cat": "Admin & observability", "help": "Max request-trace records kept in memory."},
    # Iceberg advanced
    "iceberg_manifest_stats":            {"cat": "Iceberg (advanced)", "help": "Emit column min/max stats in manifests (validate with Fabric first)."},
    "iceberg_snapshot_history":          {"cat": "Iceberg (advanced)", "help": "Retain snapshot versions for time-travel."},
    "snapshot_history_limit":            {"cat": "Iceberg (advanced)", "help": "Max retained snapshot versions."},
    "concurrent_startup_materialization":{"cat": "Iceberg (advanced)", "help": "Materialize splits concurrently at startup."},
    # Data freshness
    "auto_refresh":            {"cat": "Data freshness", "help": "Re-read source & publish new snapshots. Not recommended for the Fabric SQL endpoint (stale-Delta 404s)."},
    "refresh_poll_seconds":    {"cat": "Data freshness", "help": "Poll interval (seconds)."},
    "refresh_strategy":        {"cat": "Data freshness", "help": "auto | dialect_probe | content_hash | ttl | manual."},
    "refresh_allow_full_pull": {"cat": "Data freshness", "help": "In auto, allow full read when the probe is unavailable."},
    "refresh_ttl_seconds":     {"cat": "Data freshness", "help": "Window for the ttl strategy (seconds)."},
    # Cluster / scale (Phase 0 seam)
    "artifact_store_backend": {"cat": "Cluster (scale)", "help": "Durable artifact store backend: 'local' (filesystem/NFS/SMB) or 'memory' (ephemeral)."},
    "artifact_store_dir":     {"cat": "Cluster (scale)", "help": "Root directory for the local artifact store."},
    "artifact_store_serving": {"cat": "Cluster (scale)", "help": "Serve materialized Parquet from the artifact store (durable, shareable; zero regeneration on restart). Manager enables it for Agents."},
    "publish_serving_image": {"cat": "Cluster (scale)", "help": "Publish a complete servable image (data + metadata) to the store at startup, so a stateless/C++ Agent can serve every object as opaque bytes."},
    # Cluster / control plane (Phase 1)
    "manager_url":            {"cat": "Cluster (scale)", "help": "Agent: Manager control URL to register/heartbeat (blank = standalone, no cluster)."},
    "agent_id":               {"cat": "Cluster (scale)", "help": "Agent: stable id (blank = auto from host:port)."},
    "control_host":           {"cat": "Cluster (scale)", "help": "Manager: control-plane REST bind address."},
    "control_port":           {"cat": "Cluster (scale)", "help": "Manager: control-plane REST port."},
    "heartbeat_ms":           {"cat": "Cluster (scale)", "help": "Agent heartbeat interval (ms)."},
    "heartbeat_miss_limit":   {"cat": "Cluster (scale)", "help": "Manager marks an Agent dead after this many missed heartbeats."},
    "agent_restart_backoff":  {"cat": "Cluster (scale)", "help": "Manager: delay before respawning a crashed Agent (seconds)."},
    "agent_max_rapid_restarts": {"cat": "Cluster (scale)", "help": "Manager: crash-loop guard — stop respawning after this many restarts in the window."},
    "agent_count":            {"cat": "Cluster (scale)", "help": "Manager: number of Agents to supervise (each on PORT+i)."},
    "agent_shard_index":      {"cat": "Cluster (scale)", "help": "This Agent's materialization shard (set by the Manager)."},
    "agent_shard_count":      {"cat": "Cluster (scale)", "help": "Total materialization shards (= agent_count)."},
    "enable_gateway":         {"cat": "Cluster (scale)", "help": "Manager: front the Agent fleet with a built-in round-robin S3 gateway."},
    "materialize_wait_seconds": {"cat": "Cluster (scale)", "help": "Non-owner Agent: max wait for a sharded split to appear in the store before generating it locally."},
    "enable_admin_ui":        {"cat": "Cluster (scale)", "help": "Manager: serve the /_manager operator console (fleet monitor + start/stop/restart/drain)."},
    "admin_token":            {"cat": "Cluster (scale)", "help": "Manager: token required for mutating /_manager actions (X-Admin-Token header or ?token=). Blank = no auth.", "secret": True},
    "manager_ha":             {"cat": "Cluster (scale)", "help": "Manager HA: run a leader lease over the shared store; only the primary supervises Agents + serves the gateway."},
    "leader_lease_ttl_ms":    {"cat": "Cluster (scale)", "help": "Leader lease TTL (ms): a standby takes over if the primary doesn't renew within this window."},
    "leader_lease_renew_ms":  {"cat": "Cluster (scale)", "help": "Leader lease renew interval (ms); must be < TTL."},
    "retention_gc":           {"cat": "Cluster (scale)", "help": "Agent (shard 0): periodically prune orphaned Parquet splits from the shared store."},
    "retention_gc_interval_seconds": {"cat": "Cluster (scale)", "help": "Retention GC sweep interval (seconds)."},
    "rolling_restart_health_timeout": {"cat": "Cluster (scale)", "help": "Rolling restart: max seconds to wait for each Agent to become healthy before the next."},
}

# Order categories appear in the UI.
_SETTINGS_CAT_ORDER = [
    "Source", "S3 endpoint", "Server", "Splits & query", "Caching",
    "Robustness", "Admin & observability", "Iceberg (advanced)", "Data freshness",
    "Cluster (scale)", "Other",
]


def settings_catalog() -> list[dict]:
    """Return every registered setting with its json key, env var, type, default,
    category and help — grouped/ordered for the /_config settings view.

    Secret settings keep their (non-sensitive) built-in default but are flagged so
    the builder never treats them as prefillable values.
    """
    items = []
    for reg in _SETTINGS_REGISTRY.values():
        meta = SETTINGS_META.get(reg["key"], {})
        items.append({
            "key": reg["key"],
            "env": reg["env"],
            "type": reg["type"],
            "default": reg["default"],
            "category": meta.get("cat", "Other"),
            "help": meta.get("help", ""),
            "secret": bool(meta.get("secret", False)),
        })
    items.sort(key=lambda s: (
        _SETTINGS_CAT_ORDER.index(s["category"]) if s["category"] in _SETTINGS_CAT_ORDER else 999,
        s["key"],
    ))
    return items


# ---------------------------------------------------------------------------
# Phase 5.1 — read effective config + persist changes (config builder live mode)
# ---------------------------------------------------------------------------

def config_file_path() -> str:
    """The JSON config path this process reads (``$CONFIG_FILE`` or config.json)."""
    return os.environ.get("CONFIG_FILE", "config.json")


def _coerce_setting_value(typ: str, value):
    """Coerce a JSON/string ``value`` to a setting's declared type.

    Raises ``ValueError`` on bad input so the save endpoint can reject it.
    """
    if typ == "bool":
        if isinstance(value, bool):
            return value
        s = str(value).strip().lower()
        if s in ("1", "true", "yes", "on"):
            return True
        if s in ("0", "false", "no", "off", ""):
            return False
        raise ValueError(f"expected a boolean (got {value!r})")
    if typ == "int":
        if isinstance(value, bool):
            raise ValueError("expected an integer, got a boolean")
        return int(str(value).strip())
    if typ == "float":
        if isinstance(value, bool):
            raise ValueError("expected a number, got a boolean")
        return float(str(value).strip())
    return str(value)


def effective_settings(*, redact_secrets: bool = True) -> list[dict]:
    """Every registered setting with its CURRENT effective value + source.

    ``source`` is ``env`` | ``file`` | ``default`` following the resolution
    precedence. Secret values are never echoed back — only a ``set``/blank marker.
    """
    out: list[dict] = []
    for reg in _SETTINGS_REGISTRY.values():
        key, env, typ, default = reg["key"], reg["env"], reg["type"], reg["default"]
        meta = SETTINGS_META.get(key, {})
        secret = bool(meta.get("secret", False))

        if env and env in os.environ:
            source, raw = "env", os.environ[env]
        elif key in _FILE_CFG:
            source, raw = "file", _FILE_CFG[key]
        else:
            source, raw = "default", default

        try:
            value = _coerce_setting_value(typ, raw)
        except (ValueError, TypeError):
            value = raw

        if secret and redact_secrets:
            value = "***set***" if (source != "default" and str(raw)) else ""

        out.append({
            "key": key, "env": env, "type": typ, "default": default,
            "value": value, "source": source,
            "category": meta.get("cat", "Other"),
            "help": meta.get("help", ""), "secret": secret,
        })
    out.sort(key=lambda s: (
        _SETTINGS_CAT_ORDER.index(s["category"]) if s["category"] in _SETTINGS_CAT_ORDER else 999,
        s["key"],
    ))
    return out


def validate_setting_updates(updates: dict) -> tuple[dict, list[str]]:
    """Validate a partial settings map. Returns ``(clean, errors)``.

    Rejects unknown keys and values that don't coerce to the setting's type.
    """
    clean: dict = {}
    errors: list[str] = []
    for k, v in (updates or {}).items():
        reg = _SETTINGS_REGISTRY.get(k)
        if reg is None:
            errors.append(f"unknown setting {k!r}")
            continue
        try:
            clean[k] = _coerce_setting_value(reg["type"], v)
        except (ValueError, TypeError) as exc:
            errors.append(f"{k}: {exc}")
    return clean, errors


def write_config_updates(updates: dict) -> dict:
    """Validate + merge ``updates`` into the on-disk config file (atomic write).

    Re-reads the current file (so concurrent ``tables``/other keys are preserved),
    applies the validated changes, and writes ``config.json`` atomically. Raises
    ``ValueError`` (aggregated) if any update is invalid.
    """
    clean, errors = validate_setting_updates(updates)
    if errors:
        raise ValueError("; ".join(errors))

    path = config_file_path()
    try:
        with open(path, "r", encoding="utf-8-sig") as fh:
            existing = json.load(fh)
        if not isinstance(existing, dict):
            existing = {}
    except FileNotFoundError:
        existing = {}

    existing.update(clean)

    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(existing, fh, indent=2, sort_keys=True)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)

    return {"path": os.path.abspath(path), "changed": sorted(clean.keys()), "config": existing}

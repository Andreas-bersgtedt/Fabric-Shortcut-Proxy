"""
Central configuration for the Fabric Shortcut Proxy — organized by concern.

Splits configuration across three modules:
  - system_config.py:     S3, server, artifact store, fleet, control plane, HA
  - connection_config.py: Database URL, credentials, query limits
  - config.py (this):     Settings registry, performance, freshness, caching, tables

Settings resolve with this precedence (highest wins):
    1. environment variable
    2. external JSON config files (config.system.json, config.connection.json, 
       config.performance.json, config.freshness.json, config.tables.json)
    3. built-in default

See config.*.example.json for templates. Monolithic config.json is no longer supported.
"""
from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass

# Re-export system and connection configuration for backward compatibility
from system_config import (
    # S3
    BUCKET_NAME, WAREHOUSE_PREFIX, OBJECT_PATH_LAYOUT, ENABLE_LEGACY_PATH_ALIASES,
    ACCESS_KEY_ID, SECRET_ACCESS_KEY, REQUIRE_SIGV4,
    # Server
    HOST, PORT, TLS_CERT_FILE, TLS_KEY_FILE,
    # Admin UIs
    ENABLE_CONFIG_BUILDER, ENABLE_MONITOR,
    # Storage proxy
    ENABLE_STORAGE_PROXY, ENFORCE_MOUNT_AUTH, ENABLE_AUDIT_LOG, AUDIT_LOG_FILE,
    # Credential store
    ENABLE_CREDENTIAL_STORE, CREDENTIAL_STORE_PATH,
    # Artifact Store
    ARTIFACT_STORE_BACKEND, ARTIFACT_STORE_DIR, ARTIFACT_STORE_SERVING, PUBLISH_SERVING_IMAGE,
    # Fleet
    AGENT_COUNT, AGENT_SHARD_INDEX, AGENT_SHARD_COUNT, SHARD_STRATEGY, ENABLE_GATEWAY, MATERIALIZE_WAIT_SECONDS,
    # Control Plane
    MANAGER_URL, AGENT_ID, CONTROL_HOST, CONTROL_PORT,
    AGENT_ADVERTISE_HOST,
    HEARTBEAT_MS, HEARTBEAT_MISS_LIMIT, AGENT_RESTART_BACKOFF_SECONDS, AGENT_MAX_RAPID_RESTARTS,
    AGENT_DRAIN_GRACE_SECONDS,
    # HA
    MANAGER_HA, LEADER_LEASE_TTL_MS, LEADER_LEASE_RENEW_MS,
    RETENTION_GC, RETENTION_GC_INTERVAL_SECONDS, ROLLING_RESTART_HEALTH_TIMEOUT,
    # Admin
    ENABLE_ADMIN_UI, ADMIN_TOKEN,
)

from connection_config import (
    DB_URL, DB_SOURCE_TABLE, KEY_COLUMN, TABLE_NAME,
    QUERY_TIMEOUT_SECONDS, QUERY_MAX_ROWS,
    DB_MAX_RETRIES, DB_RETRY_BACKOFF_SECONDS, VALIDATE_SOURCE_SCHEMA,
    redact_db_url,
    Connection, CONNECTIONS, get_connection,
)


def effective_db_url(connection_id: str | None = "default") -> str:
    """Return the SQLAlchemy URL for a connection id.

    The ``"default"`` connection resolves to the live module-level ``DB_URL``
    (so tests/monkeypatching of ``config.DB_URL`` keep working); named
    connections resolve from the registry.
    """
    if not connection_id or connection_id == "default":
        return DB_URL
    conn = CONNECTIONS.get(connection_id)
    return conn.db_url if conn else DB_URL


def effective_query_max_rows(connection_id: str | None = "default") -> int:
    """Max rows per split query for a connection id (default tracks live config)."""
    if not connection_id or connection_id == "default":
        return QUERY_MAX_ROWS
    conn = CONNECTIONS.get(connection_id)
    return conn.query_max_rows if conn else QUERY_MAX_ROWS


# ---------------------------------------------------------------------------
# JSON Config loading (for performance/freshness/cache/tables)
# ---------------------------------------------------------------------------

def _load_config_file() -> dict:
    """Load config from separate section-specific JSON files only.
    
    Loads: config.performance.json, config.freshness.json, config.tables.json
    Precedence (per section):
      1. config.{section}.json
      2. empty dict (no fallback to monolithic config.json)
    """
    data = {}
    for section in ("performance", "freshness", "tables"):
        section_path = f"config.{section}.json"
        try:
            with open(section_path, "r", encoding="utf-8-sig") as fh:
                section_data = json.load(fh)
            if isinstance(section_data, dict):
                # Extract section if wrapped, else use entire file as section
                data[section] = section_data.get(section, section_data) if section in section_data else section_data
        except FileNotFoundError:
            pass  # Section is optional; use defaults
        except (OSError, json.JSONDecodeError) as exc:
            print(f"[config] failed to read {section_path!r}: {exc}", file=sys.stderr)
    
    return data


_FILE_CFG: dict = _load_config_file()
_PERF_CFG: dict = _FILE_CFG.get("performance", {})
_FRESH_CFG: dict = _FILE_CFG.get("freshness", {})


def _raw(env: str | None, key: str, default, cfg_dict: dict | None = None):
    """Return env var (if set), else JSON value (if present), else default."""
    if env and env in os.environ:
        return os.environ[env]
    lookup_dict = cfg_dict if cfg_dict is not None else _FILE_CFG
    if key in lookup_dict:
        return lookup_dict[key]
    return default


# Auto-registry: every setting read through the _get_* helpers records its json
# key, env var, type and DEFAULT here, so the config builder can show the complete
# settings catalog with defaults — no hand-maintained list to drift.
_SETTINGS_REGISTRY: dict[str, dict] = {}


def _register(env: str | None, key: str, typ: str, default) -> None:
    if key:
        _SETTINGS_REGISTRY[key] = {"key": key, "env": env, "type": typ, "default": default}


# Register connection settings so they're recognized by the config builder UI
# (these are imported from connection_config, so we register them manually)
_register("DB_URL", "db_url", "str", DB_URL)
_register("DB_SOURCE_TABLE", "source_table", "str", DB_SOURCE_TABLE)
_register("KEY_COLUMN", "key_column", "str", KEY_COLUMN)
_register("TABLE_NAME", "table_name", "str", TABLE_NAME)
_register("QUERY_TIMEOUT_SECONDS", "query_timeout_seconds", "int", QUERY_TIMEOUT_SECONDS)
_register("QUERY_MAX_ROWS", "query_max_rows", "int", QUERY_MAX_ROWS)
_register("DB_MAX_RETRIES", "db_max_retries", "int", DB_MAX_RETRIES)
_register("DB_RETRY_BACKOFF_SECONDS", "db_retry_backoff_seconds", "float", DB_RETRY_BACKOFF_SECONDS)
_register("VALIDATE_SOURCE_SCHEMA", "validate_source_schema", "bool", VALIDATE_SOURCE_SCHEMA)

# Register system settings so they're recognized by the config builder UI
# (these are imported from system_config, so we register them manually)
_register("BUCKET_NAME", "bucket", "str", BUCKET_NAME)
_register("S3_ACCESS_KEY_ID", "access_key_id", "str", ACCESS_KEY_ID)
_register("S3_SECRET_ACCESS_KEY", "secret_access_key", "str", SECRET_ACCESS_KEY)
_register("REQUIRE_SIGV4", "require_sigv4", "bool", REQUIRE_SIGV4)
_register("ENABLE_STORAGE_PROXY", "enable_storage_proxy", "bool", ENABLE_STORAGE_PROXY)
_register("ENFORCE_MOUNT_AUTH", "enforce_mount_auth", "bool", ENFORCE_MOUNT_AUTH)
_register("ENABLE_AUDIT_LOG", "enable_audit_log", "bool", ENABLE_AUDIT_LOG)
_register("AUDIT_LOG_FILE", "audit_log_file", "str", AUDIT_LOG_FILE)
_register("TLS_CERT_FILE", "tls_cert_file", "str", TLS_CERT_FILE)
_register("TLS_KEY_FILE", "tls_key_file", "str", TLS_KEY_FILE)
_register("AGENT_COUNT", "agent_count", "int", AGENT_COUNT)
_register("ENABLE_GATEWAY", "enable_gateway", "bool", ENABLE_GATEWAY)
_register("SHARD_STRATEGY", "shard_strategy", "str", SHARD_STRATEGY)

# Register memory monitoring settings
_register("MEMORY_ALERT_THRESHOLD_MB", "memory_alert_threshold_mb", "int", 800)
_register("MEMORY_RESTART_THRESHOLD_MB", "memory_restart_threshold_mb", "int", 1200)
_register("MEMORY_HISTORY_SAMPLES", "memory_history_samples", "int", 60)
# Note: table_format will be re-registered when loaded from config.performance.json via _get_str()


def _get_str(env: str | None, key: str, default: str, cfg_dict: dict | None = None) -> str:
    _register(env, key, "str", default)
    v = _raw(env, key, default, cfg_dict)
    return default if v is None else str(v)


def _get_int(env: str | None, key: str, default: int, cfg_dict: dict | None = None) -> int:
    _register(env, key, "int", default)
    v = _raw(env, key, default, cfg_dict)
    return int(str(v)) if not isinstance(v, bool) else default


def _get_float(env: str | None, key: str, default: float, cfg_dict: dict | None = None) -> float:
    _register(env, key, "float", default)
    v = _raw(env, key, default, cfg_dict)
    return float(str(v)) if not isinstance(v, bool) else default


def _get_bool(env: str | None, key: str, default: bool, cfg_dict: dict | None = None) -> bool:
    _register(env, key, "bool", default)
    v = _raw(env, key, None, cfg_dict)
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() in ("1", "true", "yes", "on")


# ---------------------------------------------------------------------------
# Output table format
# ---------------------------------------------------------------------------

TABLE_FORMAT: str = _get_str("TABLE_FORMAT", "table_format", "iceberg", _PERF_CFG).strip().lower()

# ---------------------------------------------------------------------------
# Iceberg / split settings (Performance section)
# ---------------------------------------------------------------------------

NUM_SPLITS: int = _get_int("NUM_SPLITS", "num_splits", 8, _PERF_CFG)
SPLIT_STRATEGY: str = _get_str("SPLIT_STRATEGY", "split_strategy", "modulo", _PERF_CFG).strip().lower()
SPLIT_TARGET_ROWS: int = _get_int("SPLIT_TARGET_ROWS", "split_target_rows", 100_000, _PERF_CFG)
SPLIT_COUNT_MIN: int = _get_int("SPLIT_COUNT_MIN", "split_count_min", 1, _PERF_CFG)
SPLIT_COUNT_MAX: int = _get_int("SPLIT_COUNT_MAX", "split_count_max", 256, _PERF_CFG)

STREAMING_PARQUET: bool = _get_bool("STREAMING_PARQUET", "streaming_parquet", False, _PERF_CFG)
STREAM_BATCH_ROWS: int = _get_int("STREAM_BATCH_ROWS", "stream_batch_rows", 50_000, _PERF_CFG)

SOURCE_MAX_CONCURRENCY: int = _get_int("SOURCE_MAX_CONCURRENCY", "source_max_concurrency", 0, _PERF_CFG)

# Snapshot pinning
SNAPSHOT_REFRESH_ON_START: bool = True

# Resource guards
MAX_CONCURRENT_GENERATIONS: int = _get_int("MAX_CONCURRENT_GENERATIONS", "max_concurrent_generations", 4, _PERF_CFG)
MEMORY_ALERT_THRESHOLD_MB: int = _get_int("MEMORY_ALERT_THRESHOLD_MB", "memory_alert_threshold_mb", 800, _PERF_CFG)
MEMORY_RESTART_THRESHOLD_MB: int = _get_int("MEMORY_RESTART_THRESHOLD_MB", "memory_restart_threshold_mb", 1200, _PERF_CFG)
MEMORY_HISTORY_SAMPLES: int = _get_int("MEMORY_HISTORY_SAMPLES", "memory_history_samples", 60, _PERF_CFG)

# Robustness
TIMESTAMP_ASSUME_UTC: bool = _get_bool("TIMESTAMP_ASSUME_UTC", "timestamp_assume_utc", True, _PERF_CFG)
PIN_MATERIALIZED_SPLITS: bool = _get_bool("PIN_MATERIALIZED_SPLITS", "pin_materialized_splits", True, _PERF_CFG)

# ---------------------------------------------------------------------------
# Caching
# ---------------------------------------------------------------------------

METADATA_CACHE_TTL_SECONDS: int = _get_int("METADATA_CACHE_TTL", "metadata_cache_ttl", 60, _PERF_CFG)
PARQUET_CACHE_TTL_SECONDS: int = _get_int("PARQUET_CACHE_TTL", "parquet_cache_ttl", 300, _PERF_CFG)
PARQUET_CACHE_MAX_BYTES: int = _get_int("PARQUET_CACHE_MAX_BYTES", "parquet_cache_max_bytes", 256 * 1024 * 1024, _PERF_CFG)

PARQUET_DISK_CACHE: bool = _get_bool("PARQUET_DISK_CACHE", "parquet_disk_cache", False, _PERF_CFG)
PARQUET_DISK_CACHE_DIR: str = _get_str("PARQUET_DISK_CACHE_DIR", "parquet_disk_cache_dir", "./.parquet_cache", _PERF_CFG)

# ---------------------------------------------------------------------------
# Observability
# ---------------------------------------------------------------------------

REQUEST_TRACE: bool = _get_bool("REQUEST_TRACE", "request_trace", True, _PERF_CFG)
TRACE_BUFFER_SIZE: int = _get_int("TRACE_BUFFER_SIZE", "trace_buffer_size", 5000, _PERF_CFG)

# ---------------------------------------------------------------------------
# Iceberg advanced features
# ---------------------------------------------------------------------------

ICEBERG_MANIFEST_STATS: bool = _get_bool("ICEBERG_MANIFEST_STATS", "iceberg_manifest_stats", False, _PERF_CFG)
ICEBERG_SNAPSHOT_HISTORY: bool = _get_bool("ICEBERG_SNAPSHOT_HISTORY", "iceberg_snapshot_history", False, _PERF_CFG)
SNAPSHOT_HISTORY_LIMIT: int = _get_int("SNAPSHOT_HISTORY_LIMIT", "snapshot_history_limit", 10, _PERF_CFG)

CONCURRENT_STARTUP_MATERIALIZATION: bool = _get_bool(
    "CONCURRENT_STARTUP_MATERIALIZATION", "concurrent_startup_materialization", True, _PERF_CFG
)

# ---------------------------------------------------------------------------
# Data freshness (Freshness section)
# ---------------------------------------------------------------------------

AUTO_REFRESH: bool = _get_bool("AUTO_REFRESH", "auto_refresh", False, _FRESH_CFG)
REFRESH_POLL_SECONDS: int = _get_int("REFRESH_POLL_SECONDS", "refresh_poll_seconds", 600, _FRESH_CFG)
REFRESH_STRATEGY: str = _get_str("REFRESH_STRATEGY", "refresh_strategy", "auto", _FRESH_CFG)
REFRESH_ALLOW_FULL_PULL: bool = _get_bool("REFRESH_ALLOW_FULL_PULL", "refresh_allow_full_pull", False, _FRESH_CFG)
REFRESH_TTL_SECONDS: int = _get_int("REFRESH_TTL_SECONDS", "refresh_ttl_seconds", 1200, _FRESH_CFG)

# ---------------------------------------------------------------------------
# Iceberg table schema definition
# ---------------------------------------------------------------------------

@dataclass
class ColumnDef:
    """Iceberg column definition."""
    field_id: int
    name: str
    iceberg_type: str
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

@dataclass
class TableDef:
    """Iceberg table definition."""
    name: str
    source_table: str
    schema: list[ColumnDef] | None = None
    num_splits: int | None = None
    key_column: str | None = None
    connection_id: str = "default"

    def __post_init__(self):
        if self.num_splits is None:
            self.num_splits = NUM_SPLITS
        if not self.connection_id:
            self.connection_id = "default"


def _tabledef_from_json(d: dict) -> "TableDef":
    """Build a TableDef from a JSON ``tables`` entry."""
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
        connection_id=str(d.get("connection") or d.get("connection_id") or "default"),
    )


if _FILE_CFG.get("tables"):
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
# Config validation
# ---------------------------------------------------------------------------

def validate_config() -> None:
    """Validate required configuration at startup; raise ``ValueError`` on error."""
    problems: list[str] = []

    if not DB_URL:
        problems.append("DB_URL must be set (a SQLAlchemy async URL).")
    if not BUCKET_NAME:
        problems.append("S3_BUCKET must be a non-empty bucket name.")
    if OBJECT_PATH_LAYOUT not in ("legacy", "canonical"):
        problems.append(f"OBJECT_PATH_LAYOUT must be 'legacy' or 'canonical' (got {OBJECT_PATH_LAYOUT!r}).")
    if NUM_SPLITS < 1:
        problems.append(f"NUM_SPLITS must be >= 1 (got {NUM_SPLITS}).")
    if TABLE_FORMAT not in ("iceberg", "delta"):
        problems.append(f"TABLE_FORMAT must be 'iceberg' or 'delta' (got {TABLE_FORMAT!r}).")
    if SPLIT_STRATEGY not in ("modulo", "range", "date", "auto"):
        problems.append(f"SPLIT_STRATEGY must be one of 'modulo'|'range'|'date'|'auto' (got {SPLIT_STRATEGY!r}).")
    if SPLIT_TARGET_ROWS < 0:
        problems.append(f"SPLIT_TARGET_ROWS must be >= 0 (got {SPLIT_TARGET_ROWS}).")
    if SPLIT_COUNT_MIN < 1:
        problems.append(f"SPLIT_COUNT_MIN must be >= 1 (got {SPLIT_COUNT_MIN}).")
    if SPLIT_COUNT_MAX < SPLIT_COUNT_MIN:
        problems.append(f"SPLIT_COUNT_MAX must be >= SPLIT_COUNT_MIN (got {SPLIT_COUNT_MAX} < {SPLIT_COUNT_MIN}).")
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
    if AGENT_DRAIN_GRACE_SECONDS < 0:
        problems.append(f"AGENT_DRAIN_GRACE_SECONDS must be >= 0 (got {AGENT_DRAIN_GRACE_SECONDS}).")
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
    if SHARD_STRATEGY not in ("modulo", "weighted"):
        problems.append(f"SHARD_STRATEGY must be 'modulo' or 'weighted' (got {SHARD_STRATEGY!r}).")
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
        problems.append(f"MAX_CONCURRENT_GENERATIONS must be >= 1 (got {MAX_CONCURRENT_GENERATIONS}).")
    if DB_MAX_RETRIES < 0:
        problems.append(f"DB_MAX_RETRIES must be >= 0 (got {DB_MAX_RETRIES}).")
    if DB_RETRY_BACKOFF_SECONDS < 0:
        problems.append(f"DB_RETRY_BACKOFF must be >= 0 (got {DB_RETRY_BACKOFF_SECONDS}).")

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
            if t.connection_id not in CONNECTIONS:
                problems.append(
                    f"Table {t.name!r}: connection {t.connection_id!r} is not defined "
                    f"(known: {sorted(CONNECTIONS)})."
                )

    for cid, conn in CONNECTIONS.items():
        url = DB_URL if cid == "default" else conn.db_url
        if not url:
            problems.append(f"Connection {cid!r}: db_url must be set (a SQLAlchemy URL).")

    # Storage proxy: validate mounted buckets (config.mounts.json) when enabled.
    if ENABLE_STORAGE_PROXY:
        try:
            from storage.mounts import validate_mounts
            problems.extend(validate_mounts())
        except Exception as exc:  # noqa: BLE001 - a mount config error must not mask others
            problems.append(f"Storage proxy: mount config error: {exc}")

    if problems:
        raise ValueError("Invalid configuration:\n  - " + "\n  - ".join(problems))


# ---------------------------------------------------------------------------
# Settings catalog (drives the /_config "All settings" view)
# ---------------------------------------------------------------------------

SETTINGS_META: dict[str, dict] = {
    # Connection
    "db_url":        {"cat": "Connection", "help": "SQLAlchemy async connection string.", "secret": True},
    "source_table":  {"cat": "Connection", "help": "Source SQL table/view to expose."},
    "key_column":    {"cat": "Connection", "help": "Integer split/partition key column (blank = auto-detect PK)."},
    "table_name":    {"cat": "Connection", "help": "Virtual Iceberg table name (single-table mode)."},
    "query_timeout": {"cat": "Connection", "help": "Per-query timeout (seconds)."},
    "query_max_rows": {"cat": "Connection", "help": "Max rows per split query."},
    "db_max_retries": {"cat": "Connection", "help": "Retries on transient source-DB errors."},
    "db_retry_backoff": {"cat": "Connection", "help": "Linear backoff between retries (seconds)."},
    "validate_source_schema": {"cat": "Connection", "help": "Fail fast if a declared column is missing at startup."},
    # S3 / bucket
    "bucket": {"cat": "S3 endpoint", "help": "Virtual S3 bucket name Fabric connects to."},
    "warehouse_prefix": {"cat": "S3 endpoint", "help": "Path prefix inside the bucket (warehouse root)."},
    "object_path_layout": {"cat": "S3 endpoint", "help": "Virtual object layout: legacy (db/<table>) or canonical (db/<server>/<database>/<schema>/<object>)."},
    "enable_legacy_path_aliases": {"cat": "S3 endpoint", "help": "Serve legacy path aliases while canonical layout is enabled (migration safety)."},
    "access_key_id": {"cat": "S3 endpoint", "help": "S3 access key id Fabric presents."},
    "secret_access_key": {"cat": "S3 endpoint", "help": "S3 secret key (only checked when require_sigv4).", "secret": True},
    "require_sigv4": {"cat": "S3 endpoint", "help": "Enforce AWS SigV4 request signatures."},
    # Server
    "host": {"cat": "Server", "help": "Bind address."},
    "port": {"cat": "Server", "help": "Listen port."},
    "tls_cert_file": {"cat": "Server", "help": "Path to a TLS certificate (PEM). Provide with tls_key_file to serve HTTPS."},
    "tls_key_file": {"cat": "Server", "help": "Path to the TLS private key (PEM). Provide with tls_cert_file to serve HTTPS.", "secret": True},
    # Splits & query
    "num_splits": {"cat": "Splits & query", "help": "Virtual Parquet files per table."},
    "split_strategy": {"cat": "Splits & query", "help": "'modulo' (full-scan), 'range' (integer ranges), 'date' (temporal ranges), or 'auto' (range/date then deterministic fallback)."},
    "split_target_rows": {"cat": "Splits & query", "help": "Target rows per split for dynamic split-count planning (0 disables, keeps configured split counts)."},
    "split_count_min": {"cat": "Splits & query", "help": "Lower guardrail for dynamic split-count planning."},
    "split_count_max": {"cat": "Splits & query", "help": "Upper guardrail for dynamic split-count planning."},
    "streaming_parquet": {"cat": "Splits & query", "help": "Materialize each split in row batches (bounded memory) instead of loading the whole split into RAM."},
    "stream_batch_rows": {"cat": "Splits & query", "help": "Batch/row-group size for streaming Parquet materialization."},
    "source_max_concurrency": {"cat": "Splits & query", "help": "Cap concurrent SQL queries against the source DB (backpressure). 0 = unlimited."},
    "table_format": {"cat": "Splits & query", "help": "Output format: 'iceberg' (Fabric virtualizes to Delta) or 'delta' (native, no conversion — lower lag)."},
    # Caching
    "pin_materialized_splits": {"cat": "Caching", "help": "Serve snapshot data files byte-identical (prevents size drift). Keep on."},
    "parquet_disk_cache": {"cat": "Caching", "help": "Persist generated Parquet to disk for warm restarts."},
    "parquet_disk_cache_dir": {"cat": "Caching", "help": "Directory for the disk Parquet cache."},
    "parquet_cache_max_bytes": {"cat": "Caching", "help": "In-memory Parquet LRU cap (bytes)."},
    "parquet_cache_ttl": {"cat": "Caching", "help": "In-memory Parquet entry TTL (seconds)."},
    "metadata_cache_ttl": {"cat": "Caching", "help": "Metadata cache TTL (seconds)."},
    # Robustness
    "max_concurrent_generations": {"cat": "Robustness", "help": "Max simultaneous on-demand Parquet builds."},
    "memory_alert_threshold_mb": {"cat": "Robustness", "help": "Alert when agent process RAM exceeds this (MB). 0 = disabled."},
    "memory_restart_threshold_mb": {"cat": "Robustness", "help": "Restart agent when RAM exceeds this (MB). 0 = disabled."},
    "memory_history_samples": {"cat": "Robustness", "help": "Number of historical memory samples to retain for trend analysis."},
    "timestamp_assume_utc": {"cat": "Robustness", "help": "Map naive datetimes to timestamptz (Fabric SQL endpoint rejects TIMESTAMP_NTZ)."},
    # Admin UIs / observability
    "enable_config_builder": {"cat": "Admin & observability", "help": "Serve config builder at /_config."},
    "enable_monitor": {"cat": "Admin & observability", "help": "Serve the monitoring dashboard at /_monitor."},
    "enable_storage_proxy": {"cat": "Admin & observability", "help": "Serve mounted buckets (config.mounts.json) as byte passthrough from S3/NFS/SMB backends, alongside the DB->Iceberg path."},
    "enforce_mount_auth": {"cat": "Admin & observability", "help": "Require SigV4 auth on mounted buckets even when require_sigv4 is off (a secured proxy never serves a mount unauthenticated)."},
    "enable_audit_log": {"cat": "Admin & observability", "help": "Emit a structured audit event for every mounted-object access (identity, bucket, key, bytes), secrets scrubbed."},
    "audit_log_file": {"cat": "Admin & observability", "help": "Optional file path for audit events (empty = the structured logger only)."},
    "request_trace": {"cat": "Admin & observability", "help": "Record the Fabric request timeline."},
    "trace_buffer_size": {"cat": "Admin & observability", "help": "Max request-trace records kept in memory."},
    # Iceberg advanced
    "iceberg_manifest_stats": {"cat": "Iceberg (advanced)", "help": "Emit column min/max stats in manifests (validate with Fabric first)."},
    "iceberg_snapshot_history": {"cat": "Iceberg (advanced)", "help": "Retain snapshot versions for time-travel."},
    "snapshot_history_limit": {"cat": "Iceberg (advanced)", "help": "Max retained snapshot versions."},
    "concurrent_startup_materialization": {"cat": "Iceberg (advanced)", "help": "Materialize splits concurrently at startup."},
    # Data freshness
    "auto_refresh": {"cat": "Data freshness", "help": "Re-read source & publish new snapshots."},
    "refresh_poll_seconds": {"cat": "Data freshness", "help": "Poll interval (seconds)."},
    "refresh_strategy": {"cat": "Data freshness", "help": "auto | dialect_probe | content_hash | ttl | manual."},
    "refresh_allow_full_pull": {"cat": "Data freshness", "help": "In auto, allow full read when the probe is unavailable."},
    "refresh_ttl_seconds": {"cat": "Data freshness", "help": "Window for the ttl strategy (seconds)."},
    # Cluster / scale
    "artifact_store_backend": {"cat": "Cluster (scale)", "help": "Durable artifact store backend: 'local' (filesystem/NFS/SMB) or 'memory' (ephemeral)."},
    "artifact_store_dir": {"cat": "Cluster (scale)", "help": "Root directory for the local artifact store."},
    "artifact_store_serving": {"cat": "Cluster (scale)", "help": "Serve materialized Parquet from the artifact store (durable, shareable; zero regeneration on restart)."},
    "publish_serving_image": {"cat": "Cluster (scale)", "help": "Publish a complete servable image (data + metadata) to the store at startup."},
    "manager_url": {"cat": "Cluster (scale)", "help": "Agent: Manager control URL to register/heartbeat (blank = standalone, no cluster)."},
    "agent_id": {"cat": "Cluster (scale)", "help": "Agent: stable id (blank = auto from host:port)."},
    "agent_advertise_host": {"cat": "Cluster (scale)", "help": "Agent: routable host/IP or DNS advertised to the Manager so an external LB/gateway can dial it (blank = advertise the bind host). Set for a multi-host fleet."},
    "control_host": {"cat": "Cluster (scale)", "help": "Manager: control-plane REST bind address."},
    "control_port": {"cat": "Cluster (scale)", "help": "Manager: control-plane REST port."},
    "heartbeat_ms": {"cat": "Cluster (scale)", "help": "Agent heartbeat interval (ms)."},
    "agent_drain_grace_seconds": {"cat": "Cluster (scale)", "help": "Agent: on drain, serve /readyz 503 then wait this many seconds before exiting so a load balancer can deregister the backend and in-flight requests finish."},
    "heartbeat_miss_limit": {"cat": "Cluster (scale)", "help": "Manager marks an Agent dead after this many missed heartbeats."},
    "agent_restart_backoff": {"cat": "Cluster (scale)", "help": "Manager: delay before respawning a crashed Agent (seconds)."},
    "agent_max_rapid_restarts": {"cat": "Cluster (scale)", "help": "Manager: crash-loop guard — stop respawning after this many restarts in the window."},
    "agent_count": {"cat": "Cluster (scale)", "help": "Manager: number of Agents to supervise (each on PORT+i)."},
    "agent_shard_index": {"cat": "Cluster (scale)", "help": "This Agent's materialization shard (set by the Manager)."},
    "agent_shard_count": {"cat": "Cluster (scale)", "help": "Total materialization shards (= agent_count)."},
    "shard_strategy": {"cat": "Cluster (scale)", "help": "Split-ownership across shards: 'modulo' (round-robin by split index) or 'weighted' (size-weighted, balances bytes using observed split sizes from the prior run; needs a shared artifact store). Restart to apply.", "choices": ["modulo", "weighted"]},
    "enable_gateway": {"cat": "Cluster (scale)", "help": "Manager: front the Agent fleet with a built-in round-robin S3 gateway."},
    "materialize_wait_seconds": {"cat": "Cluster (scale)", "help": "Non-owner Agent: max wait for a sharded split to appear in the store before generating it locally."},
    "enable_admin_ui": {"cat": "Cluster (scale)", "help": "Manager: serve the /_manager operator console (fleet monitor + start/stop/restart/drain)."},
    "admin_token": {"cat": "Cluster (scale)", "help": "Manager: token required for mutating /_manager actions (X-Admin-Token header or ?token=). Blank = no auth.", "secret": True},
    "manager_ha": {"cat": "Cluster (scale)", "help": "Manager HA: run a leader lease over the shared store; only the primary supervises Agents + serves the gateway."},
    "leader_lease_ttl_ms": {"cat": "Cluster (scale)", "help": "Leader lease TTL (ms): a standby takes over if the primary doesn't renew within this window."},
    "leader_lease_renew_ms": {"cat": "Cluster (scale)", "help": "Leader lease renew interval (ms); must be < TTL."},
    "retention_gc": {"cat": "Cluster (scale)", "help": "Agent (shard 0): periodically prune orphaned Parquet splits from the shared store."},
    "retention_gc_interval_seconds": {"cat": "Cluster (scale)", "help": "Retention GC sweep interval (seconds)."},
    "rolling_restart_health_timeout": {"cat": "Cluster (scale)", "help": "Rolling restart: max seconds to wait for each Agent to become healthy before the next."},
}

_SETTINGS_CAT_ORDER = [
    "Connection", "S3 endpoint", "Server", "Splits & query", "Caching",
    "Robustness", "Admin & observability", "Iceberg (advanced)", "Data freshness",
    "Cluster (scale)", "Other",
]


# ---------------------------------------------------------------------------
# Live-applicable settings — can be mutated in the running process without
# a restart. Structural settings (DB_URL, PORT, bucket, HA, control plane)
# always require a restart.
# ---------------------------------------------------------------------------

LIVE_SETTINGS: frozenset[str] = frozenset({
    # Performance / splits
    "num_splits", "split_strategy", "split_target_rows",
    "split_count_min", "split_count_max",
    "streaming_parquet", "stream_batch_rows",
    "source_max_concurrency", "max_concurrent_generations",
    "timestamp_assume_utc", "pin_materialized_splits",
    "table_format",
    # Caching
    "metadata_cache_ttl", "parquet_cache_ttl",
    "parquet_cache_max_bytes",
    "parquet_disk_cache", "parquet_disk_cache_dir",
    # Observability
    "request_trace", "trace_buffer_size",
    # Storage-proxy security (Phase 4) — read per request, safe to change live
    "enforce_mount_auth", "enable_audit_log", "audit_log_file",
    # Iceberg
    "iceberg_manifest_stats", "iceberg_snapshot_history",
    "snapshot_history_limit", "concurrent_startup_materialization",
    # Freshness
    "auto_refresh", "refresh_poll_seconds", "refresh_strategy",
    "refresh_allow_full_pull", "refresh_ttl_seconds",
    # Connection limits (safe to change mid-flight)
    "query_timeout_seconds", "query_max_rows",
})

# Mapping from config.json key -> module-level attribute name (UPPER_CASE).
_KEY_TO_ATTR: dict[str, str] = {
    "num_splits": "NUM_SPLITS",
    "split_strategy": "SPLIT_STRATEGY",
    "split_target_rows": "SPLIT_TARGET_ROWS",
    "split_count_min": "SPLIT_COUNT_MIN",
    "split_count_max": "SPLIT_COUNT_MAX",
    "streaming_parquet": "STREAMING_PARQUET",
    "stream_batch_rows": "STREAM_BATCH_ROWS",
    "source_max_concurrency": "SOURCE_MAX_CONCURRENCY",
    "max_concurrent_generations": "MAX_CONCURRENT_GENERATIONS",
    "memory_alert_threshold_mb": "MEMORY_ALERT_THRESHOLD_MB",
    "memory_restart_threshold_mb": "MEMORY_RESTART_THRESHOLD_MB",
    "memory_history_samples": "MEMORY_HISTORY_SAMPLES",
    "timestamp_assume_utc": "TIMESTAMP_ASSUME_UTC",
    "pin_materialized_splits": "PIN_MATERIALIZED_SPLITS",
    "bucket": "BUCKET_NAME",
    "require_sigv4": "REQUIRE_SIGV4",
    "enforce_mount_auth": "ENFORCE_MOUNT_AUTH",
    "enable_audit_log": "ENABLE_AUDIT_LOG",
    "audit_log_file": "AUDIT_LOG_FILE",
    "tls_cert_file": "TLS_CERT_FILE",
    "tls_key_file": "TLS_KEY_FILE",
    "agent_count": "AGENT_COUNT",
    "shard_strategy": "SHARD_STRATEGY",
    "table_format": "TABLE_FORMAT",
    "metadata_cache_ttl": "METADATA_CACHE_TTL_SECONDS",
    "parquet_cache_ttl": "PARQUET_CACHE_TTL_SECONDS",
    "parquet_cache_max_bytes": "PARQUET_CACHE_MAX_BYTES",
    "parquet_disk_cache": "PARQUET_DISK_CACHE",
    "parquet_disk_cache_dir": "PARQUET_DISK_CACHE_DIR",
    "request_trace": "REQUEST_TRACE",
    "trace_buffer_size": "TRACE_BUFFER_SIZE",
    "iceberg_manifest_stats": "ICEBERG_MANIFEST_STATS",
    "iceberg_snapshot_history": "ICEBERG_SNAPSHOT_HISTORY",
    "snapshot_history_limit": "SNAPSHOT_HISTORY_LIMIT",
    "concurrent_startup_materialization": "CONCURRENT_STARTUP_MATERIALIZATION",
    "auto_refresh": "AUTO_REFRESH",
    "refresh_poll_seconds": "REFRESH_POLL_SECONDS",
    "refresh_strategy": "REFRESH_STRATEGY",
    "refresh_allow_full_pull": "REFRESH_ALLOW_FULL_PULL",
    "refresh_ttl_seconds": "REFRESH_TTL_SECONDS",
    "query_timeout_seconds": "QUERY_TIMEOUT_SECONDS",
    "query_max_rows": "QUERY_MAX_ROWS",
    "access_key_id": "ACCESS_KEY_ID",
    "secret_access_key": "SECRET_ACCESS_KEY",
}


def settings_catalog() -> list[dict]:
    """Return every registered setting with its json key, env var, type, default, category and help."""
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
            "choices": meta.get("choices"),
            "live": reg["key"] in LIVE_SETTINGS,
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
    """The JSON config path this process reads (``$CONFIG_FILE`` or config.json).
    
    Note: This is for backward compatibility only. New code should use separate
    config files (config.system.json, config.connection.json, etc.).
    """
    return os.environ.get("CONFIG_FILE", "config.json")


# Mapping of settings to their config section files
_SETTINGS_TO_FILE_MAP: dict[str, str] = {
    # System settings → config.system.json
    "bucket": "config.system.json",
    "warehouse_prefix": "config.system.json",
    "object_path_layout": "config.system.json",
    "enable_legacy_path_aliases": "config.system.json",
    "access_key_id": "config.system.json",
    "secret_access_key": "config.system.json",
    "require_sigv4": "config.system.json",
    "host": "config.system.json",
    "port": "config.system.json",
    "enable_config_builder": "config.system.json",
    "enable_monitor": "config.system.json",
    "enable_storage_proxy": "config.system.json",
    "enforce_mount_auth": "config.system.json",
    "enable_audit_log": "config.system.json",
    "audit_log_file": "config.system.json",
    "tls_cert_file": "config.system.json",
    "tls_key_file": "config.system.json",
    "artifact_store_backend": "config.system.json",
    "artifact_store_dir": "config.system.json",
    "artifact_store_serving": "config.system.json",
    "publish_serving_image": "config.system.json",
    "agent_count": "config.system.json",
    "agent_shard_index": "config.system.json",
    "agent_shard_count": "config.system.json",
    "shard_strategy": "config.system.json",
    "enable_gateway": "config.system.json",
    "materialize_wait_seconds": "config.system.json",
    "manager_url": "config.system.json",
    "agent_id": "config.system.json",
    "agent_advertise_host": "config.system.json",
    "control_host": "config.system.json",
    "control_port": "config.system.json",
    "heartbeat_ms": "config.system.json",
    "agent_drain_grace_seconds": "config.system.json",
    "heartbeat_miss_limit": "config.system.json",
    "agent_restart_backoff_seconds": "config.system.json",
    "agent_max_rapid_restarts": "config.system.json",
    "manager_ha": "config.system.json",
    "leader_lease_ttl_ms": "config.system.json",
    "leader_lease_renew_ms": "config.system.json",
    "retention_gc": "config.system.json",
    "retention_gc_interval_seconds": "config.system.json",
    "rolling_restart_health_timeout": "config.system.json",
    "enable_admin_ui": "config.system.json",
    "admin_token": "config.system.json",
    # Connection settings → config.connection.json
    "db_url": "config.connection.json",
    "source_table": "config.connection.json",
    "key_column": "config.connection.json",
    "table_name": "config.connection.json",
    "query_timeout_seconds": "config.connection.json",
    "query_max_rows": "config.connection.json",
    "db_max_retries": "config.connection.json",
    "db_retry_backoff_seconds": "config.connection.json",
    "validate_source_schema": "config.connection.json",
    # Performance settings → config.performance.json
    "num_splits": "config.performance.json",
    "split_strategy": "config.performance.json",
    "split_target_rows": "config.performance.json",
    "split_count_min": "config.performance.json",
    "split_count_max": "config.performance.json",
    "streaming_parquet": "config.performance.json",
    "stream_batch_rows": "config.performance.json",
    "source_max_concurrency": "config.performance.json",
    "max_concurrent_generations": "config.performance.json",
    "memory_alert_threshold_mb": "config.performance.json",
    "memory_restart_threshold_mb": "config.performance.json",
    "memory_history_samples": "config.performance.json",
    "timestamp_assume_utc": "config.performance.json",
    "table_format": "config.performance.json",
    "pin_materialized_splits": "config.performance.json",
    "parquet_disk_cache": "config.performance.json",
    "parquet_disk_cache_dir": "config.performance.json",
    "parquet_cache_max_bytes": "config.performance.json",
    "parquet_cache_ttl": "config.performance.json",
    "metadata_cache_ttl": "config.performance.json",
    "request_trace": "config.performance.json",
    "trace_buffer_size": "config.performance.json",
    "iceberg_manifest_stats": "config.performance.json",
    "iceberg_snapshot_history": "config.performance.json",
    "snapshot_history_limit": "config.performance.json",
    "concurrent_startup_materialization": "config.performance.json",
    # Freshness settings → config.freshness.json
    "auto_refresh": "config.freshness.json",
    "refresh_poll_seconds": "config.freshness.json",
    "refresh_strategy": "config.freshness.json",
    "refresh_allow_full_pull": "config.freshness.json",
    "refresh_ttl_seconds": "config.freshness.json",
}


def _coerce_setting_value(typ: str, value):
    """Coerce a JSON/string ``value`` to a setting's declared type."""
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
    """Every registered setting with its CURRENT effective value + source."""
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
            "choices": meta.get("choices"),
            "live": key in LIVE_SETTINGS,
        })
    out.sort(key=lambda s: (
        _SETTINGS_CAT_ORDER.index(s["category"]) if s["category"] in _SETTINGS_CAT_ORDER else 999,
        s["key"],
    ))
    return out


def validate_setting_updates(updates: dict) -> tuple[dict, list[str]]:
    """Validate a partial settings map. Returns ``(clean, errors)``.
    
    Special handling: "tables" is allowed as a non-scalar (array of table dicts)
    and "connections" as an array of ``{id, db_url}`` source definitions; both
    pass through without registry validation.
    """
    clean: dict = {}
    errors: list[str] = []
    for k, v in (updates or {}).items():
        # Special case: "tables" is an array of table configs, not a scalar setting
        if k == "tables":
            if not isinstance(v, list):
                errors.append(f"{k}: must be a list")
            else:
                names = [str((t or {}).get("name") or "") for t in v if isinstance(t, dict)]
                dups = sorted({n for n in names if n and names.count(n) > 1})
                if dups:
                    errors.append(f"tables: duplicate table name(s) {dups} — names must be unique across all sources")
                else:
                    clean[k] = v  # Pass through as-is
            continue

        # Special case: "connections" is an array of named source definitions.
        if k == "connections":
            if not isinstance(v, list):
                errors.append(f"{k}: must be a list")
                continue
            ok = True
            seen: set[str] = set()
            for i, e in enumerate(v):
                if not isinstance(e, dict):
                    errors.append(f"{k}[{i}]: must be an object with 'id' and 'db_url'")
                    ok = False
                    continue
                cid = str(e.get("id", "")).strip()
                url = str(e.get("db_url", "")).strip()
                if not cid or not url:
                    errors.append(f"{k}[{i}]: needs non-empty 'id' and 'db_url'")
                    ok = False
                elif cid == "default":
                    errors.append(f"{k}[{i}]: id 'default' is reserved (use the db_url field)")
                    ok = False
                elif cid in seen:
                    errors.append(f"{k}[{i}]: duplicate connection id {cid!r}")
                    ok = False
                else:
                    seen.add(cid)
            if ok:
                clean[k] = v
            continue

        reg = _SETTINGS_REGISTRY.get(k)
        if reg is None:
            errors.append(f"unknown setting {k!r}")
            continue
        try:
            coerced = _coerce_setting_value(reg["type"], v)
        except (ValueError, TypeError) as exc:
            errors.append(f"{k}: {exc}")
            continue
        choices = SETTINGS_META.get(k, {}).get("choices")
        if choices and str(coerced) not in [str(c) for c in choices]:
            errors.append(f"{k}: must be one of {choices} (got {coerced!r})")
            continue
        clean[k] = coerced
    return clean, errors


def apply_live_settings(updates: dict) -> dict:
    """Apply validated settings directly to this module's runtime attributes.

    Returns ``{"applied": [keys], "restart_required": [keys]}``.
    Settings in ``LIVE_SETTINGS`` are set immediately (next access picks up the
    new value); all others are reported as restart_required so the caller can
    still persist them to config.json for the next start.
    """
    clean, errors = validate_setting_updates(updates)
    if errors:
        raise ValueError("; ".join(errors))
    this = sys.modules[__name__]
    applied: list[str] = []
    restart_required: list[str] = []
    for k, v in clean.items():
        if k in LIVE_SETTINGS:
            attr = _KEY_TO_ATTR.get(k)
            if attr:
                setattr(this, attr, v)
                applied.append(k)
            else:
                restart_required.append(k)  # in LIVE_SETTINGS but no attr map yet
        else:
            restart_required.append(k)
    return {"applied": applied, "restart_required": restart_required}


def write_config_updates(updates: dict) -> dict:
    """Validate + merge ``updates`` into the appropriate separate config files (atomic writes).
    
    Handles individual settings via _SETTINGS_TO_FILE_MAP plus special handling for
    the "tables" array (config.tables.json) and the "connections" array of named
    source definitions (top-level in config.connection.json).
    """
    # Validate all settings (including "tables" which is allowed as special case)
    clean, errors = validate_setting_updates(updates)
    if errors:
        raise ValueError("; ".join(errors))

    if not clean:
        return {"path": "none", "changed": [], "config": {}}

    # Extract array specials (not scalar settings) before per-key file routing.
    tables_update = clean.pop("tables", None)
    connections_update = clean.pop("connections", None)

    # Group settings by their target config file
    settings_by_file: dict[str, dict] = {}
    for key, value in clean.items():
        target_file = _SETTINGS_TO_FILE_MAP.get(key)
        if target_file:
            if target_file not in settings_by_file:
                settings_by_file[target_file] = {}
            settings_by_file[target_file][key] = value

    # Add tables to config.tables.json if provided
    if tables_update is not None:
        if "config.tables.json" not in settings_by_file:
            settings_by_file["config.tables.json"] = {}
        # For tables, we store it directly as the section content (special case)
        settings_by_file["config.tables.json"]["_tables"] = tables_update

    # Named connections live at the TOP LEVEL of config.connection.json (a sibling
    # of the singular "connection" section), matching the loader's expectations.
    if connections_update is not None:
        if "config.connection.json" not in settings_by_file:
            settings_by_file["config.connection.json"] = {}
        settings_by_file["config.connection.json"]["_connections"] = connections_update

    # Write to each target file
    changed_keys = list(clean.keys())
    if tables_update is not None:
        changed_keys.append("tables")
    if connections_update is not None:
        changed_keys.append("connections")
    
    for target_file, file_settings in settings_by_file.items():
        try:
            # Read existing config from the file
            try:
                with open(target_file, "r", encoding="utf-8-sig") as fh:
                    existing = json.load(fh)
                if not isinstance(existing, dict):
                    existing = {}
            except FileNotFoundError:
                existing = {}

            # Extract the section (e.g., for config.system.json, the section is "system")
            section_name = target_file.split("config.")[-1].split(".json")[0]  # "system", "connection", etc.

            # Pull special array markers out so they don't merge into the section.
            tables_marker = file_settings.pop("_tables", None)
            connections_marker = file_settings.pop("_connections", None)

            if tables_marker is not None:
                # config.tables.json -> {"tables": [...]}
                existing[section_name] = tables_marker
            elif file_settings:
                if not isinstance(existing.get(section_name), dict):
                    existing[section_name] = {}
                existing[section_name].update(file_settings)

            if connections_marker is not None:
                # Top-level "connections" array (sibling of the "connection" section).
                existing["connections"] = connections_marker

            # Atomic write with temp file
            tmp = f"{target_file}.tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(existing, fh, indent=2, sort_keys=True)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, target_file)

        except (OSError, ValueError) as exc:
            raise ValueError(f"Failed to write {target_file}: {exc}") from exc

    # Return summary (use the first config file as reference path for logging)
    first_file = next(iter(settings_by_file.keys())) if settings_by_file else "config.json"
    return {"path": os.path.abspath(first_file), "changed": sorted(changed_keys), "config": clean}

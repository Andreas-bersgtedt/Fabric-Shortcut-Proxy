"""
Connection configuration — source database access & credentials.

Database URL, query limits, connection pool settings, and source table defaults.
These settings change per deployment and are isolated to enable future multi-source
support without touching system or performance configuration.

Settings resolve with this precedence (highest wins):
    1. environment variable
    2. external JSON config file (``config.connection.json``)
    3. built-in default

Note: Monolithic config.json is no longer supported. Use config.connection.json.
"""
from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass


# ---------------------------------------------------------------------------
# JSON Config loading (shared)
# ---------------------------------------------------------------------------

def _load_config_file() -> dict:
    """Load the raw config.connection.json document.

    Returns the whole top-level object so both the singular ``connection``
    section (back-compat) and the multi-source ``connections`` array can be
    resolved from it.

    Precedence:
      1. config.connection.json
      2. empty dict (no fallback to monolithic config.json)
    """
    section_path = "config.connection.json"
    try:
        with open(section_path, "r", encoding="utf-8-sig") as fh:
            data = json.load(fh)
        if not isinstance(data, dict):
            print(f"[connection_config] {section_path}: top-level JSON must be an object; ignoring.", file=sys.stderr)
            return {}
        return data
    except FileNotFoundError:
        print(f"[connection_config] {section_path}: file not found; using defaults only.", file=sys.stderr)
        return {}
    except (OSError, json.JSONDecodeError) as exc:
        print(f"[connection_config] failed to read {section_path!r}: {exc}", file=sys.stderr)
        return {}


# Whole config.connection.json document (may hold a singular ``connection``
# section and/or a ``connections`` array for multi-source deployments).
_FILE_RAW: dict = _load_config_file()

# Singular connection section drives the DEFAULT connection's scalar settings
# (db_url, source_table, query limits). Back-compat: when there's no explicit
# "connection" wrapper the whole file is treated as that section.
_CONN_CFG: dict = _FILE_RAW.get("connection", _FILE_RAW) if "connection" in _FILE_RAW else _FILE_RAW
if not isinstance(_CONN_CFG, dict):
    _CONN_CFG = {}

# Retained for the import-time credential gate (validates the default section).
_FILE_CFG: dict = _CONN_CFG


def _raw(env: str | None, key: str, default):
    """Return env var (if set), else JSON value (if present), else default."""
    if env and env in os.environ:
        return os.environ[env]
    if key in _CONN_CFG:
        return _CONN_CFG[key]
    return default


def _get_str(env: str | None, key: str, default: str) -> str:
    v = _raw(env, key, default)
    return default if v is None else str(v)


def _get_int(env: str | None, key: str, default: int) -> int:
    v = _raw(env, key, default)
    return int(str(v)) if not isinstance(v, bool) else default


def _get_float(env: str | None, key: str, default: float) -> float:
    v = _raw(env, key, default)
    return float(str(v)) if not isinstance(v, bool) else default


# ---------------------------------------------------------------------------
# Source Database Connection
# ---------------------------------------------------------------------------

# SQLAlchemy async connection URL.
# SECURITY: For real databases, set the DB_URL environment variable (never hardcode
# passwords). The default below is the credential-free local SQLite POC database.
# Examples:
#   PostgreSQL : postgresql+asyncpg://user:pass@host/db
#   SQL Server : mssql+aioodbc://user:pass@host/db?driver=ODBC+Driver+18+for+SQL+Server
#   Oracle     : oracle+oracledb://user:pass@host:1521/ORCL
#   Databricks : databricks://token:dapi...@<server-hostname>/<catalog>/<schema>
#   SQLite     : sqlite+aiosqlite:///./poc_source.db
DB_URL: str = _get_str("DB_URL", "db_url", "sqlite+aiosqlite:///./poc_source.db")

# Single-table mode defaults (legacy; prefer config.json "tables" array)
DB_SOURCE_TABLE: str = _get_str("DB_SOURCE_TABLE", "source_table", "sales")
KEY_COLUMN: str = _get_str("KEY_COLUMN", "key_column", "")
TABLE_NAME: str = _get_str("TABLE_NAME", "table_name", "sales")

# ---------------------------------------------------------------------------
# Query Execution Limits
# ---------------------------------------------------------------------------

# Per-query timeout (seconds)
QUERY_TIMEOUT_SECONDS: int = _get_int("QUERY_TIMEOUT", "query_timeout", 30)

# Max rows returned per split query
QUERY_MAX_ROWS: int = _get_int("QUERY_MAX_ROWS", "query_max_rows", 500_000)

# ---------------------------------------------------------------------------
# Retry & Resilience (Source DB)
# ---------------------------------------------------------------------------

# Retries on transient source-DB errors
DB_MAX_RETRIES: int = _get_int("DB_MAX_RETRIES", "db_max_retries", 2)

# Linear backoff between retries (seconds)
DB_RETRY_BACKOFF_SECONDS: float = _get_float("DB_RETRY_BACKOFF", "db_retry_backoff", 0.5)

# Validate that the source table exposes every declared column at startup
VALIDATE_SOURCE_SCHEMA: bool = _raw("VALIDATE_SOURCE_SCHEMA", "validate_source_schema", None)
if VALIDATE_SOURCE_SCHEMA is None:
    VALIDATE_SOURCE_SCHEMA = True
else:
    VALIDATE_SOURCE_SCHEMA = str(VALIDATE_SOURCE_SCHEMA).strip().lower() in ("1", "true", "yes", "on")

# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

import re as _re
from security.credentials import scrub_database_url, validate_no_hardcoded_credentials


def redact_db_url(url: str) -> str:
    """Mask password in DB URL for safe logging.
    
    ``scheme://user:password@host/db`` -> ``scheme://user:***@host/db``.
    Uses the security module for consistent credential scrubbing.
    """
    return scrub_database_url(url)


# ---------------------------------------------------------------------------
# Multi-source connection registry
# ---------------------------------------------------------------------------
# The DEFAULT connection is derived from DB_URL (env / singular ``connection``
# section) exactly as before. Additional named sources are declared in a
# top-level ``connections`` array in config.connection.json; each table binds to
# one via its ``connection`` id (default: "default"). Per-connection query
# limits are optional and fall back to the global defaults above.


@dataclass(frozen=True)
class Connection:
    """A single source database connection and its effective query limits."""
    id: str
    db_url: str
    query_timeout_seconds: int
    query_max_rows: int
    db_max_retries: int
    db_retry_backoff_seconds: float
    validate_source_schema: bool


def _connection_from_json(d: dict) -> Connection:
    """Build a Connection from a ``connections[]`` entry (limits fall back to defaults)."""
    cid = str(d.get("id") or d.get("name") or "").strip()
    return Connection(
        id=cid,
        db_url=str(d.get("db_url") or ""),
        query_timeout_seconds=int(d.get("query_timeout_seconds", QUERY_TIMEOUT_SECONDS)),
        query_max_rows=int(d.get("query_max_rows", QUERY_MAX_ROWS)),
        db_max_retries=int(d.get("db_max_retries", DB_MAX_RETRIES)),
        db_retry_backoff_seconds=float(d.get("db_retry_backoff_seconds", DB_RETRY_BACKOFF_SECONDS)),
        validate_source_schema=bool(d.get("validate_source_schema", VALIDATE_SOURCE_SCHEMA)),
    )


def _default_connection() -> Connection:
    return Connection(
        id="default",
        db_url=DB_URL,
        query_timeout_seconds=QUERY_TIMEOUT_SECONDS,
        query_max_rows=QUERY_MAX_ROWS,
        db_max_retries=DB_MAX_RETRIES,
        db_retry_backoff_seconds=DB_RETRY_BACKOFF_SECONDS,
        validate_source_schema=VALIDATE_SOURCE_SCHEMA,
    )


def _build_connections() -> dict[str, "Connection"]:
    """Assemble the connection registry: the DEFAULT source plus named sources.

    Each ``connections[]`` entry is credential-gated like the default section.
    The id ``"default"`` is reserved (derived from DB_URL) and skipped here.
    """
    conns: dict[str, Connection] = {}
    raw_list = _FILE_RAW.get("connections")
    if isinstance(raw_list, list):
        for entry in raw_list:
            if not isinstance(entry, dict):
                continue
            validate_no_hardcoded_credentials(entry)
            c = _connection_from_json(entry)
            if not c.id:
                print("[connection_config] connections[] entry missing 'id'; skipped.", file=sys.stderr)
                continue
            if c.id == "default":
                print("[connection_config] connections[] id 'default' is reserved (use db_url/DB_URL); entry ignored.", file=sys.stderr)
                continue
            if not c.db_url:
                print(f"[connection_config] connection {c.id!r} missing db_url; skipped.", file=sys.stderr)
                continue
            if c.id in conns:
                print(f"[connection_config] duplicate connection id {c.id!r}; last wins.", file=sys.stderr)
            conns[c.id] = c
    conns["default"] = _default_connection()
    return conns


# Validate on import: ensure no hardcoded credentials in config, then build the
# connection registry (each named entry is credential-gated in _build_connections).
try:
    validate_no_hardcoded_credentials(_FILE_CFG)
    CONNECTIONS: dict[str, Connection] = _build_connections()
except ValueError as e:
    print(f"[connection_config] SECURITY ERROR: {e}", file=sys.stderr)
    sys.exit(1)


def get_connection(connection_id: str | None) -> Connection | None:
    """Return the Connection for an id (``None``/unknown -> the default)."""
    return CONNECTIONS.get(connection_id or "default")


def connection_ids() -> list[str]:
    """All registered connection ids (always includes ``"default"``)."""
    return list(CONNECTIONS.keys())

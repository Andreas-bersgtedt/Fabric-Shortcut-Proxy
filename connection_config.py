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


# ---------------------------------------------------------------------------
# JSON Config loading (shared)
# ---------------------------------------------------------------------------

def _load_config_file() -> dict:
    """Load connection configuration from config.connection.json.
    
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
        # Extract 'connection' section if present
        return data.get("connection", data) if "connection" in data else data
    except FileNotFoundError:
        print(f"[connection_config] {section_path}: file not found; using defaults only.", file=sys.stderr)
        return {}
    except (OSError, json.JSONDecodeError) as exc:
        print(f"[connection_config] failed to read {section_path!r}: {exc}", file=sys.stderr)
        return {}


_FILE_CFG: dict = _load_config_file()
_CONN_CFG: dict = _FILE_CFG


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


# Validate on import: ensure no hardcoded credentials in config
try:
    validate_no_hardcoded_credentials(_FILE_CFG)
except ValueError as e:
    print(f"[connection_config] SECURITY ERROR: {e}", file=sys.stderr)
    sys.exit(1)

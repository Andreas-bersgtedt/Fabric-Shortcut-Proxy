"""
Async SQL execution layer.

Provides:
  - a single shared async engine (connection pool)
  - execute_split_query(): runs a parameterised query and returns rows as
    a list of dicts, with query timeout and bounded retry.
"""
from __future__ import annotations

import asyncio
import contextlib
import time
from typing import Any

from sqlalchemy import text, inspect
from sqlalchemy import types as satypes
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

import config
from observability.logging import get_logger
from observability import metrics

log = get_logger(__name__)

_engine: AsyncEngine | None = None


class SourceUnavailable(RuntimeError):
    """Raised when the source database cannot satisfy a query after retries.

    The S3 router maps this to a ``503 ServiceUnavailable`` response so clients
    can back off and retry, instead of a bare 500.
    """


def get_engine() -> AsyncEngine:
    global _engine
    if _engine is None:
        kwargs: dict = {"echo": False}
        # SQLite (including aiosqlite) uses StaticPool and does not accept
        # pool_size / max_overflow / pool_timeout.
        if "sqlite" not in config.DB_URL:
            kwargs.update({"pool_size": 5, "max_overflow": 10, "pool_timeout": 30})
        _engine = create_async_engine(config.DB_URL, **kwargs)
    return _engine


# --- Source backpressure (Phase 4) ------------------------------------------
# A per-Agent cap on concurrent SQL queries against the source DB. Applied to
# BOTH startup materialization and on-demand serving-time regeneration, so a
# fleet doesn't stampede the source. 0 = unlimited (a null gate, behavior
# unchanged). The semaphore is (re)bound to the running loop lazily so it works
# cleanly across the app and isolated test event loops.
_source_sem: asyncio.Semaphore | None = None
_source_sem_limit: int = -1
_source_sem_loop: asyncio.AbstractEventLoop | None = None


@contextlib.asynccontextmanager
async def _null_gate():
    yield


def _source_gate():
    """Return an async context manager capping concurrent source queries."""
    global _source_sem, _source_sem_limit, _source_sem_loop
    limit = config.SOURCE_MAX_CONCURRENCY
    if limit <= 0:
        return _null_gate()
    loop = asyncio.get_event_loop()
    if _source_sem is None or _source_sem_limit != limit or _source_sem_loop is not loop:
        _source_sem = asyncio.Semaphore(limit)
        _source_sem_limit = limit
        _source_sem_loop = loop
    return _source_sem


async def execute_split_query(
    sql: str,
    params: dict[str, Any],
    split_index: int,
    *,
    max_retries: int | None = None,
) -> list[dict[str, Any]]:
    """
    Execute parameterised SQL and return rows as list[dict].

    Retries up to ``max_retries`` times (default: ``config.DB_MAX_RETRIES``) with
    linear backoff on transient errors. Raises :class:`SourceUnavailable` once
    retries are exhausted so the caller can return a 503.
    """
    if max_retries is None:
        max_retries = config.DB_MAX_RETRIES
    backoff = config.DB_RETRY_BACKOFF_SECONDS

    last_exc: Exception | None = None
    for attempt in range(max_retries + 1):
        t0 = time.perf_counter()
        try:
            rows = await _execute_once(sql, params, split_index)
            metrics.record_sql(time.perf_counter() - t0)
            return rows
        except asyncio.TimeoutError as exc:
            metrics.record_sql(time.perf_counter() - t0, error=True)
            log.warning(
                "query_timeout",
                split_index=split_index,
                attempt=attempt,
                timeout=config.QUERY_TIMEOUT_SECONDS,
            )
            last_exc = exc
        except Exception as exc:
            metrics.record_sql(time.perf_counter() - t0, error=True)
            log.error(
                "query_error",
                split_index=split_index,
                attempt=attempt,
                error=str(exc),
            )
            last_exc = exc
        if attempt < max_retries:
            await asyncio.sleep(backoff * (attempt + 1))

    raise SourceUnavailable(
        f"SQL query failed after {max_retries + 1} attempt(s)"
    ) from last_exc


async def _execute_once(
    sql: str,
    params: dict[str, Any],
    split_index: int,
) -> list[dict[str, Any]]:
    engine = get_engine()
    async with _source_gate():
        async with asyncio.timeout(config.QUERY_TIMEOUT_SECONDS):
            async with engine.connect() as conn:
                log.info(
                    "sql_execute",
                    split_index=split_index,
                    sql=sql,
                    params=params,
                )
                result = await conn.execute(text(sql), params)
                columns = list(result.keys())
                rows = [dict(zip(columns, row)) for row in result.fetchall()]
                log.info(
                    "sql_complete",
                    split_index=split_index,
                    rows_returned=len(rows),
                )
                return rows


async def stream_split_query(
    sql: str,
    params: dict[str, Any],
    split_index: int,
    *,
    batch_rows: int,
):
    """Stream a split query's rows in batches (Phase 4 streaming materialization).

    Yields ``list[dict]`` partitions of up to ``batch_rows`` rows using a
    server-side/streaming result, so peak memory is ~one batch rather than the
    whole split. Single-attempt (no mid-stream retry — a failure mid-stream
    raises :class:`SourceUnavailable`); the non-streaming
    :func:`execute_split_query` keeps the retrying path.
    """
    engine = get_engine()
    try:
        async with _source_gate():
            async with asyncio.timeout(config.QUERY_TIMEOUT_SECONDS):
                async with engine.connect() as conn:
                    log.info("sql_stream", split_index=split_index, sql=sql,
                             params=params, batch_rows=batch_rows)
                    result = await conn.stream(text(sql), params)
                    columns = list(result.keys())
                    total = 0
                    async for partition in result.partitions(batch_rows):
                        total += len(partition)
                        yield [dict(zip(columns, row)) for row in partition]
                    log.info("sql_stream_complete", split_index=split_index, rows_returned=total)
    except (SourceUnavailable, asyncio.CancelledError):
        raise
    except Exception as exc:  # noqa: BLE001
        log.error("query_stream_error", split_index=split_index, error=str(exc))
        raise SourceUnavailable(f"streamed SQL query failed: {exc}") from exc


async def ping(timeout_seconds: float = 3.0) -> bool:
    """Lightweight source-DB liveness check for readiness probes.

    Runs ``SELECT 1`` with a short timeout. Returns True on success, False on
    any error/timeout (never raises), so callers can map it to a 503.
    """
    try:
        engine = get_engine()
        async with asyncio.timeout(timeout_seconds):
            async with engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
        return True
    except Exception as exc:  # noqa: BLE001 - readiness must not raise
        log.warning("db_ping_failed", error=str(exc))
        return False


async def fetch_key_bounds(source_table: str, key_column: str) -> tuple[int, int] | None:
    """Return ``(min, max)`` of the integer key column for range planning (Phase 4).

    Runs a single ``SELECT MIN(pk), MAX(pk)`` (an index-only scan on most
    engines). Returns ``None`` when the table is empty or the key isn't an
    integer, so the caller can fall back to modulo planning. Never raises for
    an empty/odd table — only genuine SQL errors propagate.
    """
    from planner.dialects import get_dialect

    d = get_dialect(config.DB_URL)
    src = d.quote_qualified(source_table)
    pk = d.quote(key_column)
    sql = f"SELECT MIN({pk}) AS lo, MAX({pk}) AS hi FROM {src}"
    engine = get_engine()
    async with asyncio.timeout(config.QUERY_TIMEOUT_SECONDS):
        async with engine.connect() as conn:
            row = (await conn.execute(text(sql))).first()
    if row is None or row[0] is None or row[1] is None:
        return None
    try:
        return int(row[0]), int(row[1])
    except (TypeError, ValueError):
        # Non-integer key (uuid/string/etc.) — range planning needs integers.
        return None


async def introspect_columns(table: str) -> list[str]:
    """Return the source table's column names, or [] if it can't be inspected."""
    engine = get_engine()
    schema, name = _split_qualified(table)
    async with engine.connect() as conn:
        cols = await conn.run_sync(
            lambda sync_conn: inspect(sync_conn).get_columns(name, schema=schema)
        )
    return [c["name"] for c in cols]


async def validate_source_schema(table=None) -> None:
    """Fail fast (H6) if the source table is missing any declared column.

    Compares the declared columns (advertised to Fabric) against the actual
    source table. A missing column would otherwise surface later as a confusing
    runtime error or silent null column, so raise a clear error here. Extra
    source columns are fine (they're simply not projected) and only logged.

    With no argument, validates the global config table (backward compatible).
    Pass a :class:`config.TableDef` to validate a specific table (F1 multi-table).
    """
    if table is None:
        source_table = config.DB_SOURCE_TABLE
        declared = [c.name for c in config.TABLE_SCHEMA]
    else:
        source_table = table.source_table
        declared = [c.name for c in table.schema]

    try:
        actual = await introspect_columns(source_table)
    except Exception as exc:  # noqa: BLE001
        log.warning("source_schema_introspection_failed",
                    table=source_table, error=str(exc))
        return

    if not actual:
        log.warning("source_schema_empty", table=source_table)
        return

    actual_set = set(actual)
    missing = [c for c in declared if c not in actual_set]
    extra = [c for c in actual if c not in set(declared)]

    if extra:
        log.info("source_schema_extra_columns",
                 table=source_table, columns=extra)

    if missing:
        raise RuntimeError(
            f"Source table {source_table!r} is missing declared "
            f"column(s): {missing}. Update the table schema or the source "
            f"table so they match. Source columns present: {actual}"
        )

    log.info("source_schema_ok", table=source_table, columns=declared)


# ---------------------------------------------------------------------------
# Source-metadata reflection → auto-derived Iceberg schema (usability)
# ---------------------------------------------------------------------------
# Instead of hand-declaring every column, a table/view can be described by just
# its name + a key column; the column schema is reflected from the source
# database and mapped to Iceberg types automatically.

_INTEGER_ICEBERG_TYPES = ("int", "long")


def _split_qualified(name: str) -> tuple[str | None, str]:
    """Split ``schema.table`` into ``(schema, table)``; unqualified → ``(None, name)``."""
    if "." in name:
        schema, _, table = name.rpartition(".")
        return schema or None, table
    return None, name


def sqlalchemy_type_to_iceberg(sa_type) -> str:
    """Map a reflected SQLAlchemy column type to an Iceberg primitive type string."""
    name = type(sa_type).__name__.upper()
    if "UUID" in name or "UNIQUEIDENTIFIER" in name:
        # GUIDs are surfaced as text (pyodbc returns them as strings); this
        # avoids the fixed(16) binary path and "just works" for reflection.
        return "string"
    if "MONEY" in name:
        return "decimal(19,4)"   # SQL Server money / smallmoney
    if name == "BIT":
        return "boolean"

    if isinstance(sa_type, satypes.Boolean):
        return "boolean"
    # Integers: check Big/Small before the Integer base class.
    if isinstance(sa_type, satypes.BigInteger):
        return "long"
    if isinstance(sa_type, satypes.SmallInteger):
        return "int"
    if isinstance(sa_type, satypes.Integer):
        return "int"
    # Float subclasses Numeric, so check it first.
    if isinstance(sa_type, satypes.Float):
        return "double"
    if isinstance(sa_type, satypes.Numeric):
        precision = getattr(sa_type, "precision", None) or 38
        scale = getattr(sa_type, "scale", None)
        return f"decimal({precision},{0 if scale is None else scale})"
    if isinstance(sa_type, satypes.DateTime):
        if getattr(sa_type, "timezone", False):
            return "timestamptz"
        # Naive DATETIME/DATETIME2: Fabric's SQL analytics endpoint rejects
        # TIMESTAMP_NTZ (Iceberg `timestamp`), so default to `timestamptz` (UTC).
        return "timestamptz" if config.TIMESTAMP_ASSUME_UTC else "timestamp"
    if isinstance(sa_type, satypes.Date):
        return "date"
    if isinstance(sa_type, satypes.Time):
        return "time"
    if isinstance(sa_type, satypes.LargeBinary) or "BINARY" in name:
        return "binary"
    if isinstance(sa_type, (satypes.String, satypes.Text, satypes.Enum)):
        return "string"

    # Fall back to the Python type SQLAlchemy would produce.
    try:
        import datetime as _dt
        import decimal as _dec
        py = sa_type.python_type
        if py is bool:
            return "boolean"
        if py is int:
            return "long"
        if py is float:
            return "double"
        if py is _dec.Decimal:
            return "decimal(38,10)"
        if py is _dt.datetime:
            return "timestamptz" if config.TIMESTAMP_ASSUME_UTC else "timestamp"
        if py is _dt.date:
            return "date"
        if py is bytes:
            return "binary"
    except (NotImplementedError, AttributeError):
        pass
    return "string"


async def reflect_columns(source_table: str) -> list[dict]:
    """Return the source table/view's reflected columns (name, type, nullable)."""
    engine = get_engine()
    schema, name = _split_qualified(source_table)
    async with engine.connect() as conn:
        return await conn.run_sync(
            lambda sync_conn: inspect(sync_conn).get_columns(name, schema=schema)
        )


async def reflect_primary_key(source_table: str) -> list[str]:
    """Return the source table's primary-key column names, or [] if none/unknown."""
    engine = get_engine()
    schema, name = _split_qualified(source_table)
    try:
        async with engine.connect() as conn:
            pk = await conn.run_sync(
                lambda sync_conn: inspect(sync_conn).get_pk_constraint(name, schema=schema)
            )
        return list(pk.get("constrained_columns") or [])
    except Exception as exc:  # noqa: BLE001 - PK reflection is best-effort (views, perms)
        log.warning("pk_reflection_failed", table=source_table, error=str(exc))
        return []


async def derive_table_schema(source_table: str) -> list["config.ColumnDef"]:
    """Reflect a source table/view and build an Iceberg schema automatically."""
    cols = await reflect_columns(source_table)
    if not cols:
        raise RuntimeError(
            f"Could not reflect any columns for source {source_table!r}. Check the "
            f"name and permissions, or provide an explicit schema in config.TABLES."
        )
    return [
        config.ColumnDef(
            field_id=i,
            name=col["name"],
            iceberg_type=sqlalchemy_type_to_iceberg(col["type"]),
            nullable=bool(col.get("nullable", True)),
        )
        for i, col in enumerate(cols, start=1)
    ]


def _resolve_key_column(table) -> None:
    """Validate/populate ``table.key_column`` (must be an integer split key)."""
    if not table.key_column:
        for col in table.schema:
            if not col.nullable and col.iceberg_type in _INTEGER_ICEBERG_TYPES:
                table.key_column = col.name
                break
    if not table.key_column:
        raise RuntimeError(
            f"Table {table.name!r}: no integer split key found. Set key_column "
            f"(KEY_COLUMN) to an integer column in {table.source_table!r}."
        )
    col = next((c for c in table.schema if c.name == table.key_column), None)
    if col is None:
        raise RuntimeError(
            f"Table {table.name!r}: key_column {table.key_column!r} is not a column "
            f"of {table.source_table!r} (columns: {[c.name for c in table.schema]})."
        )
    if col.iceberg_type not in _INTEGER_ICEBERG_TYPES:
        raise RuntimeError(
            f"Table {table.name!r}: key_column {table.key_column!r} has type "
            f"{col.iceberg_type!r}; the split key must be an integer (int/long)."
        )


async def resolve_tables(tables) -> None:
    """Fill in each table's schema (from source metadata) and key column in place.

    Tables that already declare an explicit ``schema`` are left as-is (only the
    key column is resolved). Called once at startup before snapshots are built.
    """
    for table in tables:
        if table.schema is None:
            table.schema = await derive_table_schema(table.source_table)
            log.info(
                "schema_derived",
                table=table.name,
                source=table.source_table,
                columns=[(c.name, c.iceberg_type) for c in table.schema],
            )
        if not table.key_column:
            pk = await reflect_primary_key(table.source_table)
            if len(pk) == 1:
                table.key_column = pk[0]
        _resolve_key_column(table)
        log.info("table_resolved", table=table.name,
                 key_column=table.key_column, num_splits=table.num_splits)

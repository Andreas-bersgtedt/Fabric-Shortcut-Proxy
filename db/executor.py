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

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy import types as satypes
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

import config
from observability.logging import get_logger
from observability import metrics

log = get_logger(__name__)

_engine: AsyncEngine | None = None
_sync_engine: Engine | None = None


class SourceUnavailable(RuntimeError):
    """Raised when the source database cannot satisfy a query after retries.

    The S3 router maps this to a ``503 ServiceUnavailable`` response so clients
    can back off and retry, instead of a bare 500.
    """


def _params_for_log(params: dict[str, Any]) -> dict[str, Any]:
    """Return query parameters with tokenization secrets removed."""
    return {
        key: "[REDACTED]" if key.startswith(("fsp_token_", "__token_")) else value
        for key, value in params.items()
    }


def _db_url_uses_async_driver(db_url: str) -> bool:
    scheme = (db_url or "").lower().split("://", 1)[0]
    return (
        "+asyncpg" in scheme
        or "+aioodbc" in scheme
        or "+aiosqlite" in scheme
    )


def _make_async_engine(
    db_url: str, timeout_seconds: int | None = None
) -> AsyncEngine:
    kwargs: dict = {"echo": False, "hide_parameters": True}
    # SQLite (including aiosqlite) uses StaticPool and does not accept
    # pool_size / max_overflow / pool_timeout.
    if "sqlite" not in db_url:
        kwargs.update({"pool_size": 5, "max_overflow": 10, "pool_timeout": 30})
    if db_url.lower().startswith("mssql+") and timeout_seconds is not None:
        kwargs["connect_args"] = {"timeout": max(1, int(timeout_seconds))}
    return create_async_engine(db_url, **kwargs)


def _make_sync_engine(db_url: str, timeout_seconds: int | None = None) -> Engine:
    kwargs: dict = {
        "echo": False,
        "hide_parameters": True,
        "pool_pre_ping": True,
    }
    if "sqlite" not in db_url:
        kwargs.update({"pool_size": 5, "max_overflow": 10, "pool_timeout": 30})
    if db_url.lower().startswith("mssql+") and timeout_seconds is not None:
        kwargs["connect_args"] = {"timeout": max(1, int(timeout_seconds))}
    return create_engine(db_url, **kwargs)


def get_engine() -> AsyncEngine:
    """Async engine for the DEFAULT connection (``config.DB_URL``)."""
    if not _db_url_uses_async_driver(config.DB_URL):
        raise RuntimeError(
            "DB_URL uses a sync driver. Use executor query APIs, which apply a "
            "sync-threadpool fallback for this flavor."
        )

    global _engine
    if _engine is None:
        _engine = _make_async_engine(config.DB_URL, config.QUERY_TIMEOUT_SECONDS)
    return _engine


def get_sync_engine() -> Engine:
    """Sync engine for the DEFAULT connection (``config.DB_URL``)."""
    global _sync_engine
    if _sync_engine is None:
        _sync_engine = _make_sync_engine(config.DB_URL, config.QUERY_TIMEOUT_SECONDS)
    return _sync_engine


def _async_mode() -> bool:
    return _db_url_uses_async_driver(config.DB_URL)


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
    """Return an async context manager capping concurrent DEFAULT-source queries."""
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


# --- Named connections (multi-source) ---------------------------------------
# The DEFAULT connection keeps using the module-level engine globals above (so
# tests that reset them and monkeypatch ``config.DB_URL`` keep working). Every
# other connection id gets its own lazily-created engines + backpressure gate.

class ConnectionHandle:
    """Lazily-created engines + backpressure gate for a NAMED source connection."""

    def __init__(self, connection_id: str, db_url: str, timeout_seconds: int):
        self.connection_id = connection_id
        self.db_url = db_url
        self.timeout_seconds = timeout_seconds
        self._engine: AsyncEngine | None = None
        self._sync_engine: Engine | None = None
        self._sem: asyncio.Semaphore | None = None
        self._sem_limit: int = -1
        self._sem_loop: asyncio.AbstractEventLoop | None = None

    def async_mode(self) -> bool:
        return _db_url_uses_async_driver(self.db_url)

    def get_engine(self) -> AsyncEngine:
        if not self.async_mode():
            raise RuntimeError(
                f"Connection {self.connection_id!r} uses a sync driver. Use executor "
                f"query APIs, which apply a sync-threadpool fallback for this flavor."
            )
        if self._engine is None:
            self._engine = _make_async_engine(self.db_url, self.timeout_seconds)
        return self._engine

    def get_sync_engine(self) -> Engine:
        if self._sync_engine is None:
            self._sync_engine = _make_sync_engine(self.db_url, self.timeout_seconds)
        return self._sync_engine

    def gate(self):
        limit = config.SOURCE_MAX_CONCURRENCY
        if limit <= 0:
            return _null_gate()
        loop = asyncio.get_event_loop()
        if self._sem is None or self._sem_limit != limit or self._sem_loop is not loop:
            self._sem = asyncio.Semaphore(limit)
            self._sem_limit = limit
            self._sem_loop = loop
        return self._sem

    async def dispose(self) -> None:
        if self._engine is not None:
            await self._engine.dispose()
            self._engine = None
        if self._sync_engine is not None:
            eng = self._sync_engine
            self._sync_engine = None
            await asyncio.to_thread(eng.dispose)


_named_handles: dict[str, ConnectionHandle] = {}


def _get_named_handle(connection_id: str) -> ConnectionHandle:
    h = _named_handles.get(connection_id)
    if h is None:
        conn = config.get_connection(connection_id)
        if conn is None:
            raise RuntimeError(f"Unknown connection id {connection_id!r}.")
        h = ConnectionHandle(
            connection_id, conn.db_url, conn.query_timeout_seconds
        )
        _named_handles[connection_id] = h
    return h


# --- Per-connection dispatch (default -> module globals; else -> handle) -----

def _db_url_for(connection: str) -> str:
    return config.DB_URL if connection == "default" else _get_named_handle(connection).db_url


def _engine_for(connection: str) -> AsyncEngine:
    return get_engine() if connection == "default" else _get_named_handle(connection).get_engine()


def _sync_engine_for(connection: str) -> Engine:
    return get_sync_engine() if connection == "default" else _get_named_handle(connection).get_sync_engine()


def _async_mode_for(connection: str) -> bool:
    return _async_mode() if connection == "default" else _get_named_handle(connection).async_mode()


def _gate_for(connection: str):
    return _source_gate() if connection == "default" else _get_named_handle(connection).gate()


def _dialect_for(connection: str):
    from planner.dialects import get_dialect
    return get_dialect(_db_url_for(connection))


def _query_timeout_for(connection: str) -> int:
    if connection == "default":
        return config.QUERY_TIMEOUT_SECONDS
    conn = config.get_connection(connection)
    return conn.query_timeout_seconds if conn else config.QUERY_TIMEOUT_SECONDS


def _max_retries_for(connection: str) -> int:
    if connection == "default":
        return config.DB_MAX_RETRIES
    conn = config.get_connection(connection)
    return conn.db_max_retries if conn else config.DB_MAX_RETRIES


def _retry_backoff_for(connection: str) -> float:
    if connection == "default":
        return config.DB_RETRY_BACKOFF_SECONDS
    conn = config.get_connection(connection)
    return conn.db_retry_backoff_seconds if conn else config.DB_RETRY_BACKOFF_SECONDS


async def dispose_engines() -> None:
    global _engine, _sync_engine
    if _engine is not None:
        await _engine.dispose()
        _engine = None
    if _sync_engine is not None:
        eng = _sync_engine
        _sync_engine = None
        await asyncio.to_thread(eng.dispose)
    handles = list(_named_handles.values())
    _named_handles.clear()
    for h in handles:
        await h.dispose()



async def execute_split_query(
    sql: str,
    params: dict[str, Any],
    split_index: int,
    *,
    max_retries: int | None = None,
    connection: str = "default",
) -> list[dict[str, Any]]:
    """
    Execute parameterised SQL and return rows as list[dict].

    Retries up to ``max_retries`` times (default: the connection's
    ``db_max_retries``) with linear backoff on transient errors. Raises
    :class:`SourceUnavailable` once retries are exhausted so the caller can
    return a 503. ``connection`` selects the source (default: ``"default"``).
    """
    if max_retries is None:
        max_retries = _max_retries_for(connection)
    backoff = _retry_backoff_for(connection)

    last_exc: Exception | None = None
    for attempt in range(max_retries + 1):
        t0 = time.perf_counter()
        try:
            rows = await _execute_once(sql, params, split_index, connection)
            metrics.record_sql(time.perf_counter() - t0)
            return rows
        except asyncio.TimeoutError as exc:
            metrics.record_sql(time.perf_counter() - t0, error=True)
            log.warning(
                "query_timeout",
                split_index=split_index,
                attempt=attempt,
                timeout=_query_timeout_for(connection),
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

    detail = (
        f": {type(last_exc).__name__}: {last_exc}"
        if last_exc is not None else ""
    )
    raise SourceUnavailable(
        f"SQL query failed after {max_retries + 1} attempt(s){detail}"
    ) from last_exc


async def _execute_once(
    sql: str,
    params: dict[str, Any],
    split_index: int,
    connection: str = "default",
) -> list[dict[str, Any]]:
    async with _gate_for(connection):
        async with asyncio.timeout(_query_timeout_for(connection)):
            log.info(
                "sql_execute",
                split_index=split_index,
                sql=sql,
                params=_params_for_log(params),
                connection=connection,
                mode=("async" if _async_mode_for(connection) else "sync-fallback"),
            )
            if _async_mode_for(connection):
                engine = _engine_for(connection)
                async with engine.connect() as conn:
                    result = await conn.execute(text(sql), params)
                    columns = list(result.keys())
                    rows = [dict(zip(columns, row)) for row in result.fetchall()]
            else:
                def _work() -> list[dict[str, Any]]:
                    with _sync_engine_for(connection).connect() as conn:
                        result = conn.execute(text(sql), params)
                        columns = list(result.keys())
                        return [dict(zip(columns, row)) for row in result.fetchall()]

                rows = await asyncio.to_thread(_work)

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
    connection: str = "default",
):
    """Stream a split query's rows in batches (Phase 4 streaming materialization).

    Yields ``list[dict]`` partitions of up to ``batch_rows`` rows using a
    server-side/streaming result, so peak memory is ~one batch rather than the
    whole split. Single-attempt (no mid-stream retry — a failure mid-stream
    raises :class:`SourceUnavailable`); the non-streaming
    :func:`execute_split_query` keeps the retrying path.
    """
    try:
        async with _gate_for(connection):
            async with asyncio.timeout(_query_timeout_for(connection)):
                log.info("sql_stream", split_index=split_index, sql=sql,
                         params=_params_for_log(params), batch_rows=batch_rows,
                         connection=connection,
                         mode=("async" if _async_mode_for(connection) else "sync-fallback"))

                if _async_mode_for(connection):
                    engine = _engine_for(connection)
                    async with engine.connect() as conn:
                        result = await conn.stream(text(sql), params)
                        columns = list(result.keys())
                        total = 0
                        async for partition in result.partitions(batch_rows):
                            total += len(partition)
                            yield [dict(zip(columns, row)) for row in partition]
                        log.info("sql_stream_complete", split_index=split_index, rows_returned=total)
                else:
                    rows = await _execute_once(sql, params, split_index, connection)
                    total = len(rows)
                    for i in range(0, total, batch_rows):
                        yield rows[i:i + batch_rows]
                    log.info("sql_stream_complete", split_index=split_index, rows_returned=total)
    except (SourceUnavailable, asyncio.CancelledError):
        raise
    except Exception as exc:  # noqa: BLE001
        log.error("query_stream_error", split_index=split_index, error=str(exc))
        raise SourceUnavailable(f"streamed SQL query failed: {exc}") from exc


async def ping(timeout_seconds: float = 3.0, connection: str = "default") -> bool:
    """Lightweight source-DB liveness check for readiness probes.

    Runs ``SELECT 1`` with a short timeout. Returns True on success, False on
    any error/timeout (never raises), so callers can map it to a 503.
    """
    try:
        async with asyncio.timeout(timeout_seconds):
            if _async_mode_for(connection):
                engine = _engine_for(connection)
                async with engine.connect() as conn:
                    await conn.execute(text("SELECT 1"))
            else:
                def _sync_ping() -> None:
                    with _sync_engine_for(connection).connect() as conn:
                        conn.execute(text("SELECT 1"))
                await asyncio.to_thread(_sync_ping)
        return True
    except Exception as exc:  # noqa: BLE001 - readiness must not raise
        log.warning("db_ping_failed", error=str(exc), connection=connection)
        return False


async def execute_scalar(sql: str, params: dict[str, Any] | None = None, connection: str = "default"):
    """Execute a scalar SQL statement through the active engine mode.

    Uses async-native execution when available, else a sync-threadpool fallback.
    """
    params = params or {}
    async with _gate_for(connection):
        async with asyncio.timeout(_query_timeout_for(connection)):
            if _async_mode_for(connection):
                engine = _engine_for(connection)
                async with engine.connect() as conn:
                    return (await conn.execute(text(sql), params)).scalar()

            def _sync_scalar():
                with _sync_engine_for(connection).connect() as conn:
                    return conn.execute(text(sql), params).scalar()

            return await asyncio.to_thread(_sync_scalar)


async def fetch_column_bounds(source_table: str, key_column: str, connection: str = "default"):
    """Return raw ``(min, max)`` bounds for any comparable key column.

    Returns None for empty tables or all-NULL keys.
    """
    d = _dialect_for(connection)
    src = d.quote_qualified(source_table)
    pk = d.quote(key_column)
    sql = f"SELECT MIN({pk}) AS lo, MAX({pk}) AS hi FROM {src}"
    async with asyncio.timeout(_query_timeout_for(connection)):
        if _async_mode_for(connection):
            engine = _engine_for(connection)
            async with engine.connect() as conn:
                row = (await conn.execute(text(sql))).first()
        else:
            def _sync_bounds():
                with _sync_engine_for(connection).connect() as conn:
                    return conn.execute(text(sql)).first()

            row = await asyncio.to_thread(_sync_bounds)
    if row is None or row[0] is None or row[1] is None:
        return None
    return row[0], row[1]


async def fetch_table_row_count(source_table: str, connection: str = "default") -> int | None:
    """Return ``COUNT(*)`` for a source table/view, or None when unavailable."""
    d = _dialect_for(connection)
    src = d.quote_qualified(source_table)
    sql = f"SELECT COUNT(*) AS n FROM {src}"
    async with asyncio.timeout(_query_timeout_for(connection)):
        if _async_mode_for(connection):
            engine = _engine_for(connection)
            async with engine.connect() as conn:
                row = (await conn.execute(text(sql))).first()
        else:
            def _sync_count():
                with _sync_engine_for(connection).connect() as conn:
                    return conn.execute(text(sql)).first()

            row = await asyncio.to_thread(_sync_count)

    if row is None or row[0] is None:
        return None
    try:
        return int(row[0])
    except (TypeError, ValueError):
        return None


async def fetch_key_bounds(source_table: str, key_column: str, connection: str = "default") -> tuple[int, int] | None:
    """Return ``(min, max)`` of the integer key column for range planning (Phase 4).

    Runs a single ``SELECT MIN(pk), MAX(pk)`` (an index-only scan on most
    engines). Returns ``None`` when the table is empty or the key isn't an
    integer, so the caller can fall back to modulo planning. Never raises for
    an empty/odd table — only genuine SQL errors propagate.
    """
    bounds = await fetch_column_bounds(source_table, key_column, connection)
    if bounds is None:
        return None
    lo_raw, hi_raw = bounds
    try:
        return int(lo_raw), int(hi_raw)
    except (TypeError, ValueError):
        # Non-integer key (uuid/string/etc.) — range planning needs integers.
        return None


async def fetch_key_quantile_bounds(source_table: str, key_column: str, n: int, connection: str = "default",
                                    *, sample_rows: int = 0, key_is_integer: bool = False):
    """Return ``(bucket_min_values, overall_max)`` for equal-count split planning.

    Uses ``NTILE(n) OVER (ORDER BY key)`` to bucket rows into ``n`` equal-count
    groups, then returns each bucket's minimum key plus the overall maximum. The
    planner turns these into contiguous half-open ranges so each split holds
    roughly equal rows regardless of key skew. Returns ``None`` for empty tables,
    all-NULL keys, or ``n < 1``. Costs a single ordered scan of the key column
    (index-ordered where an index exists); best invoked once at snapshot build.

    When ``sample_rows > 0`` and the integer key column is larger than that, a
    deterministic modulo-stride sample bounds the rows fed into ``NTILE`` (caps
    the sort/window/tempdb cost on large tables). Boundaries become approximate
    but stay far more balanced than equal-span; the overall max is still taken
    from the full table so the last range always covers it.
    """
    if n < 1:
        return None
    d = _dialect_for(connection)
    src = d.quote_qualified(source_table)
    pk = d.quote(key_column)
    n_int = int(n)  # inlined as a validated integer (NTILE's arg is not a bind on all drivers)

    ntile_src = src
    if sample_rows and sample_rows > 0 and key_is_integer:
        total = await fetch_table_row_count(source_table, connection)
        if total and total > sample_rows:
            stride = (total + sample_rows - 1) // sample_rows  # ceil so the sample <= sample_rows
            if stride >= 2:
                # Value-uniform stride sample keeps the planning scan's sort/window bounded.
                ntile_src = (f"(SELECT {pk} FROM {src} "
                             f"WHERE {pk} IS NOT NULL AND (ABS({pk}) % {int(stride)}) = 0) __s")

    sql = (
        f"SELECT MIN({pk}) AS lo, MAX({pk}) AS hi "
        f"FROM (SELECT {pk}, NTILE({n_int}) OVER (ORDER BY {pk}) AS __b "
        f"FROM {ntile_src} WHERE {pk} IS NOT NULL) q "
        f"GROUP BY __b ORDER BY __b"
    )
    async with asyncio.timeout(_query_timeout_for(connection)):
        if _async_mode_for(connection):
            engine = _engine_for(connection)
            async with engine.connect() as conn:
                rows = (await conn.execute(text(sql))).all()
        else:
            def _sync_quantiles():
                with _sync_engine_for(connection).connect() as conn:
                    return conn.execute(text(sql)).all()

            rows = await asyncio.to_thread(_sync_quantiles)
    if not rows:
        return None
    mins = [r[0] for r in rows]
    overall_max = rows[-1][1]
    if sample_rows and sample_rows > 0 and key_is_integer and ntile_src is not src:
        # A sample can miss the true min/max, so anchor the outer ranges to the
        # full-table bounds — the first range must start at the real minimum and
        # the last must cover the real maximum, else edge rows go unserved.
        full = await fetch_column_bounds(source_table, key_column, connection)
        if full is not None:
            true_min, true_max = full
            if true_min is not None and mins:
                mins[0] = true_min
            if true_max is not None:
                overall_max = true_max
    if overall_max is None or any(m is None for m in mins):
        return None
    return mins, overall_max


async def _run_select_all(sql: str, params: dict, connection: str):
    """Run a read-only SELECT and return all rows (async-native or sync fallback)."""
    async with asyncio.timeout(_query_timeout_for(connection)):
        if _async_mode_for(connection):
            engine = _engine_for(connection)
            async with engine.connect() as conn:
                return (await conn.execute(text(sql), params)).all()

        def _sync():
            with _sync_engine_for(connection).connect() as conn:
                return conn.execute(text(sql), params).all()

        return await asyncio.to_thread(_sync)


async def _mssql_histogram_steps(source_table: str, key_column: str, connection: str):
    """SQL Server histogram steps ``[(range_high_key, step_rows)]`` for an integer
    key, from the first statistics object leading with that column. [] if none."""
    sql = (
        "WITH pick AS ("
        " SELECT TOP 1 s.object_id AS oid, s.stats_id AS sid"
        " FROM sys.stats s"
        " JOIN sys.stats_columns sc ON sc.object_id=s.object_id AND sc.stats_id=s.stats_id AND sc.stats_column_id=1"
        " JOIN sys.columns c ON c.object_id=s.object_id AND c.column_id=sc.column_id"
        " WHERE s.object_id = OBJECT_ID(:tbl) AND c.name = :col"
        " ORDER BY s.stats_id)"
        " SELECT CAST(h.range_high_key AS BIGINT) AS hi, (h.range_rows + h.equal_rows) AS rows_"
        " FROM pick CROSS APPLY sys.dm_db_stats_histogram(pick.oid, pick.sid) h"
        " ORDER BY h.step_number"
    )
    rows = await _run_select_all(sql, {"tbl": source_table, "col": key_column}, connection)
    return [(int(r[0]), float(r[1] or 0)) for r in rows if r[0] is not None]


async def _pg_histogram_bounds(source_table: str, key_column: str, connection: str):
    """PostgreSQL equi-depth ``histogram_bounds`` (ascending ints) for a column,
    or [] when no stats exist. Parsed from the text form of the anyarray."""
    schema, table = _split_qualified(source_table)
    if schema:
        sql = ("SELECT histogram_bounds::text FROM pg_stats "
               "WHERE schemaname=:sch AND tablename=:tbl AND attname=:col")
        params = {"sch": schema, "tbl": table, "col": key_column}
    else:
        sql = ("SELECT histogram_bounds::text FROM pg_stats "
               "WHERE tablename=:tbl AND attname=:col ORDER BY schemaname LIMIT 1")
        params = {"tbl": table, "col": key_column}
    rows = await _run_select_all(sql, params, connection)
    if not rows or rows[0][0] is None:
        return []
    inner = str(rows[0][0]).strip().lstrip("{").rstrip("}")
    if not inner:
        return []
    return [int(float(x)) for x in inner.split(",") if x.strip()]


async def fetch_key_histogram_bounds(source_table: str, key_column: str, n: int, connection: str = "default"):
    """Return ``(bucket_min_values, overall_max)`` from the source optimizer's
    statistics histogram for an INTEGER key — a metadata read with **zero data
    scan** — or ``None``.

    Capability-gated to SQL Server (``sys.dm_db_stats_histogram``) and PostgreSQL
    (``pg_stats.histogram_bounds``). Best-effort: missing stats, an unsupported
    flavor, a non-integer histogram, or any error returns ``None`` so the caller
    falls back to NTILE planning.
    """
    if n < 1:
        return None
    from db.capabilities import flavor_from_db_url
    from planner.split_planner import mins_from_equidepth, mins_from_histogram_steps

    flavor = flavor_from_db_url(_db_url_for(connection))
    try:
        if flavor == "mssql":
            steps = await _mssql_histogram_steps(source_table, key_column, connection)
            mins = mins_from_histogram_steps(steps, n)
            if not mins:
                return None
            return [int(m) for m in mins], int(steps[-1][0])
        if flavor == "postgresql":
            bounds = await _pg_histogram_bounds(source_table, key_column, connection)
            mins = mins_from_equidepth(bounds, n)
            if not mins:
                return None
            return [int(m) for m in mins], int(bounds[-1])
    except Exception as exc:  # noqa: BLE001 - a stats read must never break planning
        log.warning("histogram_bounds_error", table=source_table, key=key_column,
                    flavor=flavor, error=str(exc))
        return None
    return None


async def introspect_columns(table: str, connection: str = "default") -> list[str]:
    """Return the source table's column names, or [] if it can't be inspected."""
    schema, name = _split_qualified(table)
    if _async_mode_for(connection):
        engine = _engine_for(connection)
        async with engine.connect() as conn:
            cols = await conn.run_sync(
                lambda sync_conn: inspect(sync_conn).get_columns(name, schema=schema)
            )
    else:
        cols = await asyncio.to_thread(
            lambda: inspect(_sync_engine_for(connection)).get_columns(name, schema=schema)
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
        mappings = [(c.name, c.name) for c in config.TABLE_SCHEMA]
        connection = "default"
    else:
        source_table = table.source_table
        declared = [c.source_name for c in table.schema]
        mappings = [(c.name, c.source_name) for c in table.schema]
        connection = table.connection_id

    try:
        actual = await introspect_columns(source_table, connection)
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
            f"column(s): {missing}. Output/source mappings: {mappings}. "
            f"Update the table schema or the source "
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


# Reflected type-name tokens that denote nested/composite source types with no
# scalar Iceberg representation (issue #9: Redshift SUPER, Teradata ARRAY/PERIOD,
# Impala ARRAY/MAP/STRUCT, Snowflake-style VARIANT).
_NESTED_SOURCE_TYPE_TOKENS = ("ARRAY", "MAP", "STRUCT", "SUPER", "VARIANT", "PERIOD")


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

    # Semi-structured text (JSON/XML) serializes losslessly to a string column;
    # keep that mapping explicit rather than falling through by accident.
    if "JSON" in name or "XML" in name:
        return "string"

    # Nested/composite source types (Redshift SUPER, Snowflake VARIANT, Teradata
    # ARRAY/PERIOD, Impala ARRAY/MAP/STRUCT) have no scalar Iceberg representation;
    # fail closed instead of silently stringifying a nested value.
    if any(token in name for token in _NESTED_SOURCE_TYPE_TOKENS):
        raise ValueError(
            f"Source column type {type(sa_type).__name__!r} is a nested or composite "
            "type that cannot be materialized as a scalar column. Exclude the column "
            "or declare it explicitly in config.TABLES."
        )

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


async def reflect_columns(source_table: str, connection: str = "default") -> list[dict]:
    """Return the source table/view's reflected columns (name, type, nullable)."""
    schema, name = _split_qualified(source_table)
    if _async_mode_for(connection):
        engine = _engine_for(connection)
        async with engine.connect() as conn:
            return await conn.run_sync(
                lambda sync_conn: inspect(sync_conn).get_columns(name, schema=schema)
            )
    return await asyncio.to_thread(
        lambda: inspect(_sync_engine_for(connection)).get_columns(name, schema=schema)
    )


async def reflect_primary_key(source_table: str, connection: str = "default") -> list[str]:
    """Return the source table's primary-key column names, or [] if none/unknown."""
    schema, name = _split_qualified(source_table)
    try:
        if _async_mode_for(connection):
            engine = _engine_for(connection)
            async with engine.connect() as conn:
                pk = await conn.run_sync(
                    lambda sync_conn: inspect(sync_conn).get_pk_constraint(name, schema=schema)
                )
        else:
            pk = await asyncio.to_thread(
                lambda: inspect(_sync_engine_for(connection)).get_pk_constraint(name, schema=schema)
            )
        return list(pk.get("constrained_columns") or [])
    except Exception as exc:  # noqa: BLE001 - PK reflection is best-effort (views, perms)
        log.warning("pk_reflection_failed", table=source_table, error=str(exc))
        return []


async def derive_table_schema(source_table: str, connection: str = "default") -> list["config.ColumnDef"]:
    """Reflect a source table/view and build an Iceberg schema automatically."""
    cols = await reflect_columns(source_table, connection)
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
            table.schema = await derive_table_schema(table.source_table, table.connection_id)
            log.info(
                "schema_derived",
                table=table.name,
                source=table.source_table,
                columns=[(c.name, c.iceberg_type) for c in table.schema],
            )
        if not table.key_column:
            pk = await reflect_primary_key(table.source_table, table.connection_id)
            if len(pk) == 1:
                table.key_column = pk[0]
        _resolve_key_column(table)
        log.info("table_resolved", table=table.name,
                 key_column=table.key_column, num_splits=table.num_splits)

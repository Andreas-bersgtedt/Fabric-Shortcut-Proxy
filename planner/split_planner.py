"""
SQL pushdown planner.

Maps a SplitDescriptor → a parameterized SQL query that returns only the rows
belonging to that split.

Split strategy: modulo on the primary key column (first non-nullable long/int).
  WHERE (pk_col % :num_splits) = :split_index

This gives a deterministic, stable partition for a fixed schema and split count.
Rows are ordered by pk_col to keep results deterministic across retries.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
import math

import config
from config import TableDef
from db.capabilities import capabilities_for_db_url
from iceberg.state_store import SplitDescriptor
from planner.dialects import get_dialect
from observability.logging import get_logger

log = get_logger(__name__)

_INTEGER_TYPES = {"int", "long"}
_TEMPORAL_TYPES = {"date", "timestamp", "timestamptz"}


def _pk_column(table: TableDef) -> str:
    """Return the split key column: the explicit key_column, else the first
    non-nullable integer column, else the first column."""
    if table.key_column:
        return table.key_column
    for col in table.schema:
        if not col.nullable and col.iceberg_type in ("int", "long"):
            return col.name
    # Fallback: first column
    return table.schema[0].name


def _column_type(table: TableDef, column_name: str) -> str | None:
    for col in table.schema:
        if col.name == column_name:
            return col.iceberg_type
    return None


def _first_column_of_type(table: TableDef, allowed: set[str]) -> str | None:
    for col in table.schema:
        if col.iceberg_type in allowed:
            return col.name
    return None


def _choose_strategy_key(table: TableDef, strategy: str) -> str:
    """Choose the split column for the requested strategy.

    Priority:
      1) explicit key_column
      2) integer key for range/auto
      3) temporal key for date/auto
      4) fallback to legacy pk-column selection
    """
    if table.key_column:
        return table.key_column
    if strategy in ("range", "auto"):
        col = _first_column_of_type(table, _INTEGER_TYPES)
        if col:
            return col
    if strategy in ("date", "auto"):
        col = _first_column_of_type(table, _TEMPORAL_TYPES)
        if col:
            return col
    return _pk_column(table)


def pk_column(table: TableDef) -> str:
    """Public accessor for the split key column (used by range planning)."""
    return _pk_column(table)


def compute_key_ranges(lo: int, hi: int, n: int) -> list[tuple[int, int]]:
    """Partition the inclusive integer key range ``[lo, hi]`` into ``n``
    contiguous half-open ``[start, end)`` ranges that fully cover it with no
    overlap. The final range's ``end`` is ``hi + 1`` so ``hi`` is included.

    Uses integer boundaries ``lo + span*i//n`` so ranges are near-equal in key
    span (equal-count NTILE planning for skewed keys is a future enhancement).
    When ``n`` exceeds the number of distinct keys some ranges are empty, which
    is correct (that split simply materializes zero rows).
    """
    n = max(1, n)
    if hi < lo:
        return [(lo, lo + 1)] + [(lo + 1, lo + 1)] * (n - 1)
    span = hi - lo + 1
    bounds = [lo + (span * i) // n for i in range(n + 1)]
    bounds[0], bounds[-1] = lo, hi + 1
    return [(bounds[i], bounds[i + 1]) for i in range(n)]


def _coerce_temporal(value, kind: str):
    if kind == "date":
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, str):
            return date.fromisoformat(value)
        raise TypeError(f"unsupported date bound type: {type(value)!r}")

    # timestamp / timestamptz
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    if isinstance(value, str):
        s = value.replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    raise TypeError(f"unsupported datetime bound type: {type(value)!r}")


def _to_tick(value, kind: str) -> int:
    if kind == "date":
        return value.toordinal()
    dt = value
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return int(dt.timestamp() * 1_000_000)


def _from_tick(tick: int, kind: str):
    if kind == "date":
        return date.fromordinal(tick)
    return datetime.fromtimestamp(tick / 1_000_000, tz=timezone.utc)


def compute_temporal_ranges(lo, hi, n: int, kind: str) -> list[tuple[object, object]]:
    lo_v = _coerce_temporal(lo, kind)
    hi_v = _coerce_temporal(hi, kind)
    lo_t = _to_tick(lo_v, kind)
    hi_t = _to_tick(hi_v, kind)
    int_ranges = compute_key_ranges(lo_t, hi_t, n)
    return [(_from_tick(a, kind), _from_tick(b, kind)) for a, b in int_ranges]


def compute_split_count(
    *,
    estimated_rows: int | None,
    target_rows: int,
    min_splits: int,
    max_splits: int,
    default_splits: int,
) -> int:
    """Return split count from row-target planning with guardrails."""
    if target_rows <= 0 or estimated_rows is None or estimated_rows <= 0:
        return max(min_splits, min(max_splits, default_splits))
    proposed = int(math.ceil(estimated_rows / target_rows))
    return max(min_splits, min(max_splits, proposed))


async def choose_table_num_splits(table: TableDef) -> int:
    """Choose a table split count using SPLIT_TARGET_ROWS guardrails.

    Returns the existing ``table.num_splits`` when dynamic planning is disabled
    or when row estimation fails.
    """
    if config.SPLIT_TARGET_ROWS <= 0:
        return table.num_splits

    from db.executor import fetch_table_row_count

    try:
        est = await fetch_table_row_count(table.source_table)
    except Exception as exc:  # noqa: BLE001 - startup should not fail here
        log.warning(
            "split_count_estimation_failed",
            table=table.name,
            source=table.source_table,
            error=str(exc),
        )
        return table.num_splits

    chosen = compute_split_count(
        estimated_rows=est,
        target_rows=config.SPLIT_TARGET_ROWS,
        min_splits=config.SPLIT_COUNT_MIN,
        max_splits=config.SPLIT_COUNT_MAX,
        default_splits=table.num_splits,
    )
    log.info(
        "split_count_planned",
        table=table.name,
        estimated_rows=est,
        target_rows=config.SPLIT_TARGET_ROWS,
        min_splits=config.SPLIT_COUNT_MIN,
        max_splits=config.SPLIT_COUNT_MAX,
        chosen_splits=chosen,
        configured_splits=table.num_splits,
    )
    return chosen


def build_split_query(split: SplitDescriptor) -> tuple[str, dict]:
    """
    Return (sql_text, params) for the given split.

    The SQL uses named bind parameters compatible with SQLAlchemy's
    text() construct: :param_name notation.

    Returns all projected columns with a stable ORDER BY on the PK column.
    Identifier quoting, integer CAST type and the row-limit clause are all
    delegated to the dialect adapter selected from ``config.DB_URL`` so the
    same code targets SQLite, PostgreSQL and SQL Server (T-SQL: TOP vs LIMIT,
    BIGINT vs INTEGER, bracket vs double-quote identifiers).

    When the split carries a range (``key_lo``/``key_hi``, set by range planning)
    it emits a ``pk >= lo AND pk < hi`` predicate that reads only this split's
    slice off the PK index; otherwise it falls back to the modulo predicate.
    """
    table: TableDef = split.table
    dialect = get_dialect(config.DB_URL)
    key_name = split.split_key_column or _pk_column(table)
    key_type = _column_type(table, key_name)
    pk = dialect.quote(key_name)
    projected = ", ".join(dialect.quote(col.name) for col in table.schema)
    source = dialect.quote_qualified(table.source_table)

    if split.key_lo is not None and split.key_hi is not None:
        sql = dialect.build_select_range(
            projected=projected,
            source=source,
            pk=pk,
            key_lo_param="key_lo",
            key_hi_param="key_hi",
            max_rows_param="max_rows",
        )
        params = {
            "key_lo": split.key_lo,
            "key_hi": split.key_hi,
            "max_rows": config.QUERY_MAX_ROWS,
        }
        return sql, params

    if key_type not in _INTEGER_TYPES:
        sql = dialect.build_select_row_number(
            projected=projected,
            source=source,
            order_by=pk,
            num_splits_param="num_splits",
            split_index_param="split_index",
            max_rows_param="max_rows",
        )
        params = {
            "num_splits": split.num_splits,
            "split_index": split.split_index,
            "max_rows": config.QUERY_MAX_ROWS,
        }
        return sql, params

    sql = dialect.build_select(
        projected=projected,
        source=source,
        pk=pk,
        num_splits_param="num_splits",
        split_index_param="split_index",
        max_rows_param="max_rows",
    )

    params = {
        "num_splits": split.num_splits,
        "split_index": split.split_index,
        "max_rows": config.QUERY_MAX_ROWS,
    }
    return sql, params


async def plan_ranges_for_snapshot(snap) -> bool:
    """Assign contiguous key ranges to a snapshot's splits (SPLIT_STRATEGY="range").

    Fetches the source key MIN/MAX once and slices ``[min, max]`` into
    ``len(splits)`` contiguous half-open ranges. Returns ``True`` when ranges
    were assigned, ``False`` when it fell back to modulo (empty table or a
    non-integer key). Idempotent and best-effort — never raises, so a planning
    hiccup degrades to the known-good modulo path rather than failing startup.
    """
    from db.executor import fetch_column_bounds, fetch_key_bounds

    table = snap.table
    strategy = config.SPLIT_STRATEGY
    if strategy == "modulo":
        return False

    key = _choose_strategy_key(table, strategy)
    key_type = _column_type(table, key)

    for split in snap.splits:
        split.split_key_column = key

    caps = capabilities_for_db_url(config.DB_URL)
    if not caps.supports_range_key_bounds:
        log.warning("range_planning_fallback_modulo", table=table.name,
                    key=key, reason="flavor_capability")
        return False

    if strategy in ("range", "auto") and key_type in _INTEGER_TYPES:
        try:
            bounds = await fetch_key_bounds(table.source_table, key)
        except Exception as exc:  # noqa: BLE001 - planning must not break startup
            log.warning("range_planning_error_fallback_modulo", table=table.name, key=key, error=str(exc))
            bounds = None
        if bounds is not None:
            lo, hi = bounds
            ranges = compute_key_ranges(lo, hi, len(snap.splits))
            for split, (rlo, rhi) in zip(snap.splits, ranges):
                split.key_lo, split.key_hi = rlo, rhi
            log.info("range_planning_ok", table=table.name, key=key,
                     key_min=lo, key_max=hi, splits=len(snap.splits), strategy="range")
            return True
        if strategy == "range":
            log.warning("range_planning_fallback_modulo", table=table.name,
                        key=key, reason="no_integer_bounds")
            return False

    if strategy in ("date", "auto") and key_type in _TEMPORAL_TYPES:
        try:
            bounds = await fetch_column_bounds(table.source_table, key)
        except Exception as exc:  # noqa: BLE001 - planning must not break startup
            log.warning("date_range_planning_error_fallback_modulo", table=table.name, key=key, error=str(exc))
            bounds = None
        if bounds is not None:
            lo, hi = bounds
            ranges = compute_temporal_ranges(lo, hi, len(snap.splits), key_type)
            for split, (rlo, rhi) in zip(snap.splits, ranges):
                split.key_lo, split.key_hi = rlo, rhi
            log.info("date_range_planning_ok", table=table.name, key=key,
                     key_min=str(lo), key_max=str(hi), splits=len(snap.splits), strategy="date")
            return True
        if strategy == "date":
            log.warning("date_range_planning_fallback_modulo", table=table.name,
                        key=key, reason="no_temporal_bounds")
            return False

    log.warning("split_strategy_fallback_modulo", table=table.name, key=key,
                strategy=strategy, key_type=key_type or "unknown")
    return False

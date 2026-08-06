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


def _range_upper(hi, key_type: str):
    """Exclusive upper bound one tick past the inclusive max, so ``key < upper`` keeps max."""
    if key_type in _INTEGER_TYPES:
        return int(hi) + 1
    kind = "date" if key_type == "date" else "timestamp"
    v = _coerce_temporal(hi, kind)
    return _from_tick(_to_tick(v, kind) + 1, kind)


def _coerce_key(value, key_type: str):
    if key_type in _INTEGER_TYPES:
        return int(value)
    kind = "date" if key_type == "date" else "timestamp"
    return _coerce_temporal(value, kind)


async def _balanced_ranges(table: TableDef, key: str, key_type: str | None, n: int):
    """Equal-count (NTILE quantile) ranges, or None to fall back to equal-span.

    Turns per-bucket key minimums into contiguous half-open ranges so each split
    holds ~equal rows. Best-effort: any failure returns None so the caller uses
    the known-good equal-span path.
    """
    if key_type is None or n < 1:
        return None
    from db.executor import fetch_key_quantile_bounds
    try:
        result = await fetch_key_quantile_bounds(table.source_table, key, n, connection=table.connection_id)
    except Exception as exc:  # noqa: BLE001 - planning must not break startup
        log.warning("balanced_planning_error_fallback_span", table=table.name, key=key, error=str(exc))
        return None
    if not result:
        return None
    raw_mins, raw_max = result
    try:
        mins = [_coerce_key(m, key_type) for m in raw_mins]
        upper = _range_upper(raw_max, key_type)
    except (TypeError, ValueError) as exc:
        log.warning("balanced_planning_coerce_failed_fallback_span", table=table.name, key=key, error=str(exc))
        return None
    if not mins:
        return None
    ranges: list[tuple] = []
    for i, lo in enumerate(mins):
        hi = mins[i + 1] if i + 1 < len(mins) else upper
        ranges.append((lo, hi))
    while len(ranges) < n:  # fewer buckets than splits -> empty tail splits (correct)
        ranges.append((upper, upper))
    return ranges[:n]


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
    target_rows = table.effective_split_target_rows
    if target_rows <= 0:
        return table.num_splits

    from db.executor import fetch_table_row_count

    try:
        est = await fetch_table_row_count(table.source_table, connection=table.connection_id)
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
        target_rows=target_rows,
        min_splits=config.SPLIT_COUNT_MIN,
        max_splits=config.SPLIT_COUNT_MAX,
        default_splits=table.num_splits,
    )
    log.info(
        "split_count_planned",
        table=table.name,
        estimated_rows=est,
        target_rows=target_rows,
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
    delegated to the dialect adapter selected from the split table's connection
    URL, so the same code targets SQLite, PostgreSQL and SQL Server (T-SQL: TOP
    vs LIMIT, BIGINT vs INTEGER, bracket vs double-quote identifiers) and lets
    different tables target different source dialects in one proxy.

    When the split carries a range (``key_lo``/``key_hi``, set by range planning)
    it emits a ``pk >= lo AND pk < hi`` predicate that reads only this split's
    slice off the PK index; otherwise it falls back to the modulo predicate.
    """
    table: TableDef = split.table
    dialect = get_dialect(config.effective_db_url(table.connection_id))
    key_name = split.split_key_column or _pk_column(table)
    key_type = _column_type(table, key_name)
    pk = dialect.quote(key_name)
    if any(
        col.transform and key_name in {col.name, col.source_name}
        for col in table.schema
    ):
        raise ValueError(f"Split key {key_name!r} cannot have a column transform")

    rendered = [
        dialect.render_projection(col, str(index))
        for index, col in enumerate(table.schema)
    ]
    projected = ", ".join(item[0] for item in rendered)
    outer_projected = ", ".join(item[1] for item in rendered)
    projection_params = {
        key: value
        for item in rendered
        for key, value in item[2].items()
    }
    source = dialect.quote_qualified(table.source_table)
    max_rows = table.effective_max_rows

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
            **projection_params,
            "key_lo": split.key_lo,
            "key_hi": split.key_hi,
            "max_rows": max_rows,
        }
        return sql, params

    if key_type not in _INTEGER_TYPES:
        sql = dialect.build_select_row_number(
            projected=projected,
            outer_projected=outer_projected,
            source=source,
            order_by=pk,
            num_splits_param="num_splits",
            split_index_param="split_index",
            max_rows_param="max_rows",
        )
        params = {
            **projection_params,
            "num_splits": split.num_splits,
            "split_index": split.split_index,
            "max_rows": max_rows,
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
        **projection_params,
        "num_splits": split.num_splits,
        "split_index": split.split_index,
        "max_rows": max_rows,
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
    strategy = table.effective_split_strategy
    # Dynamic split planning targets bounded rows/split; when enabled, treat the
    # legacy modulo default as range planning so each split reads only its slice.
    if strategy == "modulo" and table.effective_split_target_rows > 0:
        strategy = "range"

    if strategy == "modulo":
        return False

    key = _choose_strategy_key(table, strategy)
    key_type = _column_type(table, key)

    for split in snap.splits:
        split.split_key_column = key

    caps = capabilities_for_db_url(config.effective_db_url(table.connection_id))
    if not caps.supports_range_key_bounds:
        log.warning("range_planning_fallback_modulo", table=table.name,
                    key=key, reason="flavor_capability")
        return False

    # Equal-count planning (opt-in via split_balance="count"): size ranges by row
    # quantiles so skewed keys yield balanced splits. Falls through to equal-span
    # when unsupported or the quantile query yields nothing.
    if (table.effective_split_balance == "count" and caps.supports_ntile
            and key_type in (_INTEGER_TYPES | _TEMPORAL_TYPES)):
        qranges = await _balanced_ranges(table, key, key_type, len(snap.splits))
        if qranges is not None:
            for split, (rlo, rhi) in zip(snap.splits, qranges):
                split.key_lo, split.key_hi = rlo, rhi
            log.info("balanced_range_planning_ok", table=table.name, key=key,
                     splits=len(snap.splits), strategy=strategy, key_type=key_type)
            return True
        log.warning("balanced_planning_fallback_span", table=table.name, key=key,
                    reason="no_quantile_bounds")

    if strategy in ("range", "auto") and key_type in _INTEGER_TYPES:
        try:
            bounds = await fetch_key_bounds(table.source_table, key, connection=table.connection_id)
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
            bounds = await fetch_column_bounds(table.source_table, key, connection=table.connection_id)
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

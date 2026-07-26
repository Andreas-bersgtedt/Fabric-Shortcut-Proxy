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

import config
from config import TableDef
from db.capabilities import capabilities_for_db_url
from iceberg.state_store import SplitDescriptor
from planner.dialects import get_dialect
from observability.logging import get_logger

log = get_logger(__name__)


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

    pk = dialect.quote(_pk_column(table))
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
    from db.executor import fetch_key_bounds

    table = snap.table
    key = _pk_column(table)
    caps = capabilities_for_db_url(config.DB_URL)
    if not caps.supports_range_key_bounds:
        log.warning("range_planning_fallback_modulo", table=table.name,
                    key=key, reason="flavor_capability")
        return False

    try:
        bounds = await fetch_key_bounds(table.source_table, key)
    except Exception as exc:  # noqa: BLE001 - planning must not break startup
        log.warning("range_planning_error_fallback_modulo", table=table.name, error=str(exc))
        return False
    if bounds is None:
        log.warning("range_planning_fallback_modulo", table=table.name,
                    key=key, reason="no_integer_bounds")
        return False

    lo, hi = bounds
    ranges = compute_key_ranges(lo, hi, len(snap.splits))
    for split, (rlo, rhi) in zip(snap.splits, ranges):
        split.key_lo, split.key_hi = rlo, rhi
    log.info("range_planning_ok", table=table.name, key=key,
             key_min=lo, key_max=hi, splits=len(snap.splits))
    return True

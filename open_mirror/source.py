"""Source database -> landing-zone orchestration.

Reuses the proxy's existing connectors: schema reflection
(:func:`db.executor.derive_table_schema`), the per-connection query executor
(:func:`db.executor.execute_split_query`), and the dialect adapter
(:func:`planner.dialects.get_dialect`) — so the source engine, dialect quoting,
and connection binding are identical to the read path. The only new behavior is
writing the reflected rows into a Fabric Open Mirroring landing zone.
"""
from __future__ import annotations

from dataclasses import dataclass

import config
from db.executor import derive_table_schema, execute_split_query
from open_mirror.config import OpenMirrorTableTarget, OpenMirrorTarget
from open_mirror.landing_zone import LandingZoneBackend, open_landing_zone
from open_mirror.publisher import LandingZonePublisher
from planner.dialects import get_dialect

_MAX_ROWS_PARAM = "max_rows"


@dataclass(frozen=True)
class PublishResult:
    """Outcome of publishing one table into a landing zone."""

    target_id: str
    table: str
    rows: int
    path: str
    columns: list["config.ColumnDef"]


def _select_all_sql(dialect, projected: str, source: str, *, max_rows_param: str = _MAX_ROWS_PARAM) -> str:
    """Bounded full-table read, using the dialect's row-limit convention."""
    name = getattr(dialect, "name", "generic")
    if name == "mssql":
        return f"SELECT TOP (:{max_rows_param}) {projected} FROM {source}"
    if name == "oracle":
        return f"SELECT {projected} FROM {source} FETCH FIRST :{max_rows_param} ROWS ONLY"
    if name == "teradata":
        return (
            f"SELECT {projected} FROM {source} "
            f"QUALIFY ROW_NUMBER() OVER (ORDER BY 1) <= :{max_rows_param}"
        )
    return f"SELECT {projected} FROM {source} LIMIT :{max_rows_param}"


async def _read_source_rows(
    source_table: str,
    columns: list["config.ColumnDef"],
    connection: str,
    *,
    max_rows: int | None = None,
) -> list[dict]:
    dialect = get_dialect(config.effective_db_url(connection))
    projected = ", ".join(dialect.quote(col.name) for col in columns)
    source = dialect.quote_qualified(source_table)
    sql = _select_all_sql(dialect, projected, source)
    limit = max_rows if max_rows is not None else config.effective_query_max_rows(connection)
    return await execute_split_query(sql, {_MAX_ROWS_PARAM: limit}, split_index=0, connection=connection)


async def publish_initial_load(
    target: OpenMirrorTarget,
    table: OpenMirrorTableTarget,
    *,
    backend: LandingZoneBackend | None = None,
    publisher: LandingZonePublisher | None = None,
    max_rows: int | None = None,
) -> PublishResult:
    """Reflect a source table, read its rows, and publish an initial-load batch.

    Writes ``_metadata.json`` (with the target's key columns), the database-level
    ``_partnerEvents.json`` when source info is set, and a numbered Parquet file.
    The initial load carries no ``__rowMarker__`` (Fabric treats it as inserts).
    """
    connection = target.connection_id
    columns = await derive_table_schema(table.source_table, connection)
    rows = await _read_source_rows(table.source_table, columns, connection, max_rows=max_rows)

    if publisher is None:
        backend = backend or open_landing_zone(target.landing_zone_root)
        publisher = LandingZonePublisher(backend, target)

    publisher.ensure_partner_events()
    rel = publisher.publish_initial_load(table, rows, columns)
    return PublishResult(
        target_id=target.id,
        table=table.name,
        rows=len(rows),
        path=rel,
        columns=columns,
    )


async def publish_target_initial_load(
    target: OpenMirrorTarget,
    *,
    backend: LandingZoneBackend | None = None,
    max_rows: int | None = None,
) -> list[PublishResult]:
    """Publish an initial load for every enabled table in a target."""
    if not target.enabled:
        return []
    backend = backend or open_landing_zone(target.landing_zone_root)
    publisher = LandingZonePublisher(backend, target)
    results: list[PublishResult] = []
    for table in target.tables:
        if not table.enabled:
            continue
        results.append(
            await publish_initial_load(target, table, publisher=publisher, max_rows=max_rows)
        )
    return results

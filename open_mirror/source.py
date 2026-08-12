"""Source database -> landing-zone orchestration.

Reuses the proxy's existing connectors: schema reflection
(:func:`db.executor.derive_table_schema`), the per-connection query executor
(:func:`db.executor.execute_split_query`), and the dialect adapter
(:func:`planner.dialects.get_dialect`) — so the source engine, dialect quoting,
and connection binding are identical to the read path.

Publishing is retry-safe and quarantined:
- incremental change-tracking state is saved only AFTER the data file is durably
  written, so a mid-cycle failure never advances the snapshot;
- one table's failure is captured and does not abort the rest of the target.
"""
from __future__ import annotations

from dataclasses import dataclass, field

import config
from db.executor import derive_table_schema, execute_split_query
from observability.logging import get_logger
from open_mirror.changes import compute_changes
from open_mirror.config import OpenMirrorTableTarget, OpenMirrorTarget
from open_mirror.landing_zone import LandingZoneBackend, open_landing_zone
from open_mirror.publisher import LandingZonePublisher
from open_mirror.state import (
    build_state_from_rows,
    delete_state,
    load_published_tables,
    load_state,
    save_published_tables,
    save_state,
)
from planner.dialects import get_dialect

log = get_logger(__name__)

_MAX_ROWS_PARAM = "max_rows"


@dataclass
class PublishResult:
    """Outcome of publishing one table into a landing zone."""

    target_id: str
    table: str
    action: str = "noop"           # initial | incremental | noop | dry_run | error
    rows: int = 0
    inserts: int = 0
    updates: int = 0
    deletes: int = 0
    path: str | None = None
    error: str | None = None
    columns: list["config.ColumnDef"] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.action != "error"


@dataclass
class TargetResult:
    """Aggregated per-target publish outcome."""

    target_id: str
    results: list[PublishResult] = field(default_factory=list)
    dropped: list[str] = field(default_factory=list)
    skipped: bool = False
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None and all(r.ok for r in self.results)


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


def _state_dir() -> str:
    return getattr(config, "OPEN_MIRROR_STATE_DIR", "./.open_mirror_state")


def _default_mode() -> str:
    return (getattr(config, "OPEN_MIRROR_MODE", "incremental") or "incremental").strip().lower()


def _effective_max_rows(max_rows: int | None) -> int | None:
    if max_rows is not None:
        return max_rows
    cfg = int(getattr(config, "OPEN_MIRROR_MAX_ROWS", 0) or 0)
    return cfg or None


# ---------------------------------------------------------------------------
# Per-table publish
# ---------------------------------------------------------------------------

async def publish_table(
    target: OpenMirrorTarget,
    table: OpenMirrorTableTarget,
    *,
    publisher: LandingZonePublisher | None = None,
    backend: LandingZoneBackend | None = None,
    mode: str | None = None,
    dry_run: bool = False,
    max_rows: int | None = None,
    state_dir: str | None = None,
) -> PublishResult:
    """Publish one table: initial load on first run, else an incremental diff.

    ``mode`` overrides the configured default (``initial`` always writes a full
    insert batch; ``incremental`` diffs against saved state). ``dry_run`` computes
    the change set and returns counts without writing anything.
    """
    mode = (mode or _default_mode()).strip().lower()
    state_dir = state_dir or _state_dir()
    max_rows = _effective_max_rows(max_rows)
    connection = target.connection_id
    key_columns = table.key_columns

    columns = await derive_table_schema(table.source_table, connection)
    rows = await _read_source_rows(table.source_table, columns, connection, max_rows=max_rows)

    if publisher is None:
        backend = backend or open_landing_zone(target.landing_zone_root)
        publisher = LandingZonePublisher(backend, target)

    prev = load_state(state_dir, target, table)
    do_initial = mode == "initial" or prev is None

    if do_initial:
        if dry_run:
            return PublishResult(target.id, table.name, action="dry_run", rows=len(rows),
                                 inserts=len(rows), columns=columns)
        publisher.ensure_partner_events()
        path = publisher.publish_initial_load(table, rows, columns)
        save_state(state_dir, target, table, build_state_from_rows(rows, columns, key_columns))
        log.info("open_mirror_initial", target=target.id, table=table.name, rows=len(rows), path=path)
        return PublishResult(target.id, table.name, action="initial", rows=len(rows),
                             inserts=len(rows), path=path, columns=columns)

    batch = compute_changes(prev, rows, columns, key_columns)
    if not batch.has_changes:
        return PublishResult(target.id, table.name, action="noop", rows=0, columns=columns)

    if dry_run:
        return PublishResult(target.id, table.name, action="dry_run", rows=batch.total,
                             inserts=batch.inserts, updates=batch.updates, deletes=batch.deletes,
                             columns=columns)

    publisher.ensure_partner_events()
    path = publisher.publish_changes(table, batch.rows, batch.markers, columns)
    # Retry-safe: advance the snapshot only AFTER the change file is written.
    save_state(state_dir, target, table, batch.new_state)
    log.info("open_mirror_incremental", target=target.id, table=table.name,
             inserts=batch.inserts, updates=batch.updates, deletes=batch.deletes, path=path)
    return PublishResult(target.id, table.name, action="incremental", rows=batch.total,
                         inserts=batch.inserts, updates=batch.updates, deletes=batch.deletes,
                         path=path, columns=columns)


# ---------------------------------------------------------------------------
# Per-target publish (quarantine + drop reconciliation)
# ---------------------------------------------------------------------------

def _reconcile_drops(
    target: OpenMirrorTarget,
    publisher: LandingZonePublisher,
    state_dir: str,
    *,
    dry_run: bool,
) -> list[str]:
    """Drop landing-zone folders for tables removed from the target config."""
    configured = {(t.schema or "", t.target_table) for t in target.tables}
    previously = load_published_tables(state_dir, target)
    dropped: list[str] = []
    for entry in previously:
        schema = entry.get("schema") or ""
        target_table = entry.get("target_table") or ""
        if not target_table or (schema, target_table) in configured:
            continue
        stale = OpenMirrorTableTarget(
            name=target_table, source_table="", target_table=target_table,
            key_column="_dropped", schema=(schema or None),
        )
        if not dry_run:
            publisher.drop_table(stale)
            delete_state(state_dir, target, stale)
        dropped.append(publisher._table_dir(stale))
        log.info("open_mirror_dropped", target=target.id, table=target_table, dry_run=dry_run)
    return dropped


async def publish_target(
    target: OpenMirrorTarget,
    *,
    backend: LandingZoneBackend | None = None,
    mode: str | None = None,
    dry_run: bool = False,
    max_rows: int | None = None,
    state_dir: str | None = None,
    reconcile_drops: bool = True,
) -> TargetResult:
    """Publish every enabled table in a target, quarantining per-table failures."""
    if not target.enabled:
        return TargetResult(target.id, skipped=True)

    state_dir = state_dir or _state_dir()
    try:
        backend = backend or open_landing_zone(target.landing_zone_root)
    except Exception as exc:  # noqa: BLE001 - a bad target must not abort the cycle
        log.warning("open_mirror_target_unavailable", target=target.id, error=str(exc))
        return TargetResult(target.id, error=str(exc))

    publisher = LandingZonePublisher(backend, target)
    out = TargetResult(target.id)

    if reconcile_drops:
        try:
            out.dropped = _reconcile_drops(target, publisher, state_dir, dry_run=dry_run)
        except Exception as exc:  # noqa: BLE001
            log.warning("open_mirror_reconcile_failed", target=target.id, error=str(exc))

    for table in target.tables:
        if not table.enabled:
            continue
        try:
            out.results.append(await publish_table(
                target, table, publisher=publisher, mode=mode, dry_run=dry_run,
                max_rows=max_rows, state_dir=state_dir,
            ))
        except Exception as exc:  # noqa: BLE001 - quarantine this table, keep going
            log.warning("open_mirror_table_failed", target=target.id, table=table.name, error=str(exc))
            out.results.append(PublishResult(target.id, table.name, action="error", error=str(exc)))

    if not dry_run:
        save_published_tables(
            state_dir, target,
            [{"schema": t.schema or "", "target_table": t.target_table}
             for t in target.tables if t.enabled],
        )
    return out


async def publish_all(
    *,
    targets: list[OpenMirrorTarget] | None = None,
    mode: str | None = None,
    dry_run: bool = False,
    max_rows: int | None = None,
    state_dir: str | None = None,
) -> list[TargetResult]:
    """Publish every configured (or supplied) target, quarantining failures."""
    if targets is None:
        from open_mirror.config import load_targets
        targets = load_targets()
    results: list[TargetResult] = []
    for target in targets:
        results.append(await publish_target(
            target, mode=mode, dry_run=dry_run, max_rows=max_rows, state_dir=state_dir,
        ))
    return results


# ---------------------------------------------------------------------------
# Back-compat: initial-load-only helpers (used by earlier phases/tests)
# ---------------------------------------------------------------------------

async def publish_initial_load(
    target: OpenMirrorTarget,
    table: OpenMirrorTableTarget,
    *,
    backend: LandingZoneBackend | None = None,
    publisher: LandingZonePublisher | None = None,
    max_rows: int | None = None,
) -> PublishResult:
    """Reflect a source table, read its rows, and publish an initial-load batch."""
    connection = target.connection_id
    columns = await derive_table_schema(table.source_table, connection)
    rows = await _read_source_rows(table.source_table, columns, connection, max_rows=max_rows)

    if publisher is None:
        backend = backend or open_landing_zone(target.landing_zone_root)
        publisher = LandingZonePublisher(backend, target)

    publisher.ensure_partner_events()
    rel = publisher.publish_initial_load(table, rows, columns)
    return PublishResult(target.id, table.name, action="initial", rows=len(rows),
                         inserts=len(rows), path=rel, columns=columns)


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


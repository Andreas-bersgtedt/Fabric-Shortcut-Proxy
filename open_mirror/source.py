"""Source database to restart-safe Open Mirroring landing-zone orchestration."""
from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass, field

import config
from db.executor import derive_table_schema, execute_split_query
from observability.logging import get_logger
from open_mirror.changes import RowMarker, compute_changes
from open_mirror.config import OpenMirrorTableTarget, OpenMirrorTarget
from open_mirror.landing_zone import LandingZoneBackend, open_landing_zone
from open_mirror.publisher import LandingZonePublisher
from open_mirror.state import (
    CommittedCursor,
    PendingBatch,
    PublishState,
    build_state_from_rows,
    decode_watermark,
    delete_state,
    encode_watermark,
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
    target_id: str
    table: str
    action: str = "noop"
    rows: int = 0
    inserts: int = 0
    updates: int = 0
    deletes: int = 0
    path: str | None = None
    error: str | None = None
    columns: list[config.ColumnDef] = field(default_factory=list)
    strategy: str | None = None
    reason: str | None = None
    input_cursor: dict | None = None
    output_cursor: dict | None = None
    pages_read: int = 0
    rows_scanned: int = 0
    rows_published: int = 0
    state_status: str | None = None
    state_path: str | None = None
    recovery: str | None = None
    query_mode: str | None = None

    @property
    def ok(self) -> bool:
        return self.action != "error"


@dataclass
class TargetResult:
    target_id: str
    results: list[PublishResult] = field(default_factory=list)
    dropped: list[str] = field(default_factory=list)
    skipped: bool = False
    error: str | None = None
    replication_status: str | None = None
    replication_action: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None and all(r.ok for r in self.results)


class StateSafetyError(RuntimeError):
    pass


class PendingStateError(StateSafetyError):
    pass


def _select_all_sql(dialect, projected: str, source: str, *, max_rows_param=_MAX_ROWS_PARAM):
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


def _limited_ordered_sql(dialect, projected, source, where, order, max_rows_param):
    name = getattr(dialect, "name", "generic")
    clauses = f"{where} ORDER BY {order}".strip()
    if name == "mssql":
        return f"SELECT TOP (:{max_rows_param}) {projected} FROM {source} {clauses}"
    if name == "oracle":
        return (
            f"SELECT {projected} FROM {source} {clauses} "
            f"FETCH FIRST :{max_rows_param} ROWS ONLY"
        )
    if name == "teradata":
        return (
            f"SELECT {projected} FROM {source} {where} "
            f"QUALIFY ROW_NUMBER() OVER (ORDER BY {order}) <= :{max_rows_param} "
            f"ORDER BY {order}"
        ).replace("  ", " ")
    return f"SELECT {projected} FROM {source} {clauses} LIMIT :{max_rows_param}"


def _select_ordered_sql(
    dialect, projected: str, source: str, order_col: str, *,
    key_cols: list[str] | None = None, max_rows_param=_MAX_ROWS_PARAM,
):
    order = ", ".join([order_col, *(key_cols or [])])
    return _limited_ordered_sql(dialect, projected, source, "", order, max_rows_param)


def _key_ladder(columns: list[str], prefix: str = "key") -> str:
    parts = []
    for index, column in enumerate(columns):
        equals = " AND ".join(
            f"{columns[j]} = :{prefix}{j}" for j in range(index)
        )
        greater = f"{column} > :{prefix}{index}"
        parts.append(f"({equals} AND {greater})" if equals else greater)
    return " OR ".join(parts)


def _select_since_sql(
    dialect, projected: str, source: str, order_col: str, *,
    wm_param: str, key_cols: list[str] | None = None,
    order_key_cols: list[str] | None = None, max_rows_param=_MAX_ROWS_PARAM,
):
    key_cols = key_cols or []
    where = f"WHERE {order_col} > :{wm_param}"
    if key_cols:
        where += (
            f" OR ({order_col} = :{wm_param} AND ({_key_ladder(key_cols)}))"
        )
    order = ", ".join([
        order_col,
        *(order_key_cols if order_key_cols is not None else key_cols),
    ])
    return _limited_ordered_sql(dialect, projected, source, where, order, max_rows_param)


async def _read_rows_watermark(
    source_table: str,
    columns: list[config.ColumnDef],
    connection: str,
    watermark_column: str,
    last_watermark,
    *,
    key_columns: list[str] | None = None,
    last_keys: list | None = None,
    max_rows: int | None = None,
) -> list[dict]:
    dialect = get_dialect(config.effective_db_url(connection))
    projected = ", ".join(dialect.quote(col.name) for col in columns)
    source = dialect.quote_qualified(source_table)
    order_col = dialect.quote(watermark_column)
    key_cols = [dialect.quote(col) for col in (key_columns or [])]
    limit = max_rows if max_rows is not None else config.effective_query_max_rows(connection)
    if last_watermark is None:
        sql = _select_ordered_sql(
            dialect, projected, source, order_col, key_cols=key_cols
        )
        params = {_MAX_ROWS_PARAM: limit}
    else:
        predicate_keys = (
            key_cols if len(last_keys or []) == len(key_cols) else []
        )
        sql = _select_since_sql(
            dialect, projected, source, order_col, wm_param="wm",
            key_cols=predicate_keys, order_key_cols=key_cols,
        )
        params = {_MAX_ROWS_PARAM: limit, "wm": last_watermark}
        params.update({f"key{i}": value for i, value in enumerate(last_keys or [])})
    return await execute_split_query(sql, params, split_index=0, connection=connection)


def _max_watermark(rows, watermark_column: str):
    vals = [r.get(watermark_column) for r in rows if r.get(watermark_column) is not None]
    try:
        return max(vals) if vals else None
    except TypeError:
        return vals[-1] if vals else None


async def _read_source_rows(source_table, columns, connection, *, max_rows=None):
    dialect = get_dialect(config.effective_db_url(connection))
    projected = ", ".join(dialect.quote(col.name) for col in columns)
    source = dialect.quote_qualified(source_table)
    sql = _select_all_sql(dialect, projected, source)
    limit = max_rows if max_rows is not None else config.effective_query_max_rows(connection)
    return await execute_split_query(
        sql, {_MAX_ROWS_PARAM: limit}, split_index=0, connection=connection
    )


def _state_dir() -> str:
    return getattr(config, "OPEN_MIRROR_STATE_DIR", "./.open_mirror_state")


def _default_mode() -> str:
    return (getattr(config, "OPEN_MIRROR_MODE", "incremental") or "incremental").strip().lower()


def _effective_max_rows(max_rows):
    if max_rows is not None:
        return max_rows
    configured = int(getattr(config, "OPEN_MIRROR_MAX_ROWS", 0) or 0)
    return configured or None


def _effective_mode(table: OpenMirrorTableTarget, invocation: str | None) -> str:
    mode = invocation.strip().lower() if invocation else (
        (table.mode or "").strip().lower() or _default_mode()
    )
    if mode not in {"incremental", "watermark", "snapshot", "initial"}:
        raise ValueError(f"unsupported Open Mirror mode {mode!r}")
    return mode


def _strategy(table: OpenMirrorTableTarget, mode: str) -> str:
    if mode in {"watermark", "snapshot"}:
        return mode
    return table.strategy


def _cursor_json(cursor: CommittedCursor | None) -> dict | None:
    return cursor.to_json() if cursor else None


def _cursor_values(cursor: CommittedCursor | None) -> tuple[object | None, list]:
    if cursor is None:
        return None, []
    return decode_watermark(cursor.watermark), [
        decode_watermark(value) for value in cursor.keys
    ]


def _next_cursor(row: dict, watermark_column: str, key_columns: list[str]) -> CommittedCursor:
    values = [row.get(watermark_column), *(row.get(k) for k in key_columns)]
    if any(value is None for value in values):
        raise ValueError(
            f"cursor columns must be non-null: {[watermark_column, *key_columns]}"
        )
    return CommittedCursor(
        watermark=encode_watermark(values[0]),
        keys=[encode_watermark(value) for value in values[1:]],
    )


def _now() -> str:
    return _dt.datetime.now(_dt.UTC).isoformat().replace("+00:00", "Z")


def _load_valid_state(state_dir, target, table) -> tuple[PublishState | None, object]:
    loaded = load_state(state_dir, target, table)
    if loaded.status not in {"missing", "valid"}:
        raise StateSafetyError(
            f"Open Mirror state is {loaded.status} at {loaded.path}: {loaded.error}; "
            "refusing an implicit full load"
        )
    return loaded.state, loaded


def _finalize_pending(state_dir, target, table, state: PublishState) -> None:
    pending = state.pending
    if pending is None:
        return
    pending.next.file = pending.path
    pending.next.committed_at = _now()
    state.committed = pending.next
    state.watermark = pending.next.watermark
    state.initialized = True
    state.published_rows_total += pending.row_count
    state.last_batch_rows = pending.row_count
    state.last_published_at = state.committed.committed_at
    if pending.snapshot_keys is not None:
        state.keys = pending.snapshot_keys
    state.pending = None
    save_state(state_dir, target, table, state)


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
    state_dir = state_dir or _state_dir()
    max_rows = _effective_max_rows(max_rows)
    effective_mode = _effective_mode(table, mode)
    strategy = _strategy(table, effective_mode)
    state, loaded = _load_valid_state(state_dir, target, table)
    explicit_initial = effective_mode == "initial"
    reason = "operator_requested" if explicit_initial else (
        "state_missing" if loaded.status == "missing" else None
    )
    if state is not None and state.strategy != strategy and not explicit_initial:
        raise StateSafetyError(
            f"tracking strategy changed from {state.strategy!r} to {strategy!r} at "
            f"{loaded.path}; explicitly reset or request an initial load"
        )
    if publisher is None:
        backend = backend or open_landing_zone(target.landing_zone_root)
        publisher = LandingZonePublisher(backend, target)

    if state is not None and state.pending and publisher.backend.exists(state.pending.path):
        input_cursor = state.pending.prior
        output_cursor = state.pending.next
        _finalize_pending(state_dir, target, table, state)
        return PublishResult(
            target.id, table.name, action="recovery", rows=0, strategy=strategy,
            reason="pending_file_found", input_cursor=_cursor_json(input_cursor),
            output_cursor=_cursor_json(output_cursor), state_status=loaded.status,
            state_path=loaded.path, recovery="finalized_existing_file",
            query_mode="watermark" if strategy == "watermark" else "snapshot_full_scan",
        )
    if explicit_initial and (state is None or state.pending is None) or state is None:
        state = PublishState(strategy=strategy)

    columns = await derive_table_schema(table.source_table, target.connection_id)
    if strategy == "watermark":
        return await _publish_watermark(
            target, table, columns, publisher, state, loaded, reason=reason,
            dry_run=dry_run, max_rows=max_rows, state_dir=state_dir,
        )
    return await _publish_snapshot(
        target, table, columns, publisher, state, loaded, reason=reason,
        dry_run=dry_run, max_rows=max_rows, state_dir=state_dir,
    )


async def _publish_watermark(
    target, table, columns, publisher, state, loaded, *,
    reason, dry_run, max_rows, state_dir,
) -> PublishResult:
    wm_col = table.watermark_column
    column_names = {column.name for column in columns}
    if not wm_col or wm_col not in column_names:
        raise ValueError(
            f"watermark_column {wm_col!r} is not a column of {table.source_table!r}"
        )
    missing_keys = [key for key in table.key_columns if key not in column_names]
    if missing_keys:
        raise ValueError(f"key columns are not present in source: {missing_keys}")

    initial = not state.initialized
    initial_publish = initial
    cursor = state.pending.prior if state.pending else state.committed
    input_cursor = cursor
    page_size = max_rows or config.effective_query_max_rows(target.connection_id)
    max_pages = int(getattr(config, "OPEN_MIRROR_MAX_PAGES_PER_CYCLE", 0) or 0)
    max_cycle_rows = int(getattr(config, "OPEN_MIRROR_MAX_ROWS_PER_CYCLE", 0) or 0)
    pages = scanned = published = 0
    last_path = None
    recovery = "retry_missing_file" if state.pending else None
    aggregate_action = "initial" if initial else "noop"

    while True:
        watermark, keys = _cursor_values(cursor)
        rows = await _read_rows_watermark(
            table.source_table, columns, target.connection_id, wm_col, watermark,
            key_columns=table.key_columns, last_keys=keys, max_rows=page_size,
        )
        pages += 1
        scanned += len(rows)
        if not rows:
            if initial and not dry_run:
                state.initialized = True
                state.strategy = "watermark"
                save_state(state_dir, target, table, state)
            break

        next_cursor = _next_cursor(rows[-1], wm_col, table.key_columns)
        markers = None if initial and published == 0 else [RowMarker.UPSERT] * len(rows)
        if dry_run:
            cursor = next_cursor
        else:
            publisher.ensure_partner_events()
            publisher.ensure_table_metadata(table)
            payload, digest = publisher.prepare_batch(rows, columns, row_markers=markers)
            if state.pending:
                pending = state.pending
                if (
                    pending.row_count != len(rows)
                    or pending.content_hash != digest
                    or pending.next.watermark != next_cursor.watermark
                    or pending.next.keys != next_cursor.keys
                ):
                    raise PendingStateError(
                        f"pending batch at {loaded.path} no longer matches its source page"
                    )
                path = pending.path
            else:
                path = publisher.reserve_batch_path(table)
                state.pending = PendingBatch(
                    prior=cursor, next=next_cursor, path=path,
                    row_count=len(rows), content_hash=digest,
                    initial=initial and published == 0,
                )
                save_state(state_dir, target, table, state)
            publisher.write_batch_at(path, payload)
            _finalize_pending(state_dir, target, table, state)
            cursor = state.committed
            last_path = path
        published += len(rows)
        aggregate_action = "initial" if initial_publish else "incremental"
        initial = False
        state.pending = None
        if len(rows) < page_size:
            break
        if max_pages and pages >= max_pages:
            break
        if max_cycle_rows and scanned >= max_cycle_rows:
            break

    result_action = "dry_run" if dry_run else aggregate_action
    result = PublishResult(
        target.id, table.name, action=result_action, rows=published,
        inserts=published if initial_publish else 0,
        updates=published if aggregate_action == "incremental" else 0,
        path=last_path, columns=columns, strategy="watermark", reason=reason,
        input_cursor=_cursor_json(input_cursor), output_cursor=_cursor_json(cursor),
        pages_read=pages, rows_scanned=scanned, rows_published=published,
        state_status=loaded.status, state_path=loaded.path, recovery=recovery,
        query_mode="initial" if input_cursor is None else "watermark",
    )
    log.info(
        "open_mirror_table", target=target.id, table=table.name,
        action=result.action, strategy=result.strategy, reason=reason,
        input_cursor=str(result.input_cursor), output_cursor=str(result.output_cursor),
        pages_read=pages, rows_scanned=scanned, rows_published=published,
        state_status=loaded.status, state_path=loaded.path, recovery=recovery,
        query_mode=result.query_mode,
    )
    return result


async def _publish_snapshot(
    target, table, columns, publisher, state, loaded, *,
    reason, dry_run, max_rows, state_dir,
) -> PublishResult:
    rows = await _read_source_rows(
        table.source_table, columns, target.connection_id, max_rows=max_rows
    )
    initial = not state.initialized
    if initial:
        output_state = build_state_from_rows(rows, columns, table.key_columns)
        batch_rows = rows
        markers = None
        inserts, updates, deletes = len(rows), 0, 0
        action = "initial"
    else:
        batch = compute_changes(state, rows, columns, table.key_columns)
        output_state = batch.new_state
        batch_rows, markers = batch.rows, batch.markers
        inserts, updates, deletes = batch.inserts, batch.updates, batch.deletes
        action = "incremental" if batch.has_changes else "noop"

    recovery = "retry_missing_file" if state.pending else None
    if dry_run:
        action = "dry_run"
    elif action == "noop":
        return PublishResult(
            target.id, table.name, action="noop", columns=columns,
            strategy="snapshot", rows_scanned=len(rows), pages_read=1,
            state_status=loaded.status, state_path=loaded.path,
            query_mode="snapshot_full_scan",
        )
    else:
        publisher.ensure_partner_events()
        publisher.ensure_table_metadata(table)
        payload, digest = publisher.prepare_batch(
            batch_rows, columns, row_markers=markers
        )
        if state.pending:
            pending = state.pending
            if pending.content_hash != digest or pending.row_count != len(batch_rows):
                raise PendingStateError(
                    f"pending snapshot batch at {loaded.path} no longer matches source"
                )
            path = pending.path
        else:
            path = publisher.reserve_batch_path(table)
            state.pending = PendingBatch(
                prior=state.committed, next=CommittedCursor(), path=path,
                row_count=len(batch_rows), content_hash=digest, initial=initial,
                snapshot_keys=output_state.keys,
            )
            save_state(state_dir, target, table, state)
        publisher.write_batch_at(path, payload)
        _finalize_pending(state_dir, target, table, state)
        state.strategy = "snapshot"
        save_state(state_dir, target, table, state)

    return PublishResult(
        target.id, table.name, action=action, rows=len(batch_rows),
        inserts=inserts, updates=updates, deletes=deletes,
        path=None if dry_run else path, columns=columns, strategy="snapshot",
        reason=reason, pages_read=1, rows_scanned=len(rows),
        rows_published=0 if dry_run else len(batch_rows),
        state_status=loaded.status, state_path=loaded.path,
        recovery=recovery,
        query_mode="snapshot_full_scan",
    )


def _reconcile_drops(target, publisher, state_dir, *, dry_run):
    configured = {(t.schema or "", t.target_table) for t in target.tables}
    dropped = []
    for entry in load_published_tables(state_dir, target):
        schema = entry.get("schema") or ""
        target_table = entry.get("target_table") or ""
        if not target_table or (schema, target_table) in configured:
            continue
        stale = OpenMirrorTableTarget(
            name=target_table, source_table="", target_table=target_table,
            key_column="_dropped", schema=schema or None,
        )
        if not dry_run:
            publisher.drop_table(stale)
            delete_state(state_dir, target, stale)
        dropped.append(publisher._table_dir(stale))
    return dropped


async def publish_target(
    target: OpenMirrorTarget, *, backend=None, mode=None, dry_run=False,
    max_rows=None, state_dir=None, reconcile_drops=True,
) -> TargetResult:
    if not target.enabled:
        return TargetResult(target.id, skipped=True)
    state_dir = state_dir or _state_dir()
    try:
        backend = backend or open_landing_zone(target.landing_zone_root)
    except Exception as exc:  # noqa: BLE001
        return TargetResult(target.id, error=str(exc))
    publisher = LandingZonePublisher(backend, target)
    out = TargetResult(target.id)
    if reconcile_drops:
        try:
            out.dropped = _reconcile_drops(
                target, publisher, state_dir, dry_run=dry_run
            )
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
        except Exception as exc:  # noqa: BLE001
            loaded = load_state(state_dir, target, table)
            log.warning(
                "open_mirror_table_failed", target=target.id, table=table.name,
                error=str(exc), state_status=loaded.status, state_path=loaded.path,
            )
            out.results.append(PublishResult(
                target.id, table.name, action="error", error=str(exc),
                strategy=table.strategy,
                reason=(
                    "state_pending_invalid"
                    if (
                        isinstance(exc, PendingStateError)
                        or (loaded.error and "pending batch" in loaded.error)
                    )
                    else f"state_{loaded.status}"
                ),
                state_status=loaded.status, state_path=loaded.path,
            ))
    if not dry_run:
        save_published_tables(
            state_dir, target,
            [{"schema": t.schema or "", "target_table": t.target_table}
             for t in target.tables if t.enabled],
        )
    return out


async def publish_all(
    *, targets=None, mode=None, dry_run=False, max_rows=None, state_dir=None,
):
    if targets is None:
        from open_mirror.config import load_targets
        targets = load_targets()
    return [
        await publish_target(
            target, mode=mode, dry_run=dry_run, max_rows=max_rows,
            state_dir=state_dir,
        )
        for target in targets
    ]


async def publish_initial_load(
    target, table, *, backend=None, publisher=None, max_rows=None,
):
    return await publish_table(
        target, table, backend=backend, publisher=publisher,
        max_rows=max_rows, mode="initial",
    )


async def publish_target_initial_load(target, *, backend=None, max_rows=None):
    if not target.enabled:
        return []
    backend = backend or open_landing_zone(target.landing_zone_root)
    publisher = LandingZonePublisher(backend, target)
    return [
        await publish_initial_load(
            target, table, publisher=publisher, max_rows=max_rows
        )
        for table in target.tables if table.enabled
    ]

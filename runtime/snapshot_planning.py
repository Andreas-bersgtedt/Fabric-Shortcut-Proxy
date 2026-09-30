"""Snapshot-aware table planning and worker hydration."""
from __future__ import annotations

from dataclasses import dataclass
import time

import config
from config import TableDef
from db.capabilities import flavor_from_db_url
from db.read_points import (
    OwnedReadPoint,
    ReadSession,
    TableReadPlan,
    get_provider,
    require_provider,
)
from iceberg.state_store import SnapshotState, build_table_snapshot
from planner.split_planner import choose_table_num_splits, plan_ranges_for_snapshot
from observability.logging import get_logger
from observability.metrics import record_read_point
from runtime.generation import (
    GenerationContext,
    GenerationError,
    PLAN_ACTIVE,
    assign_generation,
    fail_generation_plan,
    publish_generation_plan,
)

log = get_logger(__name__)
_ACTIVE_SESSIONS: dict[str, ReadSession] = {}
_ACTIVE_PLANS: dict[str, TableReadPlan] = {}
_ACTIVE_OWNED: dict[str, OwnedReadPoint] = {}


def table_read_id(table: TableDef) -> str:
    return f"{table.connection_id}::{table.source_table}"


def _key_type(table: TableDef, key: str) -> str | None:
    for column in table.schema or []:
        if key in {column.name, column.source_name}:
            return column.iceberg_type
    return None


def _decorate_splits(snapshot: SnapshotState, plan: TableReadPlan) -> None:
    provider = require_provider(
        flavor_from_db_url(config.effective_db_url(snapshot.table.connection_id))
    )
    for index, split in enumerate(snapshot.splits):
        split.split_key_column = plan.split_key or None
        if plan.ranges:
            split.key_lo, split.key_hi = plan.ranges[index]
        split.generation_owner_shard = plan.descriptor.owner_shard
        split.generation_distributed_snapshot = provider.distributed


def _plan_from_snapshot(
    snapshot: SnapshotState,
    descriptor,
) -> TableReadPlan:
    ranges = ()
    if snapshot.splits and all(
        split.key_lo is not None and split.key_hi is not None
        for split in snapshot.splits
    ):
        ranges = tuple((split.key_lo, split.key_hi) for split in snapshot.splits)
    key = (
        snapshot.splits[0].split_key_column
        if snapshot.splits else snapshot.table.key_column
    ) or ""
    return TableReadPlan(
        version=1,
        table_id=table_read_id(snapshot.table),
        descriptor=descriptor,
        split_count=len(snapshot.splits),
        split_strategy=snapshot.table.effective_split_strategy,
        split_key=key,
        split_key_type=_key_type(snapshot.table, key),
        ranges=ranges,
    )


@dataclass
class SnapshotPreparation:
    context: GenerationContext
    snapshots: list[SnapshotState]
    sessions: dict[str, ReadSession]
    owned: dict[str, OwnedReadPoint]


def activate_snapshot_preparation(preparation: SnapshotPreparation) -> None:
    _ACTIVE_SESSIONS.clear()
    _ACTIVE_SESSIONS.update(preparation.sessions)
    _ACTIVE_PLANS.clear()
    _ACTIVE_PLANS.update({
        plan.table_id: plan for plan in preparation.context.table_plans
    })
    _ACTIVE_OWNED.clear()
    _ACTIVE_OWNED.update(preparation.owned)


def read_session_for(table: TableDef) -> ReadSession | None:
    return _ACTIVE_SESSIONS.get(table_read_id(table))


def read_plan_for(table: TableDef) -> TableReadPlan | None:
    return _ACTIVE_PLANS.get(table_read_id(table))


def snapshot_status() -> dict:
    """Return token-free status for readiness and monitor payloads."""
    now_ms = time.time_ns() // 1_000_000
    plans = []
    for plan in sorted(_ACTIVE_PLANS.values(), key=lambda item: item.table_id):
        descriptor = plan.descriptor
        plans.append({
            "table_id": plan.table_id,
            "provider": descriptor.provider,
            "owner_shard": descriptor.owner_shard,
            "distributed": bool(
                require_provider(
                    flavor_from_db_url(
                        config.effective_db_url(descriptor.connection_id)
                    )
                ).distributed
            ),
            "age_ms": max(0, now_ms - descriptor.acquired_at_ms),
            "expires_at_ms": descriptor.expires_at_ms,
            "session_active": plan.table_id in _ACTIVE_SESSIONS,
        })
    return {
        "requested": config.GENERATION_SOURCE_CONSISTENCY,
        "effective": "snapshot" if plans else config.GENERATION_SOURCE_CONSISTENCY,
        "plan_count": len(plans),
        "plans": plans,
    }


async def close_active_table(
    table: TableDef, *, abort_reason: str | None = None
) -> None:
    """Close one table's joined session and release its owned read point."""
    table_id = table_read_id(table)
    owned = _ACTIVE_OWNED.pop(table_id, None)
    session = _ACTIVE_SESSIONS.pop(table_id, None)
    if owned is not None:
        if abort_reason is None:
            await owned.release()
            record_read_point("released", owned.descriptor.provider)
            log.info("read_point_released", table_id=table_id)
        else:
            await owned.abort(abort_reason)
            record_read_point("aborted", owned.descriptor.provider)
            log.warning(
                "read_point_aborted", table_id=table_id, reason=abort_reason
            )
        return
    if session is not None:
        await session.close()
        provider = _ACTIVE_PLANS.get(table_id)
        record_read_point(
            "released",
            provider.descriptor.provider if provider is not None else "unknown",
        )
        log.info("read_point_released", table_id=table_id, joined=True)


async def prepare_snapshot_generation(
    store,
    context: GenerationContext,
    tables: list[TableDef],
    *,
    bucket: str,
    warehouse_prefix: str,
    owner_shard: int,
) -> SnapshotPreparation:
    """Acquire read points, plan tables, and publish one immutable generation plan."""
    owners: dict[str, OwnedReadPoint] = {}
    sessions: dict[str, ReadSession] = {}
    snapshots: list[SnapshotState] = []
    plans: list[TableReadPlan] = []
    try:
        for table in tables:
            table_id = table_read_id(table)
            acquired_started = time.monotonic()
            flavor = flavor_from_db_url(
                config.effective_db_url(table.connection_id)
            )
            provider = require_provider(flavor)
            await provider.validate(table.connection_id, table_id)
            owned = await provider.acquire_owner(
                connection_id=table.connection_id,
                table_id=table_id,
                generation_id=context.generation_id,
                generation_fence=context.fence,
                owner_shard=owner_shard,
            )
            owners[table_id] = owned
            sessions[table_id] = owned.session
            record_read_point("acquired", owned.descriptor.provider)
            log.info(
                "read_point_acquired",
                table_id=table_id,
                provider=owned.descriptor.provider,
                owner_shard=owned.descriptor.owner_shard,
                duration_ms=int((time.monotonic() - acquired_started) * 1000),
            )
            table.num_splits = await choose_table_num_splits(table, owned.session)
            snapshot = build_table_snapshot(table, bucket, warehouse_prefix)
            await plan_ranges_for_snapshot(snapshot, owned.session)
            snapshots.append(snapshot)
            plans.append(_plan_from_snapshot(snapshot, owned.descriptor))
        ready = publish_generation_plan(store, context, plans)
        log.info(
            "generation_plan_ready",
            generation_id=ready.generation_id,
            plan_sha256=ready.plan_sha256,
            tables=len(ready.table_plans),
        )
        assign_generation(snapshots, ready)
        plans_by_id = {plan.table_id: plan for plan in ready.table_plans}
        for snapshot in snapshots:
            _decorate_splits(snapshot, plans_by_id[table_read_id(snapshot.table)])
        return SnapshotPreparation(ready, snapshots, sessions, owners)
    except Exception as exc:
        for owned in reversed(list(owners.values())):
            try:
                await owned.abort("snapshot planning failed")
            except Exception:
                pass
        fail_generation_plan(store, context, str(exc))
        log.error(
            "generation_snapshot_failed",
            generation_id=context.generation_id,
            error=type(exc).__name__,
        )
        raise


async def hydrate_snapshot_generation(
    context: GenerationContext,
    tables: list[TableDef],
    *,
    bucket: str,
    warehouse_prefix: str,
    shard_index: int,
) -> SnapshotPreparation:
    """Build worker-local snapshots from the coordinator's immutable table plans."""
    tables_by_id = {table_read_id(table): table for table in tables}
    plan_ids = {plan.table_id for plan in context.table_plans}
    if plan_ids != set(tables_by_id):
        raise GenerationError("generation table plan does not match configured tables")
    snapshots: list[SnapshotState] = []
    sessions: dict[str, ReadSession] = {}
    for plan in context.table_plans:
        table = tables_by_id[plan.table_id]
        table.num_splits = plan.split_count
        snapshot = build_table_snapshot(table, bucket, warehouse_prefix)
        if len(snapshot.splits) != plan.split_count:
            raise GenerationError(
                f"table plan split count mismatch for {plan.table_id!r}"
            )
        _decorate_splits(snapshot, plan)
        snapshots.append(snapshot)
        provider = get_provider(
            flavor_from_db_url(
                config.effective_db_url(table.connection_id)
            )
        )
        if provider is None:
            raise GenerationError(
                f"snapshot provider is unavailable for {plan.table_id!r}"
            )
        if (
            context.plan_state != PLAN_ACTIVE
            and provider.distributed
            and shard_index != plan.descriptor.owner_shard
        ):
            sessions[plan.table_id] = await provider.join(plan.descriptor)
            record_read_point("joined", plan.descriptor.provider)
            log.info(
                "read_point_joined",
                table_id=plan.table_id,
                provider=plan.descriptor.provider,
                shard_index=shard_index,
            )
    assign_generation(snapshots, context)
    return SnapshotPreparation(context, snapshots, sessions, {})


async def close_snapshot_preparation(
    preparation: SnapshotPreparation, *, abort_reason: str | None = None
) -> None:
    """Close joined sessions and release or abort owner read points."""
    owner_session_ids = {id(owned.session) for owned in preparation.owned.values()}
    for session in preparation.sessions.values():
        if id(session) not in owner_session_ids:
            await session.close()
    for owned in preparation.owned.values():
        if abort_reason is None:
            await owned.release()
        else:
            await owned.abort(abort_reason)
    _ACTIVE_SESSIONS.clear()
    _ACTIVE_PLANS.clear()
    _ACTIVE_OWNED.clear()

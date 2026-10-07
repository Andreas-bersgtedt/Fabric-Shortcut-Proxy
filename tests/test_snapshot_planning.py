from __future__ import annotations

from dataclasses import dataclass
import json

import pytest

from fabric_shortcut_proxy import config
from fabric_shortcut_proxy.config import ColumnDef, TableDef
from fabric_shortcut_proxy.db.read_points import (
    ReadPointDescriptor,
    clear_provider_registry,
    register_builtin_providers,
    register_provider,
)
from fabric_shortcut_proxy.iceberg import state_store
from fabric_shortcut_proxy.runtime.artifact_store import MemoryStore
from fabric_shortcut_proxy.runtime.generation import BUILD_KEY, acquire_generation, join_generation
from fabric_shortcut_proxy.runtime.snapshot_planning import (
    close_snapshot_preparation,
    hydrate_snapshot_generation,
    prepare_snapshot_generation,
    read_plan_for,
    read_session_for,
    snapshot_status,
)


class _Session:
    connection_id = "default"

    def __init__(self):
        self.calls = []
        self.closed = False

    async def fetch_table_row_count(self, source_table):
        self.calls.append(("count", source_table))
        return 200

    async def fetch_key_bounds(self, source_table, key_column):
        self.calls.append(("bounds", source_table, key_column))
        return 1, 100

    async def fetch_column_bounds(self, source_table, key_column):
        raise AssertionError("not used")

    async def fetch_key_histogram_bounds(self, source_table, key_column, n):
        raise AssertionError("not used")

    async def fetch_key_quantile_bounds(
        self, source_table, key_column, n, *, sample_rows=0, key_is_integer=False
    ):
        raise AssertionError("not used")

    async def close(self):
        self.closed = True


@dataclass
class _Owned:
    descriptor: ReadPointDescriptor
    session: _Session
    released: bool = False
    aborted: str | None = None

    async def healthy(self):
        return not self.released and self.aborted is None

    async def release(self):
        self.released = True
        await self.session.close()

    async def abort(self, reason):
        self.aborted = reason
        await self.session.close()


class _Provider:
    flavor = "sqlite"
    reopenable = False

    def __init__(self, *, distributed=True):
        self.distributed = distributed
        self.owner = None
        self.joined = []

    async def validate(self, connection_id, table_id):
        return None

    async def acquire_owner(self, **kwargs):
        descriptor = ReadPointDescriptor(
            version=1,
            provider="test_snapshot",
            connection_id=kwargs["connection_id"],
            table_id=kwargs["table_id"],
            owner_shard=kwargs["owner_shard"],
            generation_id=kwargs["generation_id"],
            generation_fence=kwargs["generation_fence"],
            acquired_at_ms=1000,
            expires_at_ms=5000,
            token="test-token",
        )
        self.owner = _Owned(descriptor, _Session())
        return self.owner

    async def join(self, descriptor):
        session = _Session()
        self.joined.append((descriptor, session))
        return session


def _table():
    return TableDef(
        "orders",
        "sales.orders",
        [
            ColumnDef(1, "id", "long", nullable=False),
            ColumnDef(2, "amount", "decimal(12,2)"),
        ],
        key_column="id",
        num_splits=2,
        split_target_rows=100,
        split_strategy="range",
    )


@pytest.fixture(autouse=True)
def isolated_registry(monkeypatch):
    clear_provider_registry()
    state_store._snapshots.clear()
    state_store._history.clear()
    monkeypatch.setattr(
        config,
        "effective_db_url",
        lambda _connection_id="default": "sqlite+aiosqlite:///source.db",
    )
    yield
    state_store._snapshots.clear()
    state_store._history.clear()
    clear_provider_registry()
    register_builtin_providers()


async def test_coordinator_plans_inside_read_point_and_worker_hydrates():
    provider = _Provider(distributed=True)
    register_provider(provider)
    store = MemoryStore()
    context = acquire_generation(store, 2, prepare_plan=True)

    owner = await prepare_snapshot_generation(
        store,
        context,
        [_table()],
        bucket="bucket",
        warehouse_prefix="warehouse",
        owner_shard=0,
    )
    joined_context = join_generation(store, 2, timeout_seconds=0.1)
    worker = await hydrate_snapshot_generation(
        joined_context,
        [_table()],
        bucket="bucket",
        warehouse_prefix="warehouse",
        shard_index=1,
    )

    assert provider.owner.session.calls == [
        ("count", "sales.orders"),
        ("bounds", "sales.orders", "id"),
    ]
    assert len(owner.context.table_plans) == 1
    assert owner.context.table_plans[0].ranges == ((1, 51), (51, 101))
    assert len(provider.joined) == 1
    assert len(worker.sessions) == 1
    assert [
        (split.key_lo, split.key_hi) for split in worker.snapshots[0].splits
    ] == [(1, 51), (51, 101)]
    assert all(
        split.generation_plan_sha256 == owner.context.plan_sha256
        for split in worker.snapshots[0].splits
    )

    await close_snapshot_preparation(worker)
    await close_snapshot_preparation(owner)
    assert provider.owner.released is True
    assert provider.joined[0][1].closed is True


async def test_owner_only_provider_routes_all_splits_to_owner():
    provider = _Provider(distributed=False)
    register_provider(provider)
    store = MemoryStore()
    context = acquire_generation(store, 2, prepare_plan=True)

    owner = await prepare_snapshot_generation(
        store,
        context,
        [_table()],
        bucket="bucket",
        warehouse_prefix="warehouse",
        owner_shard=0,
    )
    worker = await hydrate_snapshot_generation(
        join_generation(store, 2, timeout_seconds=0.1),
        [_table()],
        bucket="bucket",
        warehouse_prefix="warehouse",
        shard_index=1,
    )

    assert worker.sessions == {}
    assert all(
        split.generation_owner_shard == 0
        and split.generation_distributed_snapshot is False
        for split in worker.snapshots[0].splits
    )
    await close_snapshot_preparation(worker)
    await close_snapshot_preparation(owner)


async def test_active_lookup_returns_plan_and_session():
    provider = _Provider(distributed=True)
    register_provider(provider)
    store = MemoryStore()
    context = acquire_generation(store, 1, prepare_plan=True)
    table = _table()
    preparation = await prepare_snapshot_generation(
        store,
        context,
        [table],
        bucket="bucket",
        warehouse_prefix="warehouse",
        owner_shard=0,
    )
    from fabric_shortcut_proxy.runtime.snapshot_planning import activate_snapshot_preparation

    activate_snapshot_preparation(preparation)
    assert read_session_for(table) is provider.owner.session
    assert read_plan_for(table) == preparation.context.table_plans[0]
    status = snapshot_status()
    assert status["requested"] == config.GENERATION_SOURCE_CONSISTENCY
    assert status["plan_count"] == 1
    assert status["plans"][0]["provider"] == "test_snapshot"
    assert "token" not in json.dumps(status)
    await close_snapshot_preparation(preparation)
    assert read_session_for(table) is None


async def test_late_worker_uses_active_artifact_generation_without_source_join():
    provider = _Provider(distributed=True)
    register_provider(provider)
    store = MemoryStore()
    context = acquire_generation(store, 2, prepare_plan=True)
    owner = await prepare_snapshot_generation(
        store,
        context,
        [_table()],
        bucket="bucket",
        warehouse_prefix="warehouse",
        owner_shard=0,
    )
    build = json.loads(store.get(BUILD_KEY))
    build["state"] = "ACTIVE"
    store.put(BUILD_KEY, json.dumps(build).encode())

    late = await hydrate_snapshot_generation(
        join_generation(store, 2, timeout_seconds=0.1),
        [_table()],
        bucket="bucket",
        warehouse_prefix="warehouse",
        shard_index=1,
    )

    assert late.context.plan_state == "ACTIVE"
    assert late.sessions == {}
    assert provider.joined == []
    await close_snapshot_preparation(late)
    await close_snapshot_preparation(owner)

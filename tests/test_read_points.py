from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from config import ColumnDef, TableDef
from db.read_points import (
    BestEffortReadSession,
    PostgresSnapshotProvider,
    ReadPointDescriptor,
    ReadPointUnavailable,
    SqlServerSnapshotProvider,
    TableReadPlan,
    clear_provider_registry,
    decode_boundary,
    encode_boundary,
    get_provider,
    register_provider,
    register_builtin_providers,
    registered_providers,
    require_provider,
)
from iceberg.state_store import build_table_snapshot
from planner.split_planner import choose_table_num_splits, plan_ranges_for_snapshot


@pytest.mark.parametrize(
    "value",
    [
        None,
        True,
        42,
        1.25,
        Decimal("123.4500"),
        date(2026, 9, 30),
        datetime(2026, 9, 30, 12, 30, tzinfo=timezone.utc),
        "key-1",
    ],
)
def test_boundary_encoding_round_trips_with_type(value):
    encoded = encode_boundary(value)

    assert set(encoded) == {"type", "value"}
    assert decode_boundary(encoded) == value


def test_boundary_encoding_rejects_unsupported_and_nonfinite_values():
    with pytest.raises(TypeError, match="bytes"):
        encode_boundary(b"binary")
    with pytest.raises(ValueError, match="finite"):
        encode_boundary(float("inf"))
    with pytest.raises(ValueError, match="unsupported"):
        decode_boundary({"type": "uuid", "value": "x"})


def _descriptor(**overrides) -> ReadPointDescriptor:
    values = {
        "version": 1,
        "provider": "fake",
        "connection_id": "warehouse",
        "table_id": "warehouse::sales.orders",
        "owner_shard": 0,
        "generation_id": "generation-1",
        "generation_fence": 3,
        "acquired_at_ms": 1000,
        "expires_at_ms": 5000,
        "token": "snapshot-token",
        "reopenable": False,
    }
    values.update(overrides)
    return ReadPointDescriptor(**values)


def test_descriptor_redacts_token_from_observability_payload():
    descriptor = _descriptor()

    safe = descriptor.to_dict()
    state = descriptor.to_state_dict()

    assert "token" not in safe
    assert safe["token_present"] is True
    assert state["token"] == "snapshot-token"
    assert ReadPointDescriptor.from_dict(state) == descriptor


def test_descriptor_validates_identity_and_lifetime():
    with pytest.raises(ValueError, match="provider"):
        _descriptor(provider="")
    with pytest.raises(ValueError, match="owner_shard"):
        _descriptor(owner_shard=-1)
    with pytest.raises(ValueError, match="expiry"):
        _descriptor(expires_at_ms=1000)


def test_table_read_plan_round_trips_typed_ranges_and_token():
    descriptor = _descriptor()
    plan = TableReadPlan(
        version=1,
        table_id=descriptor.table_id,
        descriptor=descriptor,
        split_count=2,
        split_strategy="date",
        split_key="created_at",
        split_key_type="timestamptz",
        ranges=(
            (
                datetime(2026, 1, 1, tzinfo=timezone.utc),
                datetime(2026, 2, 1, tzinfo=timezone.utc),
            ),
            (
                datetime(2026, 2, 1, tzinfo=timezone.utc),
                datetime(2026, 3, 1, tzinfo=timezone.utc),
            ),
        ),
    )

    safe = plan.to_dict()
    state = plan.to_state_dict()

    assert "token" not in safe["descriptor"]
    assert state["descriptor"]["token"] == "snapshot-token"
    assert TableReadPlan.from_dict(state) == plan


def test_table_read_plan_rejects_mismatched_table_and_range_count():
    descriptor = _descriptor()
    with pytest.raises(ValueError, match="table IDs differ"):
        TableReadPlan(
            version=1,
            table_id="other",
            descriptor=descriptor,
            split_count=1,
            split_strategy="range",
            split_key="id",
            split_key_type="long",
        )
    with pytest.raises(ValueError, match="ranges must match"):
        TableReadPlan(
            version=1,
            table_id=descriptor.table_id,
            descriptor=descriptor,
            split_count=2,
            split_strategy="range",
            split_key="id",
            split_key_type="long",
            ranges=((1, 2),),
        )


class _FakeProvider:
    flavor = "postgres"
    distributed = True
    reopenable = False

    def __init__(self):
        self.validated = []

    async def validate(self, connection_id, table_id):
        self.validated.append((connection_id, table_id))

    async def acquire_owner(self, **kwargs):
        descriptor = ReadPointDescriptor(
            version=1,
            provider="fake",
            connection_id=kwargs["connection_id"],
            table_id=kwargs["table_id"],
            owner_shard=kwargs["owner_shard"],
            generation_id=kwargs["generation_id"],
            generation_fence=kwargs["generation_fence"],
            acquired_at_ms=1000,
            expires_at_ms=5000,
            token="owned-token",
        )
        return _FakeOwnedReadPoint(descriptor)

    async def join(self, descriptor):
        return _LifecycleSession(descriptor.connection_id)


class _LifecycleSession:
    def __init__(self, connection_id):
        self.connection_id = connection_id
        self.closed = False

    async def close(self):
        self.closed = True


class _FakeOwnedReadPoint:
    def __init__(self, descriptor):
        self.descriptor = descriptor
        self.session = _LifecycleSession(descriptor.connection_id)
        self.released = False
        self.aborted = None

    async def healthy(self):
        return not self.released and self.aborted is None

    async def release(self):
        self.released = True
        await self.session.close()

    async def abort(self, reason):
        self.aborted = reason
        await self.session.close()


def test_provider_registry_normalizes_aliases_and_rejects_duplicates():
    clear_provider_registry()
    provider = _FakeProvider()

    register_provider(provider)

    assert registered_providers() == ("postgresql",)
    assert get_provider("postgres") is provider
    assert get_provider("postgresql") is provider
    assert require_provider("postgres") is provider
    with pytest.raises(ValueError, match="already registered"):
        register_provider(provider)
    with pytest.raises(ReadPointUnavailable, match="mssql"):
        require_provider("mssql")
    clear_provider_registry()
    register_builtin_providers()


async def test_fake_provider_acquire_join_release_and_abort_lifecycle():
    provider = _FakeProvider()
    await provider.validate("warehouse", "warehouse::sales.orders")
    owned = await provider.acquire_owner(
        connection_id="warehouse",
        table_id="warehouse::sales.orders",
        generation_id="generation-1",
        generation_fence=4,
        owner_shard=0,
    )

    assert provider.validated == [("warehouse", "warehouse::sales.orders")]
    assert await owned.healthy() is True
    restored = ReadPointDescriptor.from_dict(owned.descriptor.to_state_dict())
    joined = await provider.join(restored)
    assert joined.connection_id == "warehouse"

    await joined.close()
    await owned.release()
    assert joined.closed is True
    assert owned.released is True
    assert await owned.healthy() is False

    aborted = await provider.acquire_owner(
        connection_id="warehouse",
        table_id="warehouse::sales.orders",
        generation_id="generation-2",
        generation_fence=5,
        owner_shard=0,
    )
    await aborted.abort("test failure")
    assert aborted.aborted == "test failure"
    assert aborted.session.closed is True


class _PlannerSession:
    connection_id = "default"

    def __init__(self):
        self.calls = []

    async def fetch_table_row_count(self, source_table):
        self.calls.append(("count", source_table))
        return 400

    async def fetch_key_bounds(self, source_table, key_column):
        self.calls.append(("key_bounds", source_table, key_column))
        return 1, 100

    async def fetch_column_bounds(self, source_table, key_column):
        raise AssertionError("not used")

    async def fetch_key_histogram_bounds(self, source_table, key_column, n):
        raise AssertionError("not used")

    async def fetch_key_quantile_bounds(
        self, source_table, key_column, n, *, sample_rows=0, key_is_integer=False
    ):
        raise AssertionError("not used")


def _range_table(**overrides) -> TableDef:
    values = {
        "name": "orders",
        "source_table": "sales.orders",
        "schema": [
            ColumnDef(1, "id", "long", nullable=False),
            ColumnDef(2, "amount", "decimal(12,2)"),
        ],
        "key_column": "id",
        "num_splits": 2,
        "split_target_rows": 100,
        "split_strategy": "range",
    }
    values.update(overrides)
    return TableDef(**values)


async def test_planner_uses_injected_read_session_for_count_and_bounds():
    session = _PlannerSession()
    table = _range_table()

    assert await choose_table_num_splits(table, session) == 4

    table.num_splits = 2
    snap = build_table_snapshot(table, "bucket", "warehouse")
    assert await plan_ranges_for_snapshot(snap, session) is True

    assert session.calls == [
        ("count", "sales.orders"),
        ("key_bounds", "sales.orders", "id"),
    ]
    assert [(split.key_lo, split.key_hi) for split in snap.splits] == [
        (1, 51),
        (51, 101),
    ]


async def test_best_effort_session_delegates_to_existing_executor(monkeypatch):
    import db.executor as executor

    calls = []

    async def fake_count(source_table, connection="default"):
        calls.append((source_table, connection))
        return 25

    monkeypatch.setattr(executor, "fetch_table_row_count", fake_count)
    session = BestEffortReadSession("warehouse")

    assert await session.fetch_table_row_count("sales.orders") == 25
    assert calls == [("sales.orders", "warehouse")]


class _FakeResult:
    def __init__(self, scalar=None, rows=()):
        self._scalar = scalar
        self._rows = list(rows)

    def scalar(self):
        return self._scalar

    def first(self):
        return self._rows[0] if self._rows else None

    def all(self):
        return list(self._rows)

    def keys(self):
        return []

    def fetchall(self):
        return list(self._rows)


class _FakeTransaction:
    def __init__(self):
        self.is_active = True
        self.committed = False
        self.rolled_back = False

    async def commit(self):
        self.is_active = False
        self.committed = True

    async def rollback(self):
        self.is_active = False
        self.rolled_back = True


class _FakeConnection:
    def __init__(self, snapshot_token="00000003-0000001B-1"):
        self.snapshot_token = snapshot_token
        self.isolation_level = None
        self.driver_sql = []
        self.transaction = _FakeTransaction()
        self.closed = False

    async def execution_options(self, **options):
        self.isolation_level = options.get("isolation_level")
        return self

    async def begin(self):
        return self.transaction

    async def execute(self, statement, params=None):
        sql = str(statement)
        if "pg_export_snapshot" in sql:
            return _FakeResult(scalar=self.snapshot_token)
        if "SELECT 1" in sql:
            return _FakeResult(scalar=1)
        return _FakeResult()

    async def exec_driver_sql(self, sql):
        self.driver_sql.append(sql)
        return _FakeResult()

    async def close(self):
        self.closed = True


class _AwaitableConnection:
    def __init__(self, connection):
        self.connection = connection

    def __await__(self):
        async def value():
            return self.connection

        return value().__await__()

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, *_args):
        return None


class _FakeEngine:
    def __init__(self):
        self.connections = []

    def connect(self):
        connection = _FakeConnection()
        self.connections.append(connection)
        return _AwaitableConnection(connection)


async def test_sql_server_provider_owns_one_snapshot_transaction(monkeypatch):
    import db.executor as executor

    provider = SqlServerSnapshotProvider()
    engine = _FakeEngine()

    async def validated(_connection_id, _table_id):
        return None

    monkeypatch.setattr(provider, "validate", validated)
    monkeypatch.setattr(executor, "_engine_for", lambda _connection: engine)

    owned = await provider.acquire_owner(
        connection_id="warehouse",
        table_id="warehouse::sales.orders",
        generation_id="generation-1",
        generation_fence=4,
        owner_shard=0,
    )

    assert owned.descriptor.provider == "mssql_transaction"
    assert owned.descriptor.token is None
    assert engine.connections[0].isolation_level == "SNAPSHOT"
    assert await owned.healthy() is True
    with pytest.raises(ReadPointUnavailable, match="owner-only"):
        await provider.join(owned.descriptor)
    await owned.release()
    assert engine.connections[0].transaction.committed is True
    assert engine.connections[0].closed is True


async def test_postgres_provider_exports_and_imports_snapshot(monkeypatch):
    import db.executor as executor

    provider = PostgresSnapshotProvider()
    engine = _FakeEngine()
    monkeypatch.setattr(executor, "_async_mode_for", lambda _connection: True)
    monkeypatch.setattr(executor, "_engine_for", lambda _connection: engine)

    owned = await provider.acquire_owner(
        connection_id="warehouse",
        table_id="warehouse::sales.orders",
        generation_id="generation-1",
        generation_fence=4,
        owner_shard=0,
    )
    joined = await provider.join(owned.descriptor)

    assert owned.descriptor.provider == "postgresql_exported"
    assert owned.descriptor.token == "00000003-0000001B-1"
    assert engine.connections[0].isolation_level == "REPEATABLE READ"
    assert engine.connections[0].driver_sql == ["SET TRANSACTION READ ONLY"]
    assert engine.connections[1].driver_sql == [
        "SET TRANSACTION READ ONLY",
        "SET TRANSACTION SNAPSHOT '00000003-0000001B-1'",
    ]
    await joined.close()
    await owned.release()
    assert all(connection.closed for connection in engine.connections)


async def test_postgres_provider_rejects_invalid_snapshot_identifier(monkeypatch):
    import db.executor as executor

    provider = PostgresSnapshotProvider()
    engine = _FakeEngine()

    def invalid_connect():
        connection = _FakeConnection(snapshot_token="invalid snapshot; DROP TABLE")
        engine.connections.append(connection)
        return _AwaitableConnection(connection)

    engine.connect = invalid_connect
    monkeypatch.setattr(executor, "_async_mode_for", lambda _connection: True)
    monkeypatch.setattr(executor, "_engine_for", lambda _connection: engine)

    with pytest.raises(Exception, match="invalid snapshot identifier"):
        await provider.acquire_owner(
            connection_id="warehouse",
            table_id="warehouse::sales.orders",
            generation_id="generation-1",
            generation_fence=4,
            owner_shard=0,
        )
    assert engine.connections[0].transaction.rolled_back is True
    assert engine.connections[0].closed is True


async def test_transactional_session_fails_after_read_point_expiry(monkeypatch):
    import db.read_points as read_points

    connection = _FakeConnection()
    session = read_points.TransactionalReadSession(
        "warehouse",
        connection,
        connection.transaction,
        flavor="mssql",
        expires_at_ms=2000,
    )
    monkeypatch.setattr(read_points.time, "time", lambda: 2.0)

    with pytest.raises(read_points.ReadPointExpired, match="expired"):
        await session.execute_scalar("SELECT 1")


class _RetrySession:
    connection_id = "warehouse"

    def __init__(self, *, fail_execute=False, fail_stream_after_yield=False):
        self.fail_execute = fail_execute
        self.fail_stream_after_yield = fail_stream_after_yield
        self.closed = False

    async def execute_split_query(self, sql, params, split_index):
        if self.fail_execute:
            raise OSError("connection reset")
        return [{"id": 1}]

    async def _stream(self):
        yield [{"id": 1}]
        if self.fail_stream_after_yield:
            raise OSError("connection reset")

    def stream_split_query(self, sql, params, split_index, *, batch_rows):
        return self._stream()

    async def close(self, *, commit=True):
        self.closed = True


async def test_postgres_join_retries_with_same_snapshot_before_rows(monkeypatch):
    import db.executor as executor

    provider = PostgresSnapshotProvider()
    descriptor = _descriptor(
        provider="postgresql_exported",
        token="00000003-0000001B-1",
    )
    sessions = [
        _RetrySession(fail_execute=True),
        _RetrySession(),
    ]
    descriptors = []

    async def join_once(value):
        descriptors.append(value)
        return sessions[len(descriptors) - 1]

    monkeypatch.setattr(provider, "_join_transaction", join_once)
    monkeypatch.setattr(executor, "_max_retries_for", lambda _connection: 1)
    monkeypatch.setattr(executor, "_retry_backoff_for", lambda _connection: 0)

    joined = await provider.join(descriptor)
    rows = await joined.execute_split_query("SELECT 1", {}, 0)

    assert rows == [{"id": 1}]
    assert descriptors == [descriptor, descriptor]
    assert sessions[0].closed is True


async def test_postgres_join_does_not_retry_after_yield(monkeypatch):
    import db.executor as executor

    provider = PostgresSnapshotProvider()
    descriptor = _descriptor(
        provider="postgresql_exported",
        token="00000003-0000001B-1",
    )
    session = _RetrySession(fail_stream_after_yield=True)
    joins = 0

    async def join_once(_value):
        nonlocal joins
        joins += 1
        return session

    monkeypatch.setattr(provider, "_join_transaction", join_once)
    monkeypatch.setattr(executor, "_max_retries_for", lambda _connection: 2)
    monkeypatch.setattr(executor, "_retry_backoff_for", lambda _connection: 0)

    joined = await provider.join(descriptor)
    batches = []
    with pytest.raises(OSError, match="connection reset"):
        async for batch in joined.stream_split_query(
            "SELECT 1", {}, 0, batch_rows=10
        ):
            batches.extend(batch)

    assert batches == [{"id": 1}]
    assert joins == 1

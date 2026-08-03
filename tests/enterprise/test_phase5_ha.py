"""Phase 5: robustness & Manager HA — leader lease, retention GC, rolling restart."""
from __future__ import annotations

import os

os.environ.setdefault("DB_URL", "sqlite+aiosqlite:///:memory:")
os.environ.setdefault("S3_BUCKET", "test-bucket")

from config import ColumnDef, TableDef
from enterprise.control.lease import LeaderLease
from enterprise.control.rolling import rolling_restart
from runtime.artifact_store import MemoryStore


# ---------------------------------------------------------------------------
# Leader lease (Manager failover primitive)
# ---------------------------------------------------------------------------

def test_lease_acquire_and_renew():
    a = LeaderLease(MemoryStore(), "A", ttl_ms=1000)
    assert a.acquire_or_renew(now_ms=0) is True and a.is_leader
    assert a.acquire_or_renew(now_ms=500) is True       # renew while owned


def test_lease_standby_blocked_then_takeover():
    store = MemoryStore()
    a = LeaderLease(store, "A", ttl_ms=1000)
    b = LeaderLease(store, "B", ttl_ms=1000)
    assert a.acquire_or_renew(now_ms=0) is True
    assert b.acquire_or_renew(now_ms=500) is False       # A holds & fresh -> B waits
    assert not b.is_leader
    # A stops renewing; lease expires (now - renew > ttl) -> B takes over
    assert b.acquire_or_renew(now_ms=1500) is True and b.is_leader
    # A discovers it lost leadership
    assert a.acquire_or_renew(now_ms=1600) is False and not a.is_leader


def test_lease_release_frees_it():
    store = MemoryStore()
    a = LeaderLease(store, "A", ttl_ms=1000)
    a.acquire_or_renew(now_ms=0)
    a.release()
    assert not a.is_leader
    b = LeaderLease(store, "B", ttl_ms=1000)
    assert b.acquire_or_renew(now_ms=10) is True          # free immediately after release


def test_lease_current_owner():
    store = MemoryStore()
    a = LeaderLease(store, "owner-A", ttl_ms=1000)
    assert a.current_owner() is None
    a.acquire_or_renew(now_ms=0)
    assert a.current_owner() == "owner-A"


# ---------------------------------------------------------------------------
# Retention GC
# ---------------------------------------------------------------------------

def test_gc_deletes_orphans_keeps_live(monkeypatch):
    from enterprise import retention
    store = MemoryStore()
    live = "warehouse/db/sales/data/split-0-111.parquet"
    orphan = "warehouse/db/sales/data/split-0-999.parquet"
    meta = "warehouse/db/sales/metadata/v1.metadata.json"
    for k in (live, orphan, meta):
        store.put(k, b"x")
    monkeypatch.setattr(retention, "live_object_keys", lambda: {live, meta})

    deleted = retention.gc_orphaned_data(store, warehouse_prefix="warehouse/db")
    assert deleted == [orphan]
    assert store.exists(live) and store.exists(meta)      # retained
    assert not store.exists(orphan)                        # collected


def test_gc_dry_run_reports_without_deleting(monkeypatch):
    from enterprise import retention
    store = MemoryStore()
    orphan = "warehouse/db/sales/data/split-0-999.parquet"
    store.put(orphan, b"x")
    monkeypatch.setattr(retention, "live_object_keys", lambda: set())

    deleted = retention.gc_orphaned_data(store, warehouse_prefix="warehouse/db", dry_run=True)
    assert deleted == [orphan]
    assert store.exists(orphan)                            # dry run keeps it


def test_gc_ignores_non_data_objects(monkeypatch):
    from enterprise import retention
    store = MemoryStore()
    meta_orphan = "warehouse/db/sales/metadata/old-m0.avro"
    store.put(meta_orphan, b"x")
    monkeypatch.setattr(retention, "live_object_keys", lambda: set())

    assert retention.gc_orphaned_data(store, warehouse_prefix="warehouse/db") == []
    assert store.exists(meta_orphan)                       # only /data/*.parquet collected


def test_live_object_keys_from_snapshot(monkeypatch):
    import iceberg.state_store as ss
    from enterprise.retention import live_object_keys
    monkeypatch.setattr(ss, "_snapshots", {}, raising=False)
    monkeypatch.setattr(ss, "_history", {}, raising=False)
    table = TableDef(name="sales", source_table="sales", num_splits=2, key_column="id",
                     schema=[ColumnDef(field_id=1, name="id", iceberg_type="long", nullable=False)])
    snap = ss.build_table_snapshot(table, "bucket", "warehouse/db")

    keys = live_object_keys()
    assert snap.metadata_key in keys
    assert all(s.object_key in keys for s in snap.splits)


# ---------------------------------------------------------------------------
# Rolling restart
# ---------------------------------------------------------------------------

class _FakeSup:
    def __init__(self, name, log):
        self.name = name
        self.pid = 100
        self._alive = True
        self._log = log

    @property
    def is_alive(self):
        return self._alive

    async def stop(self):
        self._alive = False
        self._log.append(f"{self.name}:stop")

    async def start(self):
        self._alive = True
        self._log.append(f"{self.name}:start")


async def test_rolling_restart_is_strictly_sequential():
    log: list[str] = []
    sups = [_FakeSup("a1", log), _FakeSup("a2", log), _FakeSup("a3", log)]
    results = await rolling_restart(sups, is_healthy=lambda n: True,
                                    health_timeout=1.0, poll=0.01)
    # each Agent fully stop->start before the next is touched (=> <=1 down at a time)
    assert log == ["a1:stop", "a1:start", "a2:stop", "a2:start", "a3:stop", "a3:start"]
    assert results == [("a1", True), ("a2", True), ("a3", True)]


async def test_rolling_restart_health_gate_times_out():
    log: list[str] = []
    sups = [_FakeSup("a1", log)]
    results = await rolling_restart(sups, is_healthy=lambda n: False,
                                    health_timeout=0.05, poll=0.01)
    assert results == [("a1", False)]                      # never went healthy


async def test_rolling_restart_deregisters_before_stop():
    log: list[str] = []
    sups = [_FakeSup("a1", log), _FakeSup("a2", log)]
    removed: list[str] = []
    await rolling_restart(sups, is_healthy=lambda n: True,
                          health_timeout=1.0, poll=0.01,
                          before_stop=removed.append)
    # each Agent is dropped from rotation just before it stops
    assert removed == ["a1", "a2"]

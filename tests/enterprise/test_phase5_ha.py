"""Phase 5: robustness & Manager HA — leader lease, retention GC, rolling restart."""
from __future__ import annotations

import os
import asyncio
import multiprocessing
import threading
import time

os.environ.setdefault("DB_URL", "sqlite+aiosqlite:///:memory:")
os.environ.setdefault("S3_BUCKET", "test-bucket")

import pytest
import httpx

from config import ColumnDef, TableDef
from enterprise.control.contract import (
    AgentHealth,
    ControlCommand,
    Drain,
    HeartbeatRequest,
    RegisterRequest,
)
from enterprise.control.lease import LeaderLease, StaleLeaderError
from enterprise.control.registry import Registry
from enterprise.control.rolling import rolling_restart
from runtime.artifact_store import LocalDirStore, MemoryStore


def _acquire_shared_lease(root, owner, now_ms, start, results):
    lease = LeaderLease(LocalDirStore(root), owner, ttl_ms=5000)
    start.wait(5)
    results.put((owner, lease.acquire_or_renew(now_ms=now_ms)))


def _active_manager_process(root, ready, hold):
    now_ms = int(time.time() * 1000)
    lease = LeaderLease(LocalDirStore(root), "manager-active", ttl_ms=1000)
    if not lease.acquire_or_renew(now_ms=now_ms):
        ready.put({"error": "lease"})
        return
    registry = Registry()
    registry.attach_durable_state(lease)
    response = registry.register(_register_request("agent-kill-test"))
    registry.queue_command(
        "agent-kill-test",
        ControlCommand(kind="drain", drain=Drain(grace_ms=250)),
    )
    ready.put({
        "lease_id": response.lease_id,
        "fence": lease.fence,
        "renew_ms": now_ms,
    })
    hold.wait(30)


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
    assert b.fence == 2
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
    assert b.fence == 2


def test_lease_current_owner():
    store = MemoryStore()
    a = LeaderLease(store, "owner-A", ttl_ms=1000)
    assert a.current_owner() is None
    a.acquire_or_renew(now_ms=0)
    assert a.current_owner() == "owner-A"


def test_lease_race_has_one_winner():
    store = MemoryStore()
    leases = [LeaderLease(store, owner, ttl_ms=1000) for owner in ("A", "B")]
    barrier = threading.Barrier(2)
    results: list[bool] = []

    def acquire(lease):
        barrier.wait()
        results.append(lease.acquire_or_renew(now_ms=100))

    threads = [threading.Thread(target=acquire, args=(lease,)) for lease in leases]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert sorted(results) == [False, True]
    assert sum(lease.is_leader for lease in leases) == 1


def test_local_store_compare_and_swap_is_shared_between_instances(tmp_path):
    first = LocalDirStore(str(tmp_path))
    second = LocalDirStore(str(tmp_path))
    assert first.compare_and_swap("control/state.json", None, b"one")
    assert not second.compare_and_swap("control/state.json", None, b"two")
    assert second.compare_and_swap("control/state.json", b"one", b"two")
    assert first.get("control/state.json") == b"two"


def test_local_store_lease_has_one_cross_process_winner(tmp_path):
    context = multiprocessing.get_context("spawn")
    start = context.Event()
    results = context.Queue()
    now_ms = int(time.time() * 1000)
    processes = [
        context.Process(
            target=_acquire_shared_lease,
            args=(str(tmp_path), owner, now_ms, start, results),
        )
        for owner in ("manager-a", "manager-b")
    ]
    for process in processes:
        process.start()
    start.set()
    acquired = [results.get(timeout=10) for _process in processes]
    for process in processes:
        process.join(timeout=10)
        assert process.exitcode == 0
    assert sorted(won for _owner, won in acquired) == [False, True]


def test_standby_recovers_registry_after_active_process_is_killed(tmp_path):
    context = multiprocessing.get_context("spawn")
    ready = context.Queue()
    hold = context.Event()
    active = context.Process(
        target=_active_manager_process,
        args=(str(tmp_path), ready, hold),
    )
    active.start()
    durable = ready.get(timeout=10)
    assert "error" not in durable
    active.terminate()
    active.join(timeout=10)
    assert active.exitcode is not None

    standby = LeaderLease(
        LocalDirStore(str(tmp_path)),
        "manager-standby",
        ttl_ms=1000,
    )
    assert standby.acquire_or_renew(now_ms=durable["renew_ms"] + 1500)
    recovered = Registry()
    recovered.attach_durable_state(standby)
    record = recovered.get("agent-kill-test")

    assert standby.fence == durable["fence"] + 1
    assert record is not None
    assert record.lease_id == durable["lease_id"]
    assert record.draining is True
    assert [command.kind for command in record.commands] == ["drain"]


def test_stale_leader_cannot_validate_after_takeover():
    store = MemoryStore()
    first = LeaderLease(store, "A", ttl_ms=1000)
    second = LeaderLease(store, "B", ttl_ms=1000)
    assert first.acquire_or_renew(now_ms=0)
    first_fence = first.fence
    assert second.acquire_or_renew(now_ms=1500)

    with pytest.raises(StaleLeaderError):
        first.validate(owner_id="A", fence=first_fence, now_ms=1600)
    assert second.validate(now_ms=1600)["fence"] == second.fence


def test_lease_status_exposes_term_and_age():
    store = MemoryStore()
    lease = LeaderLease(store, "manager-A", ttl_ms=1000)
    standby = LeaderLease(store, "manager-B", ttl_ms=1000)
    assert lease.acquire_or_renew(now_ms=100)
    standby.publish_presence("standby", now_ms=300)
    status = lease.status(now_ms=350)
    assert status["available"] is True
    assert status["degraded"] is False
    assert status["owner_id"] == "manager-A"
    assert status["fence"] == 1
    assert status["lease_age_ms"] == 250
    assert status["remaining_ms"] == 750
    assert status["healthy_standbys"] == 1


def test_lease_status_is_degraded_without_healthy_standby():
    lease = LeaderLease(MemoryStore(), "manager-A", ttl_ms=1000)
    assert lease.acquire_or_renew(now_ms=100)
    status = lease.status(now_ms=350)
    assert status["degraded"] is True
    assert status["healthy_standbys"] == 0
    assert "no healthy standby" in status["reason"]


def test_lease_status_reports_degraded_store():
    class UnavailableStore(MemoryStore):
        def get(self, key, *, offset=0, length=None):
            raise OSError("shared store unavailable")

    lease = LeaderLease(UnavailableStore(), "manager-A", ttl_ms=1000)
    status = lease.status()

    assert status["available"] is False
    assert status["degraded"] is True
    assert status["expired"] is True
    assert "unavailable" in status["reason"]


def _register_request(agent_id: str = "agent-1") -> RegisterRequest:
    return RegisterRequest(
        agent_id=agent_id,
        host="127.0.0.1",
        port=9000,
        os="linux",
        version="test",
    )


def test_registry_and_pending_commands_survive_takeover():
    store = MemoryStore()
    first = LeaderLease(store, "manager-A", ttl_ms=1000)
    started = int(time.time() * 1000)
    assert first.acquire_or_renew(now_ms=started)
    registry = Registry()
    registry.attach_durable_state(first)
    response = registry.register(_register_request())
    assert registry.queue_command(
        "agent-1",
        ControlCommand(kind="drain", drain=Drain(grace_ms=1000)),
    )

    second = LeaderLease(store, "manager-B", ttl_ms=1000)
    assert second.acquire_or_renew(now_ms=started + 1500)
    recovered = Registry()
    recovered.attach_durable_state(second)
    record = recovered.get("agent-1")

    assert record is not None
    assert record.lease_id == response.lease_id
    assert record.draining is True
    commands = recovered.heartbeat(
        HeartbeatRequest(
            agent_id="agent-1",
            lease_id=response.lease_id,
            health=AgentHealth(inflight=1),
        )
    )
    assert [command.kind for command in commands] == ["drain"]


def test_stale_registry_mutation_is_rolled_back_after_takeover():
    store = MemoryStore()
    first = LeaderLease(store, "manager-A", ttl_ms=1000)
    started = int(time.time() * 1000)
    assert first.acquire_or_renew(now_ms=started)
    registry = Registry()
    registry.attach_durable_state(first)
    registry.register(_register_request("original"))

    second = LeaderLease(store, "manager-B", ttl_ms=1000)
    assert second.acquire_or_renew(now_ms=started + 1500)

    with pytest.raises(StaleLeaderError):
        registry.register(_register_request("stale-write"))
    assert registry.get("stale-write") is None
    assert registry.get("original") is not None


async def test_manager_health_exposes_term_and_store_degradation(monkeypatch):
    import config
    from enterprise.control.manager_app import create_manager_app
    from runtime.artifact_store import reset_default_store, set_default_store

    store = MemoryStore()
    set_default_store(store)
    monkeypatch.setattr(config, "MANAGER_HA", True)
    monkeypatch.setattr(config, "MANAGER_SUPERVISION_MODE", "external")
    monkeypatch.setattr(config, "LEADER_LEASE_RENEW_MS", 20)
    monkeypatch.setattr(config, "LEADER_LEASE_TTL_MS", 200)
    monkeypatch.setattr(config, "MATERIALIZATION_WORK_QUEUE", False)
    monkeypatch.setattr(config, "OPEN_MIRROR_PUBLISH", False)
    monkeypatch.setattr(config, "ENABLE_GATEWAY", False)
    monkeypatch.setattr(config, "ENABLE_ADMIN_UI", False)
    monkeypatch.setattr(config, "ENABLE_MONITOR", False)
    monkeypatch.setenv("AGENT_AUTH_MODE", "required")
    monkeypatch.setenv("AGENT_TOKEN", "a" * 64)
    monkeypatch.delenv("KUBERNETES_SERVICE_HOST", raising=False)
    app = create_manager_app()

    try:
        async with app.router.lifespan_context(app):
            for _attempt in range(50):
                if app.state.is_leader:
                    break
                await asyncio.sleep(0.01)
            assert app.state.is_leader
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://manager",
            ) as client:
                healthy = await client.get("/healthz")
                assert healthy.status_code == 200
                assert healthy.json()["ha"]["fence"] == 1
                assert healthy.json()["ha"]["owner_id"]

                def unavailable(*_args, **_kwargs):
                    raise OSError("shared store unavailable")

                monkeypatch.setattr(store, "get", unavailable)
                degraded = await client.get("/healthz")
                assert degraded.json()["status"] == "degraded"
                assert degraded.json()["ha"]["available"] is False
                assert (await client.get("/readyz")).status_code == 503
    finally:
        reset_default_store()


async def test_manager_rejects_mutations_until_takeover_recovery_finishes(
    monkeypatch,
):
    import config
    from enterprise.control.manager_app import create_manager_app
    from runtime.artifact_store import reset_default_store, set_default_store

    store = MemoryStore()
    set_default_store(store)
    monkeypatch.setattr(config, "MANAGER_HA", True)
    monkeypatch.setattr(config, "MANAGER_SUPERVISION_MODE", "external")
    monkeypatch.setattr(config, "LEADER_LEASE_RENEW_MS", 20)
    monkeypatch.setattr(config, "LEADER_LEASE_TTL_MS", 500)
    monkeypatch.setattr(config, "MATERIALIZATION_WORK_QUEUE", False)
    monkeypatch.setattr(config, "OPEN_MIRROR_PUBLISH", False)
    monkeypatch.setattr(config, "ENABLE_GATEWAY", False)
    monkeypatch.setattr(config, "ENABLE_ADMIN_UI", False)
    monkeypatch.setattr(config, "ENABLE_MONITOR", False)
    monkeypatch.setenv("AGENT_AUTH_MODE", "required")
    monkeypatch.setenv("AGENT_TOKEN", "a" * 64)
    monkeypatch.delenv("KUBERNETES_SERVICE_HOST", raising=False)
    entered = threading.Event()
    resume = threading.Event()
    original_attach = Registry.attach_durable_state

    def blocking_attach(self, lease, *, restore=True):
        entered.set()
        assert resume.wait(5)
        return original_attach(self, lease, restore=restore)

    monkeypatch.setattr(Registry, "attach_durable_state", blocking_attach)
    app = create_manager_app()
    try:
        async with app.router.lifespan_context(app):
            assert await asyncio.wait_for(
                asyncio.to_thread(entered.wait, 2),
                timeout=3,
            )
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://manager",
            ) as client:
                response = await client.post(
                    "/control/register",
                    json=_register_request("activation-race").to_dict(),
                    headers={
                        "x-fsp-agent-id": "activation-race",
                        "x-fsp-agent-token": "a" * 64,
                    },
                )
            assert response.status_code == 503
            assert response.json()["error"] == "not_primary"
            assert app.state.registry.get("activation-race") is None
            resume.set()
            for _attempt in range(100):
                if app.state.primary_ready:
                    break
                await asyncio.sleep(0.01)
            assert app.state.primary_ready
    finally:
        resume.set()
        reset_default_store()


async def test_open_mirror_scheduler_does_not_start_on_standby(monkeypatch):
    import config
    from enterprise.control.manager_app import create_manager_app
    from open_mirror.scheduler import OpenMirrorScheduler
    from runtime.artifact_store import reset_default_store, set_default_store

    store = MemoryStore()
    active = LeaderLease(store, "other-manager", ttl_ms=5000)
    assert active.acquire_or_renew()
    set_default_store(store)
    monkeypatch.setattr(config, "MANAGER_HA", True)
    monkeypatch.setattr(config, "MANAGER_SUPERVISION_MODE", "external")
    monkeypatch.setattr(config, "LEADER_LEASE_RENEW_MS", 20)
    monkeypatch.setattr(config, "LEADER_LEASE_TTL_MS", 5000)
    monkeypatch.setattr(config, "MATERIALIZATION_WORK_QUEUE", False)
    monkeypatch.setattr(config, "OPEN_MIRROR_PUBLISH", True)
    monkeypatch.setattr(config, "ENABLE_GATEWAY", False)
    monkeypatch.setattr(config, "ENABLE_ADMIN_UI", False)
    monkeypatch.setattr(config, "ENABLE_MONITOR", False)
    monkeypatch.delenv("KUBERNETES_SERVICE_HOST", raising=False)
    starts = []
    monkeypatch.setattr(
        OpenMirrorScheduler,
        "start",
        lambda self: starts.append(self),
    )
    app = create_manager_app()

    try:
        async with app.router.lifespan_context(app):
            await asyncio.sleep(0.1)
            assert app.state.is_leader is False
            assert app.state.primary_ready is False
            assert starts == []
    finally:
        reset_default_store()


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


async def test_rolling_restart_reports_durable_progress_events():
    events = []
    supervisors = [_FakeSup("a1", []), _FakeSup("a2", [])]
    await rolling_restart(
        supervisors,
        is_healthy=lambda _name: True,
        health_timeout=1.0,
        poll=0.01,
        on_event=lambda event, agent, healthy: events.append(
            (event, agent, healthy)
        ),
    )

    assert events == [
        ("restarting", "a1", False),
        ("restarted", "a1", True),
        ("restarting", "a2", False),
        ("restarted", "a2", True),
        ("complete", "", True),
    ]

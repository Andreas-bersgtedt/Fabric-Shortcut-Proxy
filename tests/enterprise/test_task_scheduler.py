from __future__ import annotations

import asyncio
import hashlib
import io
import time

import pyarrow as pa
import pyarrow.parquet as pq

from enterprise.control.contract import (
    AgentHealth,
    ControlCommand,
    Drain,
    HeartbeatRequest,
    MaterializeTask,
    RegisterRequest,
    SnapshotManifest,
    SplitRef,
    TaskResult,
    TASK_CLAIMED,
    TASK_RETRY_WAIT,
)
from enterprise.control.registry import Registry
from enterprise.control.lease import LeaderLease
from enterprise.control.task_scheduler import TaskScheduler
from enterprise.control.work_queue import DurableWorkQueue
from runtime.artifact_store import MemoryStore


def _register(
    registry: Registry,
    agent_id: str,
    capabilities: list[str],
    *,
    shard_index: int = -1,
    capacity_hint: int = 0,
):
    return registry.register(
        RegisterRequest(
            agent_id=agent_id,
            host="127.0.0.1",
            port=9000 + len(registry.list_public()),
            os="linux",
            version="test",
            capabilities=capabilities,
            shard_index=shard_index,
            capacity_hint=capacity_hint,
        )
    )


def _successful_result(claimed: MaterializeTask, data: bytes) -> TaskResult:
    return TaskResult(
        agent_id="",
        table=claimed.table,
        epoch=claimed.epoch,
        split_index=claimed.split_index,
        ok=True,
        size_bytes=len(data),
        record_count=2,
        content_hash=hashlib.sha256(data).hexdigest(),
        task_id=claimed.task_id,
        request_id=claimed.request_id,
        claim_token=claimed.claim_token,
        attempt=claimed.attempt,
        generation_id=claimed.generation_id,
        generation_fence=claimed.generation_fence,
        plan_sha256=claimed.plan_sha256,
        membership_version=claimed.membership_version,
        worker_fence=claimed.worker_fence,
    )


def _queue(task_count: int = 1) -> tuple[DurableWorkQueue, dict]:
    queue = DurableWorkQueue(MemoryStore())
    tasks = [
        MaterializeTask(
            table="sales",
            epoch=1,
            split_index=index,
            source_table="sales",
            output_key=f"warehouse/sales/{index}.parquet",
        )
        for index in range(task_count)
    ]
    request = queue.create_request(
        requested_key="warehouse/sales/metadata.json",
        table="sales",
        epoch=1,
        table_format="iceberg",
        generation_id="generation-1",
        generation_fence=1,
        plan_sha256="a" * 64,
        tasks=tasks,
        deadline_ms=int(time.time() * 1000) + 300_000,
    )
    return queue, request


def test_scheduler_filters_capabilities_dead_and_draining_agents():
    queue, request = _queue()
    registry = Registry(heartbeat_ms=1000, miss_limit=1)
    _register(registry, "serving", ["serving"])
    dead = _register(registry, "dead", ["materializer"])
    _register(registry, "draining", ["materializer"])
    healthy = _register(registry, "healthy", ["materializer"])
    registry.get("dead").last_seen -= 10
    registry.queue_command(
        "draining", ControlCommand(kind="drain", drain=Drain())
    )

    scheduler = TaskScheduler(
        queue,
        registry,
        manager_owner="manager",
        manager_fence=1,
        max_inflight_per_agent=1,
    )
    assert scheduler.dispatch_once() == 1
    task = queue.get_task(request["task_ids"][0])
    assert task["state"] == TASK_CLAIMED
    assert task["claim"]["agent_id"] == "healthy"
    commands = registry.heartbeat(
        HeartbeatRequest(agent_id="healthy", lease_id=healthy.lease_id)
    )
    assert len(commands) == 1 and commands[0].kind == "materialize"
    assert dead.lease_id


def test_scheduler_stable_tie_break_and_inflight_limit():
    queue, request = _queue(task_count=2)
    registry = Registry()
    a = _register(registry, "agent-a", ["materializer"])
    b = _register(registry, "agent-b", ["materializer"])
    scheduler = TaskScheduler(
        queue,
        registry,
        manager_owner="manager",
        manager_fence=9,
        max_inflight_per_agent=1,
    )
    expected_first = min(
        ("agent-a", "agent-b"),
        key=lambda agent: scheduler._tie_break(request["task_ids"][0], agent),
    )
    assert scheduler.dispatch_once() == 2
    claims = {
        queue.get_task(task_id)["claim"]["agent_id"]
        for task_id in request["task_ids"]
    }
    assert claims == {"agent-a", "agent-b"}
    assert queue.get_task(request["task_ids"][0])["claim"]["agent_id"] == expected_first
    assert a.lease_id and b.lease_id


def test_scheduler_filters_owner_only_snapshot_agents():
    queue, request = _queue(task_count=2)
    registry = Registry()
    _register(
        registry, "agent-0", ["materializer"], shard_index=0
    )
    owner = _register(
        registry, "agent-1", ["materializer"], shard_index=1
    )
    scheduler = TaskScheduler(
        queue,
        registry,
        manager_owner="manager",
        manager_fence=1,
    )

    eligible = scheduler._eligible_agents(required_owner_shard=1)
    assert [agent_id for _, agent_id in eligible] == ["agent-1"]
    queue.claim_task(
        request["task_ids"][0],
        agent_id="agent-1",
        agent_lease_id=owner.lease_id,
        manager_owner="manager",
        manager_fence=1,
    )
    assert scheduler._eligible_agents(
        active_claims={"agent-1": 1},
        required_owner_shard=1,
    ) == []


def test_scheduler_releases_claim_when_command_delivery_fails(monkeypatch):
    queue, request = _queue()
    registry = Registry()
    _register(registry, "agent-1", ["materializer"])
    scheduler = TaskScheduler(
        queue,
        registry,
        manager_owner="manager",
        manager_fence=1,
    )
    monkeypatch.setattr(registry, "queue_command", lambda *_args: False)

    assert scheduler.dispatch_once() == 0
    task = queue.get_task(request["task_ids"][0])
    assert task["state"] == TASK_RETRY_WAIT
    assert task["claim"] is None


def test_scheduler_cancels_tasks_from_fenced_generation():
    from runtime.generation import acquire_generation

    queue, request = _queue()
    acquire_generation(queue.store, shard_count=1)
    registry = Registry()
    _register(registry, "agent-1", ["materializer"])
    scheduler = TaskScheduler(
        queue,
        registry,
        manager_owner="manager",
        manager_fence=1,
    )

    assert scheduler.dispatch_once() == 0
    assert queue.get_request(request["request_id"])["state"] == "CANCELLED"


def test_heartbeat_renews_claim_via_control_service(monkeypatch):
    from enterprise.control.server import ControlService
    import enterprise.control.work_queue as work_queue

    queue, request = _queue()
    registry = Registry()
    lease = _register(registry, "agent-1", ["materializer"])
    now_ms = int(queue.get_task(request["task_ids"][0])["available_at_ms"])
    claimed = queue.claim_task(
        request["task_ids"][0],
        agent_id="agent-1",
        agent_lease_id=lease.lease_id,
        manager_owner="manager",
        manager_fence=1,
        now_ms=now_ms,
    )
    original_expiry = claimed.claim_expires_at_ms
    assert queue.renew_agent_claims(
        "agent-1",
        lease.lease_id,
        task_ids=[],
        now_ms=now_ms + 500,
    ) == 0
    assert (
        queue.get_task(request["task_ids"][0])["claim"]["expires_at_ms"]
        == original_expiry
    )
    monkeypatch.setattr(work_queue, "_now_ms", lambda: now_ms + 1_000)
    service = ControlService(registry, work_queue=queue)
    service.heartbeat(
        HeartbeatRequest(
            agent_id="agent-1",
            lease_id=lease.lease_id,
            health=AgentHealth(),
            active_task_ids=[claimed.task_id],
        )
    )
    renewed = queue.get_task(request["task_ids"][0])
    assert renewed["claim"]["expires_at_ms"] > original_expiry


async def test_scheduler_store_scan_does_not_block_event_loop(monkeypatch):
    queue, _ = _queue()
    scheduler = TaskScheduler(
        queue,
        Registry(),
        manager_owner="manager",
        manager_fence=1,
        scan_interval_seconds=1,
    )

    def slow_dispatch():
        time.sleep(0.2)
        return 0

    monkeypatch.setattr(scheduler, "dispatch_once", slow_dispatch)
    started = time.monotonic()
    scheduler.start()
    try:
        await asyncio.sleep(0.03)
        assert time.monotonic() - started < 0.15
    finally:
        await scheduler.stop()


def test_standby_takeover_fences_claim_and_publishes_once():
    store = MemoryStore()
    queue = DurableWorkQueue(store)
    task = MaterializeTask(
        table="sales",
        epoch=1,
        split_index=0,
        source_table="sales",
        output_key="warehouse/sales/0.parquet",
    )
    request = queue.create_request(
        requested_key="warehouse/sales/metadata.json",
        table="sales",
        epoch=1,
        table_format="iceberg",
        generation_id="generation-1",
        generation_fence=1,
        plan_sha256="a" * 64,
        tasks=[task],
        deadline_ms=int(time.time() * 1000) + 60_000,
    )
    registry = Registry()
    lease = _register(registry, "agent-1", ["materializer"])
    primary = LeaderLease(store, "manager-a", ttl_ms=1_000)
    standby = LeaderLease(store, "manager-b", ttl_ms=1_000)
    assert primary.acquire_or_renew(now_ms=0)
    scheduler_a = TaskScheduler(
        queue,
        registry,
        manager_owner=primary.owner_id,
        manager_fence=primary.fence,
    )
    assert scheduler_a.dispatch_once() == 1
    first = registry.heartbeat(
        HeartbeatRequest(agent_id="agent-1", lease_id=lease.lease_id)
    )[0].materialize

    assert standby.acquire_or_renew(now_ms=1_500)
    assert queue.fence_claims(standby.owner_id, standby.fence) == 1
    scheduler_b = TaskScheduler(
        queue,
        registry,
        manager_owner=standby.owner_id,
        manager_fence=standby.fence,
    )
    assert scheduler_b.dispatch_once() == 1
    second = registry.heartbeat(
        HeartbeatRequest(agent_id="agent-1", lease_id=lease.lease_id)
    )[0].materialize
    assert first.claim_token != second.claim_token

    sink = io.BytesIO()
    pq.write_table(pa.table({"id": [1, 2]}), sink)
    data = sink.getvalue()
    store.put(second.output_key, data)

    def result(claimed):
        return TaskResult(
            agent_id="agent-1",
            table=claimed.table,
            epoch=claimed.epoch,
            split_index=claimed.split_index,
            ok=True,
            size_bytes=len(data),
            record_count=2,
            content_hash=hashlib.sha256(data).hexdigest(),
            task_id=claimed.task_id,
            request_id=claimed.request_id,
            claim_token=claimed.claim_token,
            attempt=claimed.attempt,
            generation_id=claimed.generation_id,
            generation_fence=claimed.generation_fence,
            plan_sha256=claimed.plan_sha256,
            membership_version=claimed.membership_version,
            worker_fence=claimed.worker_fence,
        )

    assert not queue.accept_result(
        result(first), agent_lease_id=lease.lease_id
    ).ok
    assert queue.accept_result(
        result(second), agent_lease_id=lease.lease_id
    ).ok

    metadata_key = "warehouse/sales/metadata.json"
    store.put(metadata_key, b"{}")
    manifest = SnapshotManifest(
        table="sales",
        epoch=1,
        table_format="iceberg",
        splits=[
            SplitRef(
                object_key=second.output_key,
                size_bytes=len(data),
                record_count=2,
                content_hash=hashlib.sha256(data).hexdigest(),
            )
        ],
        metadata_keys=[metadata_key],
        generation_id="generation-1",
        generation_fence=1,
        plan_sha256="a" * 64,
        request_id=request["request_id"],
    )
    queue.publish_snapshot(request["request_id"], manifest)
    assert len(store.list("_control/work-queue/v1/snapshots/")) == 1


def test_elastic_scale_up_completes_generation_without_duplicate_splits():
    queue, request = _queue(task_count=4)
    registry = Registry()
    first_lease = _register(
        registry, "agent-a", ["materializer"], capacity_hint=1
    )
    scheduler = TaskScheduler(
        queue,
        registry,
        manager_owner="manager",
        manager_fence=1,
        max_inflight_per_agent=1,
        membership_policy="elastic",
    )
    assert scheduler.dispatch_once() == 1
    second_lease = _register(
        registry, "agent-b", ["materializer"], capacity_hint=1
    )
    assert scheduler.dispatch_once() == 1

    sink = io.BytesIO()
    pq.write_table(pa.table({"id": [1, 2]}), sink)
    data = sink.getvalue()

    def finish(agent_id, lease_id):
        commands = registry.heartbeat(
            HeartbeatRequest(agent_id=agent_id, lease_id=lease_id)
        )
        for command in commands:
            claimed = command.materialize
            queue.store.put(claimed.output_key, data)
            result = _successful_result(claimed, data)
            result.agent_id = agent_id
            assert queue.accept_result(result, agent_lease_id=lease_id).ok

    finish("agent-a", first_lease.lease_id)
    finish("agent-b", second_lease.lease_id)
    assert scheduler.dispatch_once() == 2
    finish("agent-a", first_lease.lease_id)
    finish("agent-b", second_lease.lease_id)

    assert queue.get_request(request["request_id"])["state"] == "SUCCEEDED"
    assert {
        queue.get_task(task_id)["result"]["agent_id"]
        for task_id in request["task_ids"]
    } == {"agent-a", "agent-b"}
    membership = queue.status()["membership"]
    assert membership["membership_version"] == 2
    assert membership["completed_tasks"] == 4
    assert membership["progress"] == 1.0


def test_elastic_worker_loss_reassigns_only_unfinished_and_rejects_late_result():
    queue, request = _queue(task_count=2)
    registry = Registry(heartbeat_ms=1000, miss_limit=1)
    first = _register(registry, "agent-a", ["materializer"])
    failed = _register(registry, "agent-b", ["materializer"])
    scheduler = TaskScheduler(
        queue,
        registry,
        manager_owner="manager",
        manager_fence=1,
        max_inflight_per_agent=1,
        membership_policy="elastic",
    )
    assert scheduler.dispatch_once() == 2
    first_claim = registry.heartbeat(
        HeartbeatRequest(agent_id="agent-a", lease_id=first.lease_id)
    )[0].materialize
    stale_claim = registry.heartbeat(
        HeartbeatRequest(agent_id="agent-b", lease_id=failed.lease_id)
    )[0].materialize
    sink = io.BytesIO()
    pq.write_table(pa.table({"id": [1, 2]}), sink)
    data = sink.getvalue()
    queue.store.put(first_claim.output_key, data)
    first_result = _successful_result(first_claim, data)
    first_result.agent_id = "agent-a"
    assert queue.accept_result(
        first_result, agent_lease_id=first.lease_id
    ).ok

    registry.get("agent-b").last_seen -= 10
    replacement = _register(registry, "agent-c", ["materializer"])
    assert scheduler.dispatch_once() == 1
    replacement_claim = registry.heartbeat(
        HeartbeatRequest(agent_id="agent-c", lease_id=replacement.lease_id)
    )[0].materialize
    assert replacement_claim.task_id == stale_claim.task_id
    assert replacement_claim.claim_token != stale_claim.claim_token
    stale_result = _successful_result(stale_claim, data)
    stale_result.agent_id = "agent-b"
    assert not queue.accept_result(
        stale_result, agent_lease_id=failed.lease_id
    ).ok
    queue.store.put(replacement_claim.output_key, data)
    replacement_result = _successful_result(replacement_claim, data)
    replacement_result.agent_id = "agent-c"
    assert queue.accept_result(
        replacement_result, agent_lease_id=replacement.lease_id
    ).ok

    assert queue.get_request(request["request_id"])["state"] == "SUCCEEDED"
    assert queue.status()["membership"]["reassignment_count"] == 1


def test_fixed_membership_does_not_admit_new_worker_mid_generation():
    queue, _request = _queue(task_count=2)
    registry = Registry()
    _register(registry, "agent-a", ["materializer"])
    scheduler = TaskScheduler(
        queue,
        registry,
        manager_owner="manager",
        manager_fence=1,
        max_inflight_per_agent=1,
        membership_policy="fixed",
    )
    assert scheduler.dispatch_once() == 1
    _register(registry, "agent-b", ["materializer"])
    assert scheduler.dispatch_once() == 0
    membership = queue.status()["membership"]
    assert membership["policy"] == "fixed"
    assert [worker["agent_id"] for worker in membership["workers"]] == [
        "agent-a"
    ]


def test_elastic_reregistration_fences_old_worker_during_rolling_update():
    queue, _request = _queue()
    registry = Registry()
    first = _register(registry, "agent-a", ["materializer"])
    scheduler = TaskScheduler(
        queue,
        registry,
        manager_owner="manager",
        manager_fence=1,
        max_inflight_per_agent=1,
        membership_policy="elastic",
    )
    assert scheduler.dispatch_once() == 1
    old_claim = registry.heartbeat(
        HeartbeatRequest(agent_id="agent-a", lease_id=first.lease_id)
    )[0].materialize

    replacement = _register(registry, "agent-a", ["materializer"])
    assert replacement.lease_id != first.lease_id
    assert scheduler.dispatch_once() == 1
    new_claim = registry.heartbeat(
        HeartbeatRequest(agent_id="agent-a", lease_id=replacement.lease_id)
    )[0].materialize

    assert new_claim.task_id == old_claim.task_id
    assert new_claim.claim_token != old_claim.claim_token
    assert new_claim.worker_fence > old_claim.worker_fence
    stale = _successful_result(old_claim, b"stale")
    stale.agent_id = "agent-a"
    assert not queue.accept_result(
        stale, agent_lease_id=first.lease_id
    ).ok


def test_elastic_scheduler_weights_claim_limit_by_capacity():
    queue, request = _queue(task_count=4)
    registry = Registry()
    _register(
        registry, "low-capacity", ["materializer"], capacity_hint=1
    )
    _register(
        registry, "high-capacity", ["materializer"], capacity_hint=3
    )
    scheduler = TaskScheduler(
        queue,
        registry,
        manager_owner="manager",
        manager_fence=1,
        max_inflight_per_agent=1,
        membership_policy="elastic",
    )

    assert scheduler.dispatch_once() == 4
    claims = [
        queue.get_task(task_id)["claim"]["agent_id"]
        for task_id in request["task_ids"]
    ]
    assert claims.count("low-capacity") == 1
    assert claims.count("high-capacity") == 3

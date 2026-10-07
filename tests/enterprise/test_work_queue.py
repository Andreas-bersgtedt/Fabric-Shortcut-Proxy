from __future__ import annotations

import hashlib
import io
import threading
import time

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from enterprise.control.contract import (
    Column,
    MaterializeTask,
    SnapshotManifest,
    SplitRef,
    TaskResult,
    RESULT_CONFLICT,
    RESULT_DUPLICATE,
    RESULT_INVALID_OUTPUT,
    RESULT_STALE_CLAIM,
    RESULT_STALE_GENERATION,
    RESULT_WRONG_OWNER,
    TASK_CANCELLED,
    TASK_CLAIMED,
    TASK_FAILED,
    TASK_QUEUED,
    TASK_RETRY_WAIT,
    TASK_SUCCEEDED,
)
from enterprise.control.work_queue import (
    DurableWorkQueue,
    IDEMPOTENCY_PREFIX,
    REQUESTS_PREFIX,
    TASKS_PREFIX,
    WorkQueueConflict,
    WorkQueueError,
)
from enterprise.control.lease import LeaderLease, StaleLeaderError
from fabric_shortcut_proxy.runtime.artifact_store import MemoryStore


def _parquet(values: list[int] | None = None) -> bytes:
    sink = io.BytesIO()
    pq.write_table(pa.table({"id": values or [1, 2, 3]}), sink)
    return sink.getvalue()


def _create(
    queue: DurableWorkQueue,
    *,
    deadline_ms: int | None = None,
) -> tuple[dict, str]:
    task = MaterializeTask(
        table="sales",
        epoch=7,
        split_index=0,
        source_table="sales",
        output_key="warehouse/sales/data/0.parquet",
        schema=[Column(1, "id", "long", False, "id")],
        connection_id="source",
    )
    request = queue.create_request(
        requested_key="warehouse/sales/metadata.json",
        table="sales",
        epoch=7,
        table_format="iceberg",
        generation_id="generation-4",
        generation_fence=4,
        plan_sha256="a" * 64,
        tasks=[task],
        deadline_ms=deadline_ms or int(time.time() * 1000) + 60_000,
    )
    return request, request["task_ids"][0]


def _claim(
    queue: DurableWorkQueue, task_id: str, *, now_ms: int | None = None
):
    if now_ms is None:
        now_ms = int(queue.get_task(task_id)["available_at_ms"])
    return queue.claim_task(
        task_id,
        agent_id="agent-1",
        agent_lease_id="lease-1",
        manager_owner="manager-1",
        manager_fence=3,
        now_ms=now_ms,
    )


def _result(task: MaterializeTask, data: bytes, **changes) -> TaskResult:
    values = {
        "agent_id": "agent-1",
        "table": task.table,
        "epoch": task.epoch,
        "split_index": task.split_index,
        "ok": True,
        "size_bytes": len(data),
        "record_count": 3,
        "content_hash": hashlib.sha256(data).hexdigest(),
        "task_id": task.task_id,
        "request_id": task.request_id,
        "claim_token": task.claim_token,
        "attempt": task.attempt,
        "generation_id": task.generation_id,
        "generation_fence": task.generation_fence,
        "plan_sha256": task.plan_sha256,
        "membership_version": task.membership_version,
        "worker_fence": task.worker_fence,
    }
    values.update(changes)
    return TaskResult(**values)


def test_queue_survives_restart_and_publishes_verified_snapshot():
    store = MemoryStore()
    queue = DurableWorkQueue(store)
    request, task_id = _create(queue)
    assert _create(queue)[0]["request_id"] == request["request_id"]

    claimed = _claim(queue, task_id)
    data = _parquet()
    store.put(claimed.output_key, data)
    result = _result(claimed, data)
    accepted = queue.accept_result(result, agent_lease_id="lease-1", now_ms=11_000)
    assert accepted.ok and accepted.state == TASK_SUCCEEDED
    duplicate = queue.accept_result(result, agent_lease_id="lease-1", now_ms=12_000)
    assert duplicate.ok and duplicate.duplicate
    assert duplicate.reason_code == RESULT_DUPLICATE
    conflict = queue.accept_result(
        _result(claimed, data, content_hash="0" * 64),
        agent_lease_id="lease-1",
        now_ms=12_000,
    )
    assert not conflict.ok and conflict.reason_code == RESULT_CONFLICT

    metadata_key = "warehouse/sales/metadata.json"
    store.put(metadata_key, b"{}")
    manifest = SnapshotManifest(
        table="sales",
        epoch=7,
        table_format="iceberg",
        splits=[
            SplitRef(
                object_key=claimed.output_key,
                size_bytes=len(data),
                record_count=3,
                content_hash=hashlib.sha256(data).hexdigest(),
            )
        ],
        metadata_keys=[metadata_key],
        generation_id="generation-4",
        generation_fence=4,
        plan_sha256="a" * 64,
        published_at_ms=12_000,
        request_id=request["request_id"],
    )
    queue.publish_snapshot(request["request_id"], manifest)

    restarted = DurableWorkQueue(store)
    assert restarted.get_request(request["request_id"])["state"] == TASK_SUCCEEDED
    assert restarted.get_snapshot("sales", 7) == manifest
    store.put(claimed.output_key, b"corrupt")
    with pytest.raises(WorkQueueError, match="published snapshot output"):
        restarted.get_snapshot("sales", 7)


def test_queue_records_manager_term_and_rejects_stale_leader():
    store = MemoryStore()
    started = int(time.time() * 1000)
    first = LeaderLease(store, "manager-a", ttl_ms=1000)
    assert first.acquire_or_renew(now_ms=started)
    queue = DurableWorkQueue(store, leadership_check=first.validate)
    request, _task_id = _create(queue)
    persisted = queue.get_request(request["request_id"])
    assert persisted["manager_owner"] == "manager-a"
    assert persisted["manager_fence"] == 1

    second = LeaderLease(store, "manager-b", ttl_ms=1000)
    assert second.acquire_or_renew(now_ms=started + 1500)
    with pytest.raises(StaleLeaderError):
        queue.cancel_request(request["request_id"], "stale-manager")


def test_superseded_generation_cannot_publish_completed_request():
    from fabric_shortcut_proxy.runtime.generation import acquire_generation

    store = MemoryStore()
    generation = acquire_generation(store, shard_count=1)
    queue = DurableWorkQueue(store)
    task = MaterializeTask(
        table="sales",
        epoch=7,
        split_index=0,
        source_table="sales",
        output_key="warehouse/sales/data/0.parquet",
        schema=[Column(1, "id", "long", False, "id")],
    )
    request = queue.create_request(
        requested_key="warehouse/sales/metadata.json",
        table="sales",
        epoch=7,
        table_format="iceberg",
        generation_id=generation.generation_id,
        generation_fence=generation.fence,
        plan_sha256=generation.plan_sha256,
        tasks=[task],
        deadline_ms=int(time.time() * 1000) + 60_000,
    )
    claimed = queue.claim_task(
        request["task_ids"][0],
        agent_id="agent-1",
        agent_lease_id="lease-1",
        manager_owner="manager",
        manager_fence=1,
    )
    data = _parquet()
    store.put(claimed.output_key, data)
    assert queue.accept_result(
        _result(claimed, data),
        agent_lease_id="lease-1",
    ).ok
    metadata_key = "warehouse/sales/metadata.json"
    store.put(metadata_key, b"{}")
    manifest = SnapshotManifest(
        table="sales",
        epoch=7,
        table_format="iceberg",
        splits=[SplitRef(
            object_key=claimed.output_key,
            size_bytes=len(data),
            record_count=3,
            content_hash=hashlib.sha256(data).hexdigest(),
        )],
        metadata_keys=[metadata_key],
        generation_id=generation.generation_id,
        generation_fence=generation.fence,
        plan_sha256=generation.plan_sha256,
        request_id=request["request_id"],
    )
    acquire_generation(store, shard_count=1)

    with pytest.raises(WorkQueueConflict, match="no longer active"):
        queue.publish_snapshot(request["request_id"], manifest)
    assert queue.get_request(request["request_id"])["published"] is False


def test_accepted_request_survives_manager_takeover():
    store = MemoryStore()
    started = int(time.time() * 1000)
    first = LeaderLease(store, "manager-a", ttl_ms=1000)
    assert first.acquire_or_renew(now_ms=started)
    first_queue = DurableWorkQueue(store, leadership_check=first.validate)
    request, task_id = _create(first_queue)

    second = LeaderLease(store, "manager-b", ttl_ms=1000)
    assert second.acquire_or_renew(now_ms=started + 1500)
    recovered = DurableWorkQueue(store, leadership_check=second.validate)
    recovery = recovered.recover()

    assert recovery["removed_requests"] == 0
    assert recovered.get_request(request["request_id"])["ready"] is True
    assert recovered.get_task(task_id)["state"] == TASK_QUEUED


def test_membership_fence_rejects_result_before_claim_cleanup():
    queue = DurableWorkQueue(MemoryStore())
    request, task_id = _create(queue)
    membership = queue.reconcile_membership(
        "generation-4",
        4,
        [{"agent_id": "agent-1", "lease_id": "lease-1", "capacity": 1}],
        policy="elastic",
    )
    claimed = queue.claim_task(
        task_id,
        agent_id="agent-1",
        agent_lease_id="lease-1",
        manager_owner="manager",
        manager_fence=1,
        membership_version=membership["membership_version"],
        worker_fence=membership["workers"]["agent-1"]["worker_fence"],
    )
    queue.reconcile_membership(
        "generation-4",
        4,
        [],
        policy="elastic",
    )

    assert queue.get_task(task_id)["state"] == TASK_CLAIMED
    rejected = queue.accept_result(
        _result(claimed, b"stale"),
        agent_lease_id="lease-1",
    )
    assert not rejected.ok
    assert rejected.reason_code == RESULT_STALE_CLAIM
    assert queue.get_request(request["request_id"])["state"] == TASK_CLAIMED


def test_membership_status_prefers_generation_with_nonterminal_work():
    queue = DurableWorkQueue(MemoryStore())
    _create(queue)
    workers = [{"agent_id": "agent-1", "lease_id": "lease-1", "capacity": 1}]
    queue.reconcile_membership(
        "generation-4", 4, workers, policy="elastic", now_ms=100
    )
    queue.reconcile_membership(
        "historical-generation", 1, workers, policy="elastic", now_ms=200
    )

    assert queue.status()["membership"]["generation_id"] == "generation-4"


def test_takeover_between_validation_and_queue_write_rejects_stale_commit():
    class PausingStore(MemoryStore):
        def __init__(self):
            super().__init__()
            self.pause_key = ""
            self.entered = threading.Event()
            self.resume = threading.Event()

        def fenced_put(self, fence_key, owner_id, fence, key, data):
            if key == self.pause_key:
                self.entered.set()
                assert self.resume.wait(5)
            return super().fenced_put(
                fence_key,
                owner_id,
                fence,
                key,
                data,
            )

    store = PausingStore()
    started = int(time.time() * 1000)
    first = LeaderLease(store, "manager-a", ttl_ms=1000)
    assert first.acquire_or_renew(now_ms=started)
    queue = DurableWorkQueue(store, leadership_check=first.validate)
    request, _task_id = _create(queue)
    store.pause_key = f"{REQUESTS_PREFIX}/{request['request_id']}.json"
    errors = []

    def stale_cancel():
        try:
            queue.cancel_request(request["request_id"], "stale-manager")
        except Exception as exc:  # noqa: BLE001 - asserted below
            errors.append(exc)

    mutation = threading.Thread(target=stale_cancel)
    mutation.start()
    assert store.entered.wait(5)
    second = LeaderLease(store, "manager-b", ttl_ms=1000)
    assert second.acquire_or_renew(now_ms=started + 1500)
    store.resume.set()
    mutation.join(timeout=5)

    assert len(errors) == 1
    assert isinstance(errors[0], StaleLeaderError)
    assert queue.get_request(request["request_id"])["state"] == TASK_QUEUED


def test_queue_rejects_wrong_claim_generation_and_output():
    store = MemoryStore()
    queue = DurableWorkQueue(store)
    _, task_id = _create(queue)
    claimed = _claim(queue, task_id)
    data = _parquet()
    store.put(claimed.output_key, data)

    wrong_owner = queue.accept_result(
        _result(claimed, data, agent_id="agent-2"),
        agent_lease_id="lease-1",
    )
    assert wrong_owner.reason_code == RESULT_WRONG_OWNER
    stale_lease = queue.accept_result(
        _result(claimed, data), agent_lease_id="lease-2"
    )
    assert stale_lease.reason_code == RESULT_STALE_CLAIM
    stale_generation = queue.accept_result(
        _result(claimed, data, generation_fence=5),
        agent_lease_id="lease-1",
    )
    assert stale_generation.reason_code == RESULT_STALE_GENERATION
    invalid = queue.accept_result(
        _result(claimed, data, record_count=99),
        agent_lease_id="lease-1",
    )
    assert invalid.reason_code == RESULT_INVALID_OUTPUT
    assert queue.get_task(task_id)["state"] != TASK_SUCCEEDED


def test_queue_retries_expired_and_retryable_claims_and_cancels():
    queue = DurableWorkQueue(
        MemoryStore(),
        task_lease_seconds=1,
        max_attempts=3,
        retry_backoff_seconds=2,
    )
    request, task_id = _create(queue)
    base = int(queue.get_task(task_id)["available_at_ms"])
    claimed = _claim(queue, task_id, now_ms=base)
    assert queue.expire_claims(now_ms=base + 1_001) == 1
    assert queue.get_task(task_id)["state"] == TASK_QUEUED

    claimed = _claim(queue, task_id, now_ms=base + 2_000)
    failed = _result(
        claimed,
        b"",
        ok=False,
        retryable=True,
        error="temporary source failure",
        error_code="source_unavailable",
    )
    ack = queue.accept_result(
        failed, agent_lease_id="lease-1", now_ms=base + 2_100
    )
    assert ack.ok and ack.state == TASK_RETRY_WAIT
    assert queue.runnable_tasks(now_ms=base + 6_099) == []
    assert [
        task["task_id"]
        for task in queue.runnable_tasks(now_ms=base + 6_100)
    ] == [
        task_id
    ]
    assert queue.release_claim(
        _claim(queue, task_id, now_ms=base + 6_100).task_id,
        queue.get_task(task_id)["claim"]["claim_token"],
        reason="delivery failed",
    )
    assert queue.get_task(task_id)["state"] == TASK_RETRY_WAIT
    assert queue.cancel_request(request["request_id"], "operator cancelled")
    assert queue.get_task(task_id)["state"] == TASK_CANCELLED


def test_queue_expires_unclaimed_deadline_and_surfaces_corrupt_records():
    store = MemoryStore()
    queue = DurableWorkQueue(store)
    _, task_id = _create(queue, deadline_ms=20_000)
    assert queue.expire_claims(now_ms=20_000) == 1
    assert queue.get_task(task_id)["state"] == TASK_FAILED

    store.put(f"{TASKS_PREFIX}/bad.json", b"{not-json")
    with pytest.raises(WorkQueueError, match="invalid queue record"):
        queue.list_tasks()


def test_queue_fences_old_manager_and_supports_retry_and_retention():
    queue = DurableWorkQueue(MemoryStore())
    request, task_id = _create(queue)
    claimed = _claim(queue, task_id)
    assert queue.fence_claims("manager-2", 2) == 1
    assert queue.get_task(task_id)["state"] == TASK_RETRY_WAIT
    stale = queue.accept_result(
        _result(claimed, b"", ok=False, error_code="stale"),
        agent_lease_id="lease-1",
    )
    assert not stale.ok and stale.reason_code == RESULT_STALE_CLAIM

    claimed = _claim(queue, task_id)
    failed = queue.accept_result(
        _result(
            claimed,
            b"",
            ok=False,
            retryable=False,
            error_code="permanent",
        ),
        agent_lease_id="lease-1",
    )
    assert failed.state == TASK_FAILED
    failed_task = queue.get_task(task_id)
    failed_task["task"]["deadline_ms"] = 1
    queue._write(f"{TASKS_PREFIX}/{task_id}.json", failed_task)
    assert queue.retry_task(task_id)
    assert queue.get_task(task_id)["state"] == TASK_QUEUED
    assert queue.get_task(task_id)["task"]["deadline_ms"] > int(time.time() * 1000)
    assert queue.cancel_request(request["request_id"], "done")
    updated = int(queue.get_request(request["request_id"])["updated_at_ms"])
    assert queue.prune_terminal(retention_seconds=0, now_ms=updated + 1) == 1
    assert queue.get_request(request["request_id"]) is None
    assert queue.get_task(task_id) is None


def test_queue_recovery_removes_partial_create_and_restores_idempotency():
    store = MemoryStore()
    queue = DurableWorkQueue(store)
    request, task_id = _create(queue)
    id_key = f"{IDEMPOTENCY_PREFIX}/{request['idempotency_key']}.json"
    store.delete(id_key)
    recovered = DurableWorkQueue(store).recover()
    assert recovered["removed_requests"] == 0
    assert store.exists(id_key)

    request["ready"] = False
    queue._write(
        f"_control/work-queue/v1/requests/{request['request_id']}.json",
        request,
    )
    recovered = DurableWorkQueue(store).recover()
    assert recovered["removed_requests"] == 1
    assert recovered["removed_tasks"] == 1
    assert queue.get_request(request["request_id"]) is None
    assert queue.get_task(task_id) is None


def test_terminal_task_failure_cancels_request_siblings():
    queue = DurableWorkQueue(MemoryStore())
    tasks = [
        MaterializeTask(
            table="sales",
            epoch=1,
            split_index=index,
            source_table="sales",
            output_key=f"warehouse/sales/{index}.parquet",
        )
        for index in range(2)
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
        deadline_ms=int(time.time() * 1000) + 60_000,
    )
    claimed = _claim(queue, request["task_ids"][0])
    result = _result(
        claimed,
        b"",
        ok=False,
        retryable=False,
        error_code="permanent",
    )

    queue.accept_result(result, agent_lease_id="lease-1")

    assert queue.get_request(request["request_id"])["state"] == TASK_FAILED
    assert queue.get_task(request["task_ids"][1])["state"] == TASK_CANCELLED


def test_shared_store_outage_recovers_without_losing_request():
    class OutageStore(MemoryStore):
        unavailable = False

        def list(self, prefix: str = ""):
            if self.unavailable:
                raise OSError("shared store unavailable")
            return super().list(prefix)

    store = OutageStore()
    queue = DurableWorkQueue(store)
    request, task_id = _create(queue)
    store.unavailable = True
    with pytest.raises(OSError, match="shared store unavailable"):
        queue.recover()

    store.unavailable = False
    assert queue.recover()["removed_requests"] == 0
    assert queue.get_request(request["request_id"]) is not None
    assert queue.get_task(task_id) is not None

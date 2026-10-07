from __future__ import annotations

import hashlib
import io
import time

import pyarrow as pa
import pyarrow.parquet as pq

from fabric_shortcut_proxy import config
from enterprise.control.contract import Column, MaterializeTask
from enterprise.materialize_worker import execute_task
from fabric_shortcut_proxy.runtime.artifact_store import MemoryStore


def _data() -> bytes:
    sink = io.BytesIO()
    pq.write_table(pa.table({"id": [1, 2]}), sink)
    return sink.getvalue()


def _task(connection_fingerprint: str) -> MaterializeTask:
    return MaterializeTask(
        table="sales",
        epoch=1,
        split_index=0,
        source_table="sales",
        output_key="warehouse/sales/0.parquet",
        schema=[Column(1, "id", "long", False, "id")],
        task_id="task-1",
        request_id="request-1",
        claim_token="claim-1",
        attempt=1,
        connection_id="default",
        connection_fingerprint=connection_fingerprint,
        generation_id="generation-1",
        generation_fence=1,
        plan_sha256="a" * 64,
        deadline_ms=int(time.time() * 1000) + 60_000,
    )


async def test_worker_materializes_and_reports_verified_identity(monkeypatch):
    import enterprise.materialize_worker as worker

    store = MemoryStore()
    data = _data()
    monkeypatch.setattr(config, "DB_URL", "sqlite+aiosqlite:///:memory:")
    fingerprint = hashlib.sha256(
        config.redact_db_url(config.DB_URL).encode()
    ).hexdigest()
    monkeypatch.setattr(worker, "get_default_store", lambda: store)

    async def materialize(split):
        store.put(split.object_key, data)
        return 2

    monkeypatch.setattr(worker, "materialize_queued_split", materialize)
    result = await execute_task(_task(fingerprint), "agent-1")

    assert result.ok
    assert result.record_count == 2
    assert result.size_bytes == len(data)
    assert result.content_hash == hashlib.sha256(data).hexdigest()
    assert result.claim_token == "claim-1"
    assert result.generation_id == "generation-1"


async def test_worker_rejects_connection_identity_mismatch(monkeypatch):
    monkeypatch.setattr(config, "DB_URL", "sqlite+aiosqlite:///:memory:")
    result = await execute_task(_task("0" * 64), "agent-1")

    assert not result.ok
    assert not result.retryable
    assert result.error_code == "task_failed"
    assert "does not match" in result.error


async def test_worker_rejects_expired_task_without_query(monkeypatch):
    import enterprise.materialize_worker as worker

    called = False

    async def materialize(_split):
        nonlocal called
        called = True
        return 0

    monkeypatch.setattr(worker, "materialize_queued_split", materialize)
    task = _task("")
    task.deadline_ms = int(time.time() * 1000) - 1
    result = await execute_task(task, "agent-1")

    assert not result.ok
    assert result.error_code == "deadline_expired"
    assert not called


async def test_worker_rejects_fenced_generation_before_query(monkeypatch):
    import enterprise.materialize_worker as worker
    from fabric_shortcut_proxy.runtime.generation import acquire_generation

    store = MemoryStore()
    acquire_generation(store, shard_count=1)
    called = False

    async def materialize(_split):
        nonlocal called
        called = True
        return 0

    monkeypatch.setattr(worker, "get_default_store", lambda: store)
    monkeypatch.setattr(worker, "materialize_queued_split", materialize)
    result = await execute_task(_task(""), "agent-1")

    assert not result.ok
    assert result.error_code == "generation_fenced"
    assert not called

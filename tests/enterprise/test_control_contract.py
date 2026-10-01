"""Tests for the Manager<->Agent control contract (Phase 0 frozen contract)."""
from __future__ import annotations

import pathlib
import re

from enterprise.control.contract import (
    CONTRACT_VERSION,
    MIN_COMPATIBLE_CONTRACT_VERSION,
    TASK_CLAIMED,
    TASK_EXPIRED,
    TASK_FAILED,
    TASK_QUEUED,
    TASK_RETRY_WAIT,
    TASK_SUCCEEDED,
    KeyRange,
    Column,
    SplitRef,
    SnapshotManifest,
    AgentHealth,
    RegisterRequest,
    RegisterResponse,
    HeartbeatRequest,
    MaterializeTask,
    TaskResult,
    Ack,
    contract_compatible,
    valid_task_transition,
    to_json,
    from_json,
)


def _roundtrip(cls, msg):
    """dict and JSON roundtrips must both reproduce the message."""
    assert cls.from_dict(msg.to_dict()) == msg
    assert from_json(cls, to_json(msg)) == msg


def test_value_types_roundtrip():
    _roundtrip(KeyRange, KeyRange(lo=0, hi=1_000_000))
    _roundtrip(Column, Column(
        field_id=1,
        name="id_token",
        iceberg_type="string",
        nullable=False,
        source="id",
        transform={"kind": "deterministic_hash", "key_ref": "orders"},
        policy_id="orders-v1",
    ))
    _roundtrip(SplitRef, SplitRef(
        object_key="warehouse/db/t/data/split-0-abc.parquet",
        size_bytes=166566, record_count=6250, content_hash="abc123def456",
        range=KeyRange(0, 1_000_000),
    ))


def test_split_ref_optional_range():
    s = SplitRef(object_key="k", size_bytes=1, record_count=2, content_hash="h")
    assert "range" not in s.to_dict()
    _roundtrip(SplitRef, s)


def test_snapshot_manifest_roundtrip():
    m = SnapshotManifest(
        table="Customer", epoch=7, table_format="delta",
        splits=[
            SplitRef("warehouse/db/Customer/data/split-0-a.parquet", 100, 10, "a", KeyRange(0, 10)),
            SplitRef("warehouse/db/Customer/data/split-1-b.parquet", 200, 20, "b", KeyRange(10, 30)),
        ],
        metadata_keys=["warehouse/db/Customer/_delta_log/00000000000000000000.json"],
        generation_id="generation-7",
        generation_fence=4,
        plan_sha256="a" * 64,
        published_at_ms=1000,
        request_id="request-7",
    )
    _roundtrip(SnapshotManifest, m)
    assert m.to_dict()["table_format"] == "delta"


def test_agent_lifecycle_roundtrip():
    _roundtrip(RegisterRequest, RegisterRequest(
        agent_id="agent-1", host="10.0.0.5", port=9000, os="linux", version="abc123",
        capacity_hint=8, capabilities=["materializer"]))
    _roundtrip(RegisterResponse, RegisterResponse(lease_id="L1", heartbeat_ms=2000))
    _roundtrip(HeartbeatRequest, HeartbeatRequest(
        agent_id="agent-1", lease_id="L1",
        health=AgentHealth(cpu_pct=12.5, mem_bytes=1 << 30, cache_bytes=1 << 20, inflight=3),
        serving_tables=["Customer", "Product"],
        epochs={"Customer": 7, "Product": 3},
    ))


def test_register_carries_contract_version():
    r = RegisterRequest(
        agent_id="a",
        host="h",
        port=1,
        os="windows",
        version="v",
        shard_index=2,
    )
    assert r.contract_version == CONTRACT_VERSION
    assert r.to_dict()["contract_version"] == CONTRACT_VERSION
    assert CONTRACT_VERSION == "1.1"
    assert MIN_COMPATIBLE_CONTRACT_VERSION == "1.0"
    assert contract_compatible("1.0") is True
    assert contract_compatible("1.1") is True
    assert contract_compatible("1.2") is False
    assert contract_compatible("2.0") is False
    assert RegisterRequest.from_dict(r.to_dict()).shard_index == 2


def test_version_one_registration_payload_defaults_compatibly():
    request = RegisterRequest.from_dict({
        "agent_id": "old-agent",
        "host": "127.0.0.1",
        "port": 9000,
        "os": "linux",
        "version": "old",
    })
    response = RegisterResponse.from_dict({"lease_id": "L1", "heartbeat_ms": 2000})

    assert request.contract_version == "1.0"
    assert request.capabilities == []
    assert response.contract_version == "1.0"


def test_materialize_task_and_result_roundtrip():
    _roundtrip(MaterializeTask, MaterializeTask(
        table="Customer", epoch=8, split_index=0, source_table="SalesLT.Customer",
        output_key="warehouse/db/Customer/data/split-0-new.parquet",
        schema=[Column(1, "id", "long", False), Column(2, "name", "string", True)],
        range=KeyRange(0, 1_000_000),
        task_id="task-1",
        request_id="request-1",
        claim_token="claim-1",
        attempt=2,
        claim_expires_at_ms=5000,
        connection_id="warehouse",
        connection_fingerprint="c" * 64,
        generation_id="generation-1",
        generation_fence=3,
        plan_sha256="b" * 64,
        table_format="delta",
        deadline_ms=10000,
        num_splits=8,
        key_column="id",
        split_strategy="range",
    ))
    _roundtrip(TaskResult, TaskResult(
        agent_id="agent-1", table="Customer", epoch=8, split_index=0, ok=True,
        size_bytes=166579, record_count=6250, content_hash="5ce2c1b9a09f",
        task_id="task-1", request_id="request-1", claim_token="claim-1",
        attempt=2, generation_id="generation-1", generation_fence=3,
        plan_sha256="b" * 64, completed_at_ms=6000))
    _roundtrip(TaskResult, TaskResult(
        agent_id="agent-1", table="Customer", epoch=8, split_index=3, ok=False,
        error="source timeout"))
    _roundtrip(Ack, Ack(
        ok=True, state=TASK_SUCCEEDED, duplicate=True, terminal=True,
        reason_code="duplicate",
    ))


def test_task_state_transitions_are_explicit_and_terminal():
    assert valid_task_transition(TASK_QUEUED, TASK_CLAIMED)
    assert valid_task_transition(TASK_QUEUED, TASK_FAILED)
    assert valid_task_transition(TASK_CLAIMED, TASK_SUCCEEDED)
    assert valid_task_transition(TASK_CLAIMED, TASK_EXPIRED)
    assert valid_task_transition(TASK_CLAIMED, TASK_RETRY_WAIT)
    assert valid_task_transition(TASK_CLAIMED, TASK_FAILED)
    assert valid_task_transition(TASK_RETRY_WAIT, TASK_FAILED)
    assert not valid_task_transition(TASK_SUCCEEDED, TASK_QUEUED)
    assert not valid_task_transition("UNKNOWN", TASK_QUEUED)


def test_version_one_task_result_and_ack_payloads_decode_with_defaults():
    task = MaterializeTask.from_dict({
        "table": "Customer",
        "epoch": 1,
        "split_index": 0,
        "source_table": "SalesLT.Customer",
        "output_key": "warehouse/db/Customer/data/split-0.parquet",
        "schema": [],
        "unknown_future_field": "ignored",
    })
    result = TaskResult.from_dict({
        "agent_id": "agent-1",
        "table": "Customer",
        "epoch": 1,
        "split_index": 0,
        "ok": True,
    })
    ack = Ack.from_dict({"ok": True})

    assert task.task_id == ""
    assert task.connection_id == "default"
    assert task.generation_fence == 0
    assert result.task_id == ""
    assert result.retryable is False
    assert ack == Ack(ok=True)


def test_frozen_proto_present_and_mirrors_contract():
    proto = (pathlib.Path(__file__).resolve().parents[2]
             / "enterprise" / "control" / "proto" / "control.proto")
    text = proto.read_text(encoding="utf-8")
    assert "service ControlPlane" in text
    for pattern in (
        r"repeated\s+string\s+capabilities\s*=\s*9",
        r"int32\s+shard_index\s*=\s*10",
        r"repeated\s+string\s+active_task_ids\s*=\s*6",
        r"string\s+task_id\s*=\s*8",
        r"string\s+claim_token\s*=\s*10",
        r"string\s+task_id\s*=\s*10",
        r"string\s+plan_sha256\s*=\s*16",
        r"string\s+connection_fingerprint\s*=\s*22",
    ):
        assert re.search(pattern, text), pattern
    # every contract message name appears in the frozen .proto
    for name in ("KeyRange", "Column", "SplitRef", "SnapshotManifest", "AgentHealth",
                 "RegisterRequest", "RegisterResponse", "HeartbeatRequest",
                 "MaterializeTask", "TaskResult"):
        assert f"message {name}" in text, f"{name} missing from control.proto"

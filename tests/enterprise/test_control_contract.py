"""Tests for the Manager<->Agent control contract (Phase 0 frozen contract)."""
from __future__ import annotations

import pathlib

from enterprise.control.contract import (
    CONTRACT_VERSION, KeyRange, Column, SplitRef, SnapshotManifest, AgentHealth,
    RegisterRequest, RegisterResponse, HeartbeatRequest, MaterializeTask, TaskResult,
    to_json, from_json,
)


def _roundtrip(cls, msg):
    """dict and JSON roundtrips must both reproduce the message."""
    assert cls.from_dict(msg.to_dict()) == msg
    assert from_json(cls, to_json(msg)) == msg


def test_value_types_roundtrip():
    _roundtrip(KeyRange, KeyRange(lo=0, hi=1_000_000))
    _roundtrip(Column, Column(field_id=1, name="id", iceberg_type="long", nullable=False))
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
    )
    _roundtrip(SnapshotManifest, m)
    assert m.to_dict()["table_format"] == "delta"


def test_agent_lifecycle_roundtrip():
    _roundtrip(RegisterRequest, RegisterRequest(
        agent_id="agent-1", host="10.0.0.5", port=9000, os="linux", version="abc123",
        capacity_hint=8))
    _roundtrip(RegisterResponse, RegisterResponse(lease_id="L1", heartbeat_ms=2000))
    _roundtrip(HeartbeatRequest, HeartbeatRequest(
        agent_id="agent-1", lease_id="L1",
        health=AgentHealth(cpu_pct=12.5, mem_bytes=1 << 30, cache_bytes=1 << 20, inflight=3),
        serving_tables=["Customer", "Product"],
        epochs={"Customer": 7, "Product": 3},
    ))


def test_register_carries_contract_version():
    r = RegisterRequest(agent_id="a", host="h", port=1, os="windows", version="v")
    assert r.contract_version == CONTRACT_VERSION
    assert r.to_dict()["contract_version"] == CONTRACT_VERSION


def test_materialize_task_and_result_roundtrip():
    _roundtrip(MaterializeTask, MaterializeTask(
        table="Customer", epoch=8, split_index=0, source_table="SalesLT.Customer",
        output_key="warehouse/db/Customer/data/split-0-new.parquet",
        schema=[Column(1, "id", "long", False), Column(2, "name", "string", True)],
        range=KeyRange(0, 1_000_000),
    ))
    _roundtrip(TaskResult, TaskResult(
        agent_id="agent-1", table="Customer", epoch=8, split_index=0, ok=True,
        size_bytes=166579, record_count=6250, content_hash="5ce2c1b9a09f"))
    _roundtrip(TaskResult, TaskResult(
        agent_id="agent-1", table="Customer", epoch=8, split_index=3, ok=False,
        error="source timeout"))


def test_frozen_proto_present_and_mirrors_contract():
    proto = (pathlib.Path(__file__).resolve().parents[2]
             / "enterprise" / "control" / "proto" / "control.proto")
    text = proto.read_text(encoding="utf-8")
    assert "service ControlPlane" in text
    # every contract message name appears in the frozen .proto
    for name in ("KeyRange", "Column", "SplitRef", "SnapshotManifest", "AgentHealth",
                 "RegisterRequest", "RegisterResponse", "HeartbeatRequest",
                 "MaterializeTask", "TaskResult"):
        assert f"message {name}" in text, f"{name} missing from control.proto"

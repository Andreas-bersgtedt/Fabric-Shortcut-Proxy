from __future__ import annotations

import time

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from enterprise.control.contract import MaterializeTask, RegisterRequest
from enterprise.control.placement import (
    AgentVersionUnsupported,
    PlacementConfig,
)
from enterprise.control.registry import Registry
from enterprise.control.server import ControlService
from enterprise.control.task_scheduler import TaskScheduler
from enterprise.control.transport import create_control_router
from enterprise.control.work_queue import DurableWorkQueue
from fabric_shortcut_proxy.runtime.artifact_store import MemoryStore


def _config() -> dict:
    return {
        "agent_versions": {
            "current": "3.1",
            "previous": "3.0",
            "rings": {
                "canary": ["3.1"],
                "production": ["3.1", "3.0"],
                "remote": ["3.0"],
            },
            "rollout_order": [
                "canary",
                "production",
                "remote",
            ],
        },
        "pools": [
            {
                "pool_id": "canary-pool",
                "upgrade_ring": "canary",
                "identities": ["canary-agent"],
                "allowed_connection_ids": ["source"],
            },
            {
                "pool_id": "production-pool",
                "upgrade_ring": "production",
                "identities": ["production-agent"],
                "allowed_connection_ids": ["source"],
            },
            {
                "pool_id": "remote-pool",
                "upgrade_ring": "remote",
                "identities": ["remote-agent"],
                "allowed_connection_ids": ["source"],
            },
        ],
        "connections": {
            "source": {
                "required_pool": "canary-pool",
            }
        },
    }


def _request(agent_id: str, version: str) -> RegisterRequest:
    return RegisterRequest(
        agent_id=agent_id,
        host="127.0.0.1",
        port=9000,
        os="linux",
        version=version,
        capabilities=["materializer"],
        contract_version="1.1",
    )


def test_n_and_n_minus_one_are_enforced_by_rollout_ring():
    registry = Registry(placement=PlacementConfig.from_dict(_config()))

    registry.register(_request("canary-agent", "3.1.0+build.4"))
    registry.register(_request("production-agent", "3.0.9"))
    registry.register(_request("remote-agent", "3.0.2"))

    assert {
        item["upgrade_ring"] for item in registry.list_public()
    } == {"canary", "production", "remote"}
    with pytest.raises(
        AgentVersionUnsupported,
        match="not enabled for ring",
    ):
        registry.register(_request("canary-agent", "3.0.9"))
    with pytest.raises(
        AgentVersionUnsupported,
        match="outside N/N-1",
    ):
        registry.register(_request("production-agent", "2.9.8"))


def test_invalid_version_policy_or_pool_ring_is_rejected():
    value = _config()
    value["agent_versions"]["rings"]["canary"] = ["2.9"]
    with pytest.raises(ValueError, match="outside N/N-1"):
        PlacementConfig.from_dict(value)

    value = _config()
    value["pools"][0]["upgrade_ring"] = "missing"
    with pytest.raises(ValueError, match="configured upgrade ring"):
        PlacementConfig.from_dict(value)


def test_transport_returns_agent_version_unsupported():
    registry = Registry(placement=PlacementConfig.from_dict(_config()))
    app = FastAPI()
    app.include_router(create_control_router(ControlService(registry)))

    with TestClient(app) as client:
        response = client.post(
            "/control/register",
            json=_request("canary-agent", "3.0.9").to_dict(),
        )

    assert response.status_code == 409
    assert response.json()["error"] == "agent_version_unsupported"
    assert registry.count() == 0


def test_rejected_agent_leaves_tasks_queued():
    placement = PlacementConfig.from_dict(_config())
    registry = Registry(placement=placement)
    with pytest.raises(AgentVersionUnsupported):
        registry.register(_request("canary-agent", "3.0.9"))
    queue = DurableWorkQueue(MemoryStore())
    request = queue.create_request(
        requested_key="warehouse/metadata.json",
        table="sales",
        epoch=1,
        table_format="iceberg",
        generation_id="version-generation",
        generation_fence=1,
        plan_sha256="a" * 64,
        tasks=[
            MaterializeTask(
                table="sales",
                epoch=1,
                split_index=0,
                source_table="sales",
                connection_id="source",
                output_key="warehouse/0.parquet",
            )
        ],
        deadline_ms=int(time.time() * 1000) + 60_000,
    )
    scheduler = TaskScheduler(
        queue,
        registry,
        manager_owner="manager",
        manager_fence=1,
    )

    assert scheduler.dispatch_once() == 0
    task = queue.get_task(request["task_ids"][0])
    assert task["state"] == "QUEUED"
    assert task["claim"] is None


def test_restore_drops_agent_no_longer_allowed_by_ring():
    original = Registry(
        placement=PlacementConfig.from_dict(_config())
    )
    original.register(_request("production-agent", "3.0.9"))
    snapshot = original._snapshot_locked()
    updated = _config()
    updated["agent_versions"]["rings"]["production"] = ["3.1"]
    restored = Registry(
        placement=PlacementConfig.from_dict(updated)
    )

    restored._restore_locked(snapshot)

    assert restored.count() == 0

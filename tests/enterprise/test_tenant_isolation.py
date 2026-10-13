from __future__ import annotations

import time

import pytest

from enterprise.control.contract import MaterializeTask, RegisterRequest
from enterprise.control.federation import GlobalCatalog
from enterprise.control.placement import (
    PlacementConfig,
    TenantIsolationViolation,
)
from enterprise.control.registry import Registry
from enterprise.control.task_scheduler import TaskScheduler
from enterprise.control.work_queue import (
    DurableWorkQueue,
    WorkQueueConflict,
)
from fabric_shortcut_proxy.runtime.artifact_store import MemoryStore
from tests.enterprise.test_residency_policy import _config as residency_config


def _tenant_config() -> dict:
    return {
        "tenants": ["tenant-a", "tenant-b"],
        "pools": [
            {
                "pool_id": "pool-a",
                "tenant_id": "tenant-a",
                "identities": ["agent-a"],
                "allowed_connection_ids": ["source-a"],
            },
            {
                "pool_id": "pool-b",
                "tenant_id": "tenant-b",
                "identities": ["agent-b"],
                "allowed_connection_ids": ["source-b"],
            },
        ],
        "connections": {
            "source-a": {
                "tenant_id": "tenant-a",
                "required_pool": "pool-a",
            },
            "source-b": {
                "tenant_id": "tenant-b",
                "required_pool": "pool-b",
            },
        },
    }


def _register(registry: Registry, agent_id: str) -> None:
    registry.register(
        RegisterRequest(
            agent_id=agent_id,
            host="127.0.0.1",
            port=9000,
            os="linux",
            version="test",
            capabilities=["materializer"],
            contract_version="1.1",
        )
    )


def _queue_task(*, tenant_id: str, connection_id: str = "source-a"):
    queue = DurableWorkQueue(MemoryStore())
    request = queue.create_request(
        requested_key="warehouse/metadata.json",
        table="sales",
        epoch=1,
        table_format="iceberg",
        generation_id="tenant-generation",
        generation_fence=1,
        plan_sha256="a" * 64,
        tasks=[
            MaterializeTask(
                table="sales",
                epoch=1,
                split_index=0,
                source_table="sales",
                connection_id=connection_id,
                output_key="warehouse/0.parquet",
                tenant_id=tenant_id,
            )
        ],
        deadline_ms=int(time.time() * 1000) + 60_000,
    )
    return queue, request


def test_tenant_resources_must_be_complete_and_consistent():
    value = _tenant_config()
    value["pools"][0].pop("tenant_id")
    with pytest.raises(TenantIsolationViolation, match="one configured tenant"):
        PlacementConfig.from_dict(value)

    value = _tenant_config()
    value["pools"][0]["allowed_connection_ids"] = ["source-b"]
    with pytest.raises(TenantIsolationViolation, match="another tenant"):
        PlacementConfig.from_dict(value)

    value = _tenant_config()
    value["connections"]["source-a"]["required_pool"] = "pool-b"
    with pytest.raises(TenantIsolationViolation, match="another tenant"):
        PlacementConfig.from_dict(value)


def test_tenant_fields_require_explicit_tenant_mode():
    value = _tenant_config()
    value.pop("tenants")

    with pytest.raises(
        TenantIsolationViolation,
        match="top-level tenants",
    ):
        PlacementConfig.from_dict(value)


def test_registry_derives_tenant_from_operator_owned_pool():
    registry = Registry(placement=PlacementConfig.from_dict(_tenant_config()))

    _register(registry, "agent-a")

    assert registry.list_public()[0]["tenant_id"] == "tenant-a"


def test_scheduler_rejects_task_tenant_before_pool_eligibility(monkeypatch):
    placement = PlacementConfig.from_dict(_tenant_config())
    queue, request = _queue_task(tenant_id="tenant-b")
    registry = Registry(placement=placement)
    _register(registry, "agent-a")
    _register(registry, "agent-b")

    def eligibility_must_not_run(*_args, **_kwargs):
        raise AssertionError("pool eligibility ran before tenant rejection")

    monkeypatch.setattr(
        type(placement.pools["pool-a"]),
        "allows",
        eligibility_must_not_run,
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
    assert task["dispatch_status"] == "tenant_isolation_violation"


def test_scheduler_assigns_only_same_tenant_pool():
    placement = PlacementConfig.from_dict(_tenant_config())
    queue, request = _queue_task(tenant_id="tenant-a")
    registry = Registry(placement=placement)
    _register(registry, "agent-a")
    _register(registry, "agent-b")
    scheduler = TaskScheduler(
        queue,
        registry,
        manager_owner="manager",
        manager_fence=1,
    )

    assert scheduler.dispatch_once() == 1
    task = queue.get_task(request["task_ids"][0])
    assert task["claim"]["agent_id"] == "agent-a"
    assert task["task"]["tenant_id"] == "tenant-a"
    assert queue.get_request(request["request_id"])["tenant_id"] == "tenant-a"


def test_queue_rejects_request_spanning_tenants():
    queue = DurableWorkQueue(MemoryStore())
    tasks = [
        MaterializeTask(
            table="sales",
            epoch=1,
            split_index=index,
            source_table="sales",
            output_key=f"warehouse/{index}.parquet",
            tenant_id=tenant_id,
        )
        for index, tenant_id in enumerate(("tenant-a", "tenant-b"))
    ]

    with pytest.raises(WorkQueueConflict, match="multiple tenants"):
        queue.create_request(
            requested_key="warehouse/metadata.json",
            table="sales",
            epoch=1,
            table_format="iceberg",
            generation_id="tenant-generation",
            generation_fence=1,
            plan_sha256="a" * 64,
            tasks=tasks,
            deadline_ms=int(time.time() * 1000) + 60_000,
        )


def test_catalog_record_carries_policy_tenant():
    value = residency_config()
    value["tenants"] = ["tenant-a"]
    for pool in value["pools"]:
        pool["tenant_id"] = "tenant-a"
    for store in value["stores"].values():
        store["tenant_id"] = "tenant-a"
    for endpoint in value["serving_endpoints"].values():
        endpoint["tenant_id"] = "tenant-a"
    value["connections"]["erp"]["tenant_id"] = "tenant-a"
    catalog = GlobalCatalog(
        MemoryStore(),
        PlacementConfig.from_dict(value),
    )

    record = catalog.create(
        "erp::sales",
        owner_id="manager-a",
        owner_location="azure:swedencentral",
        lease_ms=60_000,
        freshness_target_ms=300_000,
        now_ms=1_000,
    )

    assert record["tenant_id"] == "tenant-a"


def test_audit_export_filters_exact_tenant(monkeypatch):
    from fabric_shortcut_proxy import config
    from fabric_shortcut_proxy.observability import audit

    monkeypatch.setattr(config, "ENABLE_AUDIT_LOG", True)
    audit.record_placement_decision(
        task_id="task-a",
        request_id="request-a",
        dataset="source-a::sales",
        tenant_id="tenant-a",
        pool_id="pool-a",
        location="site-a",
        outcome="assigned",
    )
    audit.record_placement_decision(
        task_id="task-b",
        request_id="request-b",
        dataset="source-b::sales",
        tenant_id="tenant-b",
        pool_id="pool-b",
        location="site-b",
        outcome="assigned",
    )

    events = audit.recent_for_tenant("tenant-a")

    assert events
    assert {event["tenant_id"] for event in events} == {"tenant-a"}
    assert events[-1]["task_id"] == "task-a"

from __future__ import annotations

import time

import pytest

from enterprise.control.contract import MaterializeTask, RegisterRequest
from enterprise.control.placement import PlacementConfig
from enterprise.control.registry import Registry
from enterprise.control.task_scheduler import TaskScheduler
from enterprise.control.work_queue import DurableWorkQueue
from fabric_shortcut_proxy.runtime.artifact_store import MemoryStore


def _placement_config() -> PlacementConfig:
    return PlacementConfig.from_dict({
        "pools": [
            {
                "pool_id": "primary",
                "identities": ["primary-agent"],
                "location": "site-a",
                "storage_profile": "central",
                "allowed_connection_ids": ["erp-a"],
                "allowed_table_patterns": ["sales.*"],
            },
            {
                "pool_id": "fallback",
                "identities": ["fallback-agent"],
                "location": "site-b",
                "storage_profile": "central",
                "allowed_connection_ids": ["erp-a"],
            },
            {
                "pool_id": "other",
                "identities": ["other-agent"],
                "location": "site-c",
                "storage_profile": "central",
                "allowed_connection_ids": ["erp-a"],
            },
        ],
        "connections": {
            "erp-a": {
                "required_pool": "primary",
                "required_location": "site-a",
            }
        },
        "tables": {
            "erp-a::sales.orders": {
                "required_pool": "fallback",
                "required_location": "site-b",
            }
        },
    })


def _register(registry: Registry, agent_id: str, contract_version: str = "1.1"):
    return registry.register(RegisterRequest(
        agent_id=agent_id,
        host="127.0.0.1",
        port=9000,
        os="linux",
        version="test",
        capabilities=["materializer"],
        contract_version=contract_version,
        pool_id="primary",
        location="forged-location",
    ))


def _queue(*, table: str = "sales.customers", task_count: int = 1):
    queue = DurableWorkQueue(MemoryStore())
    tasks = [
        MaterializeTask(
            table=table,
            epoch=1,
            split_index=index,
            source_table=table,
            connection_id="erp-a",
            output_key=f"warehouse/{index}.parquet",
        )
        for index in range(task_count)
    ]
    request = queue.create_request(
        requested_key="warehouse/metadata.json",
        table=table,
        epoch=1,
        table_format="iceberg",
        generation_id="placement-generation",
        generation_fence=1,
        plan_sha256="a" * 64,
        tasks=tasks,
        deadline_ms=int(time.time() * 1000) + 60_000,
    )
    return queue, request


def _scheduler(queue, registry):
    return TaskScheduler(
        queue,
        registry,
        manager_owner="manager",
        manager_fence=1,
        max_inflight_per_agent=4,
    )


def test_registry_derives_pool_from_identity_not_registration_labels():
    placement = _placement_config()
    registry = Registry(placement=placement)
    _register(registry, "primary-agent")

    public_agent = registry.list_public()[0]

    assert public_agent["pool_id"] == "primary"
    assert public_agent["location"] == "site-a"
    assert public_agent["allowed_connection_ids"] == ["erp-a"]
    assert public_agent["location"] != "forged-location"


def test_scheduler_applies_table_override_and_ignores_forged_pool_labels():
    queue, request = _queue(table="sales.orders")
    registry = Registry(placement=_placement_config())
    _register(registry, "primary-agent")
    fallback = _register(registry, "fallback-agent")
    scheduler = _scheduler(queue, registry)

    assert scheduler.dispatch_once() == 1
    task = queue.get_task(request["task_ids"][0])
    assert task["claim"]["agent_id"] == "fallback-agent"
    assert fallback.lease_id
    membership = queue.get_membership("placement-generation")
    assert set(membership["workers"]) == {"fallback-agent"}


def test_scheduler_keeps_no_match_queued_and_reports_it(monkeypatch):
    from fabric_shortcut_proxy.observability import audit

    queue, request = _queue()
    registry = Registry(placement=_placement_config())
    _register(registry, "other-agent")
    audit_events = []
    monkeypatch.setattr(
        audit,
        "record_placement_decision",
        lambda **event: audit_events.append(event),
    )

    assert _scheduler(queue, registry).dispatch_once() == 0

    task = queue.get_task(request["task_ids"][0])
    assert task["state"] == "QUEUED"
    assert task["dispatch_status"] == "no_eligible_materializer"
    placement_status = queue.status()["placement"]
    assert placement_status["no_eligible_materializer_count"] == 1
    assert placement_status["no_eligible_materializer"][0]["task_id"] == task["task_id"]
    assert audit_events[0] == {
        "task_id": task["task_id"],
        "request_id": request["request_id"],
        "dataset": "erp-a::sales.customers",
        "pool_id": "primary",
        "location": "site-a",
        "outcome": "queued",
        "reason": "no_eligible_materializer",
        "fallback": False,
    }


def test_legacy_agent_can_only_receive_unpinned_work():
    queue, request = _queue()
    placement = PlacementConfig.from_dict({
        "pools": [{
            "pool_id": "primary",
            "identities": ["primary-agent"],
            "location": "site-a",
            "allowed_connection_ids": ["erp-a"],
        }],
        "connections": {
            "erp-a": {"required_pool": "primary"}
        },
    })
    registry = Registry(placement=placement)
    _register(registry, "primary-agent", contract_version="1.0")

    assert _scheduler(queue, registry).dispatch_once() == 0
    assert queue.get_task(request["task_ids"][0])["state"] == "QUEUED"


def test_legacy_agent_receives_unpinned_work_when_its_pool_is_authorized():
    queue, request = _queue()
    placement = PlacementConfig.from_dict({
        "pools": [{
            "pool_id": "primary",
            "identities": ["primary-agent"],
            "allowed_connection_ids": ["erp-a"],
        }]
    })
    registry = Registry(placement=placement)
    _register(registry, "primary-agent", contract_version="1.0")

    assert _scheduler(queue, registry).dispatch_once() == 1
    assert (
        queue.get_task(request["task_ids"][0])["claim"]["agent_id"]
        == "primary-agent"
    )


def test_explicit_fallback_is_used_only_after_primary_is_unavailable():
    queue, request = _queue()
    placement = PlacementConfig.from_dict({
        "pools": [{
            "pool_id": "primary",
            "identities": ["primary-agent"],
            "allowed_connection_ids": ["erp-a"],
        }, {
            "pool_id": "fallback",
            "identities": ["fallback-agent"],
            "allowed_connection_ids": ["erp-a"],
        }, {
            "pool_id": "unauthorized",
            "identities": ["unauthorized-agent"],
            "allowed_connection_ids": ["erp-a"],
        }],
        "connections": {
            "erp-a": {
                "required_pool": "primary",
                "fallback_pools": ["fallback"],
            }
        },
    })
    registry = Registry(placement=placement)
    _register(registry, "fallback-agent")
    _register(registry, "unauthorized-agent")

    assert _scheduler(queue, registry).dispatch_once() == 1
    assert (
        queue.get_task(request["task_ids"][0])["claim"]["agent_id"]
        == "fallback-agent"
    )


def test_connection_max_concurrency_limits_active_tasks():
    queue, request = _queue(task_count=2)
    placement = PlacementConfig.from_dict({
        "pools": [{
            "pool_id": "primary",
            "identities": ["primary-agent"],
            "allowed_connection_ids": ["erp-a"],
        }],
        "connections": {
            "erp-a": {"required_pool": "primary", "max_concurrency": 1}
        },
    })
    registry = Registry(placement=placement)
    _register(registry, "primary-agent")

    assert _scheduler(queue, registry).dispatch_once() == 1
    states = [
        queue.get_task(task_id)
        for task_id in request["task_ids"]
    ]
    assert sum(task["state"] == "CLAIMED" for task in states) == 1
    assert sum(
        task.get("dispatch_status") == "no_eligible_materializer"
        for task in states
    ) == 1


@pytest.mark.parametrize(
    "value, message",
    [
        (
            {"pools": [{"pool_id": "p", "identities": ["a"]}]},
            "allowed_connection_ids",
        ),
        (
            {
                "pools": [{
                    "pool_id": "p",
                    "identities": ["a"],
                    "allowed_connection_ids": [],
                }],
                "connections": {
                    "c": {"required_pool": "missing"}
                },
            },
            "unknown pools",
        ),
    ],
)
def test_invalid_placement_configuration_is_rejected(value, message):
    with pytest.raises(ValueError, match=message):
        PlacementConfig.from_dict(value)


def test_empty_connection_allow_list_grants_no_source_access():
    placement = PlacementConfig.from_dict({
        "pools": [{
            "pool_id": "empty",
            "identities": ["empty-agent"],
            "allowed_connection_ids": [],
        }]
    })

    assert not placement.pools["empty"].allows("erp-a", "sales.orders")


def test_placement_requires_identity_token_and_pool_distinct_credentials():
    placement = PlacementConfig.from_dict({
        "pools": [{
            "pool_id": "primary",
            "identities": ["primary-agent"],
            "allowed_connection_ids": ["erp-a"],
        }]
    })

    with pytest.raises(ValueError, match="missing pool identities"):
        placement.validate_identity_tokens("{}", ("shared-" + "s" * 32,))
    with pytest.raises(ValueError, match="must differ"):
        placement.validate_identity_tokens(
            '{"primary-agent":"' + "s" * 32 + '"}',
            ("s" * 32,),
        )

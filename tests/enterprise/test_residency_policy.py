from __future__ import annotations

from dataclasses import replace
import time

import pytest

from enterprise.control.contract import MaterializeTask, RegisterRequest
from enterprise.control.placement import PlacementConfig, ResidencyPolicyViolation, StoreProfile
from enterprise.control.registry import Registry
from enterprise.control.task_scheduler import TaskScheduler
from enterprise.control.work_queue import DurableWorkQueue
from fabric_shortcut_proxy.runtime.artifact_store import MemoryStore


def _config():
    return {
        "pools": [{
            "pool_id": "eu", "identities": ["eu-agent"],
            "location": "azure:swedencentral", "storage_profile": "published",
            "allowed_connection_ids": ["erp"],
        }, {
            "pool_id": "backup", "identities": ["backup-agent"],
            "location": "azure:northeurope", "storage_profile": "published",
            "allowed_connection_ids": ["erp"],
        }],
        "stores": {
            "published": {"location": "azure:swedencentral", "provider": "azure"},
            "staging": {"location": "azure:swedencentral", "provider": "azure"},
            "replica": {"location": "azure:northeurope", "provider": "azure"},
            "cache": {"location": "azure:northeurope", "provider": "local"},
        },
        "serving_endpoints": {
            "primary": {
                "location": "azure:swedencentral", "storage_profile": "published",
                "url": "https://primary.example.test",
            },
            "secondary": {
                "location": "azure:northeurope", "storage_profile": "replica",
                "url": "https://secondary.example.test",
            },
        },
        "connections": {"erp": {
            "required_pool": "eu", "fallback_pools": ["backup"],
            "required_storage_profile": "published", "staging_storage_profile": "staging",
            "residency_locations": ["azure:swedencentral", "azure:northeurope"],
            "serving_endpoints": ["primary", "secondary"],
            "replica_storage_profiles": ["replica"], "cache_storage_profiles": ["cache"],
        }},
    }


def test_whole_chain_and_legacy_defaults():
    placement = PlacementConfig.from_dict(_config())
    placement.validate_residency(placement.policy_for("erp", "sales"))
    assert not PlacementConfig.from_dict({}).enabled
    assert placement.serving_endpoints["secondary"].storage_profile == "replica"


@pytest.mark.parametrize("store", ["published", "staging", "replica", "cache"])
def test_outside_store_is_rejected_at_apply(store):
    value = _config()
    value["stores"][store]["location"] = "aws:us-east-1"
    with pytest.raises(ResidencyPolicyViolation, match="residency_policy_violation"):
        PlacementConfig.from_dict(value)


@pytest.mark.parametrize("pool", [0, 1])
def test_outside_pool_is_rejected_even_when_offline(pool):
    value = _config()
    value["pools"][pool]["location"] = "aws:us-east-1"
    with pytest.raises(ResidencyPolicyViolation):
        PlacementConfig.from_dict(value)


@pytest.mark.parametrize("endpoint", ["primary", "secondary"])
def test_outside_endpoint_is_rejected(endpoint):
    value = _config()
    value["serving_endpoints"][endpoint]["location"] = "aws:us-east-1"
    with pytest.raises(ResidencyPolicyViolation):
        PlacementConfig.from_dict(value)


def test_serving_must_be_local_and_reference_published_or_replica_store():
    value = _config()
    value["serving_endpoints"]["secondary"]["storage_profile"] = "published"
    with pytest.raises(ResidencyPolicyViolation, match="own location"):
        PlacementConfig.from_dict(value)
    value = _config()
    value["serving_endpoints"]["secondary"]["storage_profile"] = "cache"
    with pytest.raises(ResidencyPolicyViolation, match="configured replica"):
        PlacementConfig.from_dict(value)


@pytest.mark.parametrize("field", [
    "required_pool", "required_storage_profile", "serving_endpoints",
])
def test_incomplete_chain_is_rejected(field):
    value = _config()
    value["connections"]["erp"].pop(field)
    with pytest.raises(ValueError):
        PlacementConfig.from_dict(value)


@pytest.mark.parametrize("field", [
    "required_storage_profile", "staging_storage_profile",
    "replica_storage_profiles", "cache_storage_profiles", "serving_endpoints",
])
def test_unknown_chain_reference_is_rejected(field):
    value = _config()
    value["connections"]["erp"][field] = ["missing"] if field.endswith("s") else "missing"
    with pytest.raises(ResidencyPolicyViolation):
        PlacementConfig.from_dict(value)


@pytest.mark.parametrize("locations", [[], ["aws:us-east-1"]])
def test_table_override_cannot_remove_or_widen_boundary(locations):
    value = _config()
    value["tables"] = {"erp::sales": {"residency_locations": locations}}
    with pytest.raises(ResidencyPolicyViolation, match="cannot widen"):
        PlacementConfig.from_dict(value)


def test_table_override_inherits_connection_chain():
    value = _config()
    value["tables"] = {"erp::sales": {"max_concurrency": 2}}
    placement = PlacementConfig.from_dict(value)
    policy = placement.policy_for("erp", "sales")
    assert policy.max_concurrency == 2
    assert policy.residency_locations == placement.connections["erp"].residency_locations
    assert policy.cache_storage_profiles == ("cache",)


def test_table_override_inherits_normalized_connection_key():
    value = _config()
    value["connections"][" erp "] = value["connections"].pop("erp")
    value["tables"] = {"erp::sales": {"max_concurrency": 2}}
    assert PlacementConfig.from_dict(value).policy_for("erp", "sales").max_concurrency == 2


def test_runtime_binding_rejects_unconfigured_and_cross_store_dispatch():
    placement = PlacementConfig.from_dict(_config())
    with pytest.raises(ResidencyPolicyViolation, match="FSP_ARTIFACT_STORE_PROFILE"):
        placement.bind_runtime_store("", "azure")
    with pytest.raises(ResidencyPolicyViolation, match="one staging/published"):
        placement.bind_runtime_store("published", "azure")
    with pytest.raises(ResidencyPolicyViolation, match="declared provider"):
        placement.bind_runtime_store("published", "s3")
    value = _config()
    value["connections"]["erp"]["staging_storage_profile"] = "published"
    placement = PlacementConfig.from_dict(value)
    placement.bind_runtime_store("published", "azure")
    placement.validate_dispatch(placement.policy_for("erp", "sales"), connection_id="erp")


@pytest.mark.parametrize("url", [
    "http://endpoint.test", "https://user:secret@endpoint.test", "https:///no-host",
    "https://endpoint.test?token=secret", "https://endpoint.test#secret",
])
def test_serving_url_validation(url):
    value = _config()
    value["serving_endpoints"]["primary"]["url"] = url
    with pytest.raises(ValueError, match="HTTPS"):
        PlacementConfig.from_dict(value)


@pytest.mark.parametrize("mutation", ["store", "override"])
def test_dispatch_rechecks_policy_before_delivering_any_claim(mutation):
    placement = PlacementConfig.from_dict(_config())
    registry = Registry(placement=placement)
    registry.register(RegisterRequest(
        agent_id="eu-agent", host="127.0.0.1", port=9000,
        os="linux", version="test", capabilities=["materializer"], contract_version="1.1",
    ))
    queue = DurableWorkQueue(MemoryStore())
    request = queue.create_request(
        requested_key="metadata", table="sales", epoch=1, table_format="iceberg",
        generation_id="g", generation_fence=1, plan_sha256="a" * 64,
        tasks=[MaterializeTask(
            table="sales", epoch=1, split_index=0, source_table="sales",
            output_key="warehouse/data", connection_id="erp",
        )],
        deadline_ms=int(time.time() * 1000) + 60_000,
    )
    if mutation == "store":
        placement.stores["staging"] = StoreProfile("aws:us-east-1", "s3")
    else:
        placement.tables[("erp", "sales")] = replace(
            placement.connections["erp"], residency_locations=(),
        )
    scheduler = TaskScheduler(queue, registry, manager_owner="m", manager_fence=1)
    assert scheduler.dispatch_once() == 0
    task = queue.get_task(request["task_ids"][0])
    assert task is not None
    assert task["state"] == "QUEUED"
    assert task["dispatch_status"] == "residency_policy_violation"
    status = queue.status()["placement"]
    assert status["residency_policy_violation_count"] == 1
    assert status["residency_policy_violation"][0]["task_id"] == task["task_id"]
    assert not task.get("claim")
    assert registry.list_public()[0]["pending_commands"] == 0

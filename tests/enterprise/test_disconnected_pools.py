from __future__ import annotations

import pytest

from enterprise.control.contract import RegisterRequest
from enterprise.control.placement import PlacementConfig
from enterprise.control.registry import Registry
from enterprise.control.task_scheduler import TaskScheduler
from enterprise.control.work_queue import DurableWorkQueue
from fabric_shortcut_proxy.runtime.artifact_store import MemoryStore
from tests.enterprise.test_work_queue import _create


@pytest.fixture
def queue_clock(monkeypatch):
    now = 1_000_000
    monkeypatch.setattr("enterprise.control.work_queue._now_ms", lambda: now)
    monkeypatch.setattr("enterprise.control.registry._now", lambda: now / 1_000)
    return now


def _placement():
    return {
        "pools": [{
            "pool_id": "wan", "identities": ["edge-agent"], "location": "edge",
            "storage_profile": "central", "allowed_connection_ids": ["source"],
            "heartbeat_ms": 10_000, "heartbeat_miss_limit": 5, "claim_lease_seconds": 45,
        }],
        "connections": {"source": {"required_pool": "wan"}},
    }


def _register(registry):
    return registry.register(RegisterRequest(
        agent_id="edge-agent", host="127.0.0.1", port=9000,
        os="linux", version="test", capabilities=["materializer"], contract_version="1.1",
    ))


def test_pool_heartbeat_interval_and_miss_budget(monkeypatch):
    placement = PlacementConfig.from_dict(_placement())
    registry = Registry(heartbeat_ms=1_000, miss_limit=3, placement=placement)
    monkeypatch.setattr("enterprise.control.registry._now", lambda: 100.0)
    assert _register(registry).heartbeat_ms == 10_000
    monkeypatch.setattr("enterprise.control.registry._now", lambda: 149.0)
    assert registry.is_alive("edge-agent")
    monkeypatch.setattr("enterprise.control.registry._now", lambda: 151.0)
    assert not registry.is_alive("edge-agent")
    assert registry.dead_agents() == ["edge-agent"]


def test_claim_pool_duration_is_preserved_on_renewal_and_capped_by_deadline(queue_clock):
    placement = PlacementConfig.from_dict(_placement())
    registry = Registry(placement=placement)
    registration = _register(registry)
    queue = DurableWorkQueue(MemoryStore(), task_lease_seconds=120)
    now = queue_clock
    _, task_id = _create(queue, deadline_ms=now + 60_000)
    assert TaskScheduler(queue, registry, manager_owner="m", manager_fence=1).dispatch_once() == 1
    task = queue.get_task(task_id)
    assert task is not None
    claim = task["claim"]
    assert claim["lease_seconds"] == 45
    assert claim["expires_at_ms"] - claim["claimed_at_ms"] == 45_000
    assert queue.renew_agent_claims(
        "edge-agent", registration.lease_id, task_ids=[task_id], now_ms=now + 10_000,
    ) == 1
    renewed = queue.get_task(task_id)
    assert renewed is not None
    assert renewed["claim"]["expires_at_ms"] == now + 55_000
    assert queue.renew_agent_claims(
        "edge-agent", registration.lease_id, task_ids=[task_id], now_ms=now + 20_000,
    ) == 1
    renewed = queue.get_task(task_id)
    assert renewed is not None
    assert renewed["claim"]["expires_at_ms"] == now + 60_000


def test_expired_claim_cannot_be_revived_by_late_wan_heartbeat(queue_clock):
    queue = DurableWorkQueue(MemoryStore())
    now = queue_clock
    _, task_id = _create(queue, deadline_ms=now + 60_000)
    claimed = queue.claim_task(
        task_id, agent_id="edge-agent", agent_lease_id="lease", manager_owner="m",
        manager_fence=1, now_ms=now, lease_seconds=10,
    )
    assert queue.renew_agent_claims(
        "edge-agent", "lease", task_ids=[task_id], now_ms=claimed.claim_expires_at_ms,
    ) == 0
    task = queue.get_task(task_id)
    assert task is not None
    assert task["claim"]["expires_at_ms"] == claimed.claim_expires_at_ms
    queue.expire_claims(now_ms=claimed.claim_expires_at_ms + 1)
    task = queue.get_task(task_id)
    assert task is not None
    assert task["state"] == "QUEUED"


@pytest.mark.parametrize("field", ["heartbeat_ms", "heartbeat_miss_limit", "claim_lease_seconds"])
@pytest.mark.parametrize("value", [0, -1, True, "45"])
def test_invalid_pool_timeouts_fail_at_apply(field, value):
    definition = _placement()
    definition["pools"][0][field] = value
    with pytest.raises(ValueError, match=field):
        PlacementConfig.from_dict(definition)

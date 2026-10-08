from __future__ import annotations

import json
import time

import pytest

from fabric_shortcut_proxy.db.read_points import ReadPointDescriptor, TableReadPlan
from fabric_shortcut_proxy.runtime.artifact_store import MemoryStore
from fabric_shortcut_proxy.runtime.generation import (
    BUILD_KEY,
    COORDINATOR_KEY,
    GenerationError,
    PLAN_ACTIVE,
    PLAN_PREPARING,
    PLAN_READY,
    acquire_generation,
    assert_generation_lease,
    fail_generation_plan,
    join_generation,
    publish_generation_plan,
    renew_generation,
)


def _plan(context, table_id="default::sales.orders"):
    descriptor = ReadPointDescriptor(
        version=1,
        provider="fake",
        connection_id="default",
        table_id=table_id,
        owner_shard=0,
        generation_id=context.generation_id,
        generation_fence=context.fence,
        acquired_at_ms=1000,
        expires_at_ms=5000,
        token="provider-token",
    )
    return TableReadPlan(
        version=1,
        table_id=table_id,
        descriptor=descriptor,
        split_count=2,
        split_strategy="range",
        split_key="id",
        split_key_type="long",
        ranges=((1, 51), (51, 101)),
    )


def test_new_coordinator_fences_prior_generation():
    store = MemoryStore()
    prior = acquire_generation(store, 3)
    current = acquire_generation(store, 3)

    assert current.fence == prior.fence + 1
    with pytest.raises(GenerationError, match="fenced"):
        assert_generation_lease(store, prior)
    assert join_generation(store, 3, timeout_seconds=0.1) == current


def test_join_rejects_stale_build_record():
    store = MemoryStore()
    stale = acquire_generation(store, 2)
    acquire_generation(store, 2)
    stale_build = {
        "version": 1,
        "state": "STAGING",
        "generation_id": stale.generation_id,
        "fence": stale.fence,
        "lease_token": stale.lease_token,
        "shard_count": stale.shard_count,
        "expires_at_ms": stale.expires_at_ms,
    }
    store.put(".fsp/generation-build.json", json.dumps(stale_build).encode())

    with pytest.raises(TimeoutError):
        join_generation(store, 2, timeout_seconds=0.1)

    coordinator = json.loads(store.get(COORDINATOR_KEY))
    assert coordinator["fence"] == stale.fence + 1


def test_generation_records_best_effort_source_consistency():
    store = MemoryStore()
    acquired = acquire_generation(store, 2, source_consistency="best_effort")
    joined = join_generation(store, 2, timeout_seconds=0.1)

    assert acquired.source_consistency == "best_effort"
    assert acquired.version == 2
    assert acquired.plan_state == PLAN_READY
    assert acquired.plan_sha256
    assert joined.source_consistency == "best_effort"
    assert json.loads(store.get(COORDINATOR_KEY))["source_consistency"] == "best_effort"


def test_late_worker_joins_active_generation():
    store = MemoryStore()
    acquired = acquire_generation(store, 3)
    store.put(
        ".fsp/generation-build.json",
        json.dumps(
            {
                "version": 2,
                "state": "ACTIVE",
                "generation_id": acquired.generation_id,
                "fence": acquired.fence,
                "lease_token": acquired.lease_token,
                "plan_sha256": acquired.plan_sha256,
                "object_count": 5,
                "index_sha256": "abc123",
            }
        ).encode(),
    )

    joined = join_generation(store, 3, timeout_seconds=0.1)
    assert joined.generation_id == acquired.generation_id
    assert joined.plan_state == PLAN_ACTIVE


def test_version_one_generation_records_remain_joinable():
    store = MemoryStore()
    record = {
        "version": 1,
        "generation_id": "legacy-generation",
        "fence": 2,
        "lease_token": "legacy-token",
        "shard_count": 2,
        "expires_at_ms": 4102444800000,
        "source_consistency": "best_effort",
    }
    store.put(COORDINATOR_KEY, json.dumps(record).encode())
    store.put(BUILD_KEY, json.dumps({**record, "state": "STAGING"}).encode())

    joined = join_generation(store, 2, timeout_seconds=0.1)

    assert joined.version == 1
    assert joined.plan_state == PLAN_READY
    assert joined.table_plans == ()


def test_workers_wait_until_version_two_plan_is_ready():
    store = MemoryStore()
    preparing = acquire_generation(store, 2, prepare_plan=True)

    assert preparing.plan_state == PLAN_PREPARING
    with pytest.raises(TimeoutError):
        join_generation(store, 2, timeout_seconds=0.1)

    ready = publish_generation_plan(store, preparing, [_plan(preparing)])

    assert ready.plan_state == PLAN_READY
    assert join_generation(store, 2, timeout_seconds=0.1) == ready


def test_plan_publish_rejects_duplicate_tables_and_wrong_generation():
    store = MemoryStore()
    preparing = acquire_generation(store, 2, prepare_plan=True)
    plan = _plan(preparing)

    with pytest.raises(GenerationError, match="duplicate"):
        publish_generation_plan(store, preparing, [plan, plan])

    wrong_descriptor = ReadPointDescriptor(
        **{
            **plan.descriptor.__dict__,
            "generation_id": "other-generation",
        }
    )
    wrong_plan = TableReadPlan(
        **{
            **plan.__dict__,
            "descriptor": wrong_descriptor,
        }
    )
    with pytest.raises(GenerationError, match="different generation"):
        publish_generation_plan(store, preparing, [wrong_plan])


def test_renew_preserves_ready_plan_and_build_state():
    store = MemoryStore()
    preparing = acquire_generation(store, 2, prepare_plan=True)
    ready = publish_generation_plan(store, preparing, [_plan(preparing)])

    renewed = renew_generation(store, ready, lease_seconds=600)
    build = json.loads(store.get(BUILD_KEY))

    assert renewed.table_plans == ready.table_plans
    assert renewed.plan_sha256 == ready.plan_sha256
    assert build["state"] == PLAN_READY
    assert build["plan_sha256"] == ready.plan_sha256
    assert len(build["table_plans"]) == 1


def test_failed_plan_is_redacted_and_join_fails_immediately():
    store = MemoryStore()
    preparing = acquire_generation(store, 2, prepare_plan=True)
    ready = publish_generation_plan(store, preparing, [_plan(preparing)])

    fail_generation_plan(
        store,
        ready,
        f"provider failed for provider-token and {ready.lease_token}",
    )
    failure = json.loads(store.get(BUILD_KEY))

    assert "lease_token" not in failure
    assert "table_plans" not in failure
    assert failure["state"] == "FAILED"
    assert "provider-token" not in failure["error"]
    assert ready.lease_token not in failure["error"]
    assert failure["error"].count("[REDACTED]") == 2
    with pytest.raises(GenerationError, match="planning failed"):
        join_generation(store, 2, timeout_seconds=0.1)


def _expire_lease(store):
    record = json.loads(store.get(COORDINATOR_KEY))
    record["expires_at_ms"] = int(time.time() * 1000) - 60_000
    store.put(COORDINATOR_KEY, json.dumps(record).encode())


def _write_build(store, context, state):
    store.put(
        BUILD_KEY,
        json.dumps(
            {
                "version": 2,
                "state": state,
                "generation_id": context.generation_id,
                "fence": context.fence,
                "lease_token": context.lease_token,
                "plan_sha256": context.plan_sha256,
            }
        ).encode(),
    )


def test_late_worker_joins_active_generation_after_lease_expired():
    store = MemoryStore()
    acquired = acquire_generation(store, 3)
    _write_build(store, acquired, "ACTIVE")
    _expire_lease(store)

    joined = join_generation(store, 3, timeout_seconds=0.1)
    assert joined.generation_id == acquired.generation_id


def test_join_rejects_expired_lease_for_unfinished_build():
    store = MemoryStore()
    acquired = acquire_generation(store, 3)
    _write_build(store, acquired, "STAGING")
    _expire_lease(store)

    with pytest.raises(TimeoutError):
        join_generation(store, 3, timeout_seconds=0.3)


def test_renewal_with_stale_context_after_external_renewal():
    store = MemoryStore()
    acquired = acquire_generation(store, 1)
    renew_generation(store, acquired, lease_seconds=600)

    renewed = renew_generation(store, acquired, lease_seconds=3600)
    assert renewed.generation_id == acquired.generation_id
    assert renewed.expires_at_ms > int(time.time() * 1000) + 3000_000

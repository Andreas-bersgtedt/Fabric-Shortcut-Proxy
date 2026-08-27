from __future__ import annotations

import json

import pytest

from runtime.artifact_store import MemoryStore
from runtime.generation import (
    COORDINATOR_KEY,
    GenerationError,
    acquire_generation,
    assert_generation_lease,
    join_generation,
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
    assert joined.source_consistency == "best_effort"
    assert json.loads(store.get(COORDINATOR_KEY))["source_consistency"] == "best_effort"

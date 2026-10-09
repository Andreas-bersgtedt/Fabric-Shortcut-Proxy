from __future__ import annotations

from dataclasses import replace
import json

import httpx
import pytest

from enterprise.control.contract import SnapshotManifest, SplitRef
from enterprise.control.placement import PlacementConfig
from enterprise.control.publication_health import dataset_freshness
from enterprise.control.work_queue import DurableWorkQueue, SNAPSHOTS_PREFIX, WorkQueueConflict
from fabric_shortcut_proxy.observability import metrics
from fabric_shortcut_proxy import config
from fabric_shortcut_proxy.runtime.artifact_store import MemoryStore
from tests.enterprise.test_work_queue import _claim, _create, _parquet, _result


@pytest.fixture
def publication(monkeypatch):
    monkeypatch.setattr("enterprise.control.work_queue._now_ms", lambda: 1_000)
    store = MemoryStore()
    queue = DurableWorkQueue(store)
    request, task_id = _create(queue, deadline_ms=60_000)
    claim = _claim(queue, task_id, now_ms=1_000)
    data = _parquet()
    store.put(claim.output_key, data)
    result = _result(claim, data)
    assert queue.accept_result(result, agent_lease_id="lease-1", now_ms=1_100).ok
    manifest = SnapshotManifest(
        table="sales", epoch=7, table_format="iceberg",
        splits=[SplitRef(claim.output_key, result.size_bytes, result.record_count, result.content_hash)],
        generation_id="generation-4", generation_fence=4, plan_sha256="a" * 64,
        published_at_ms=1_200, request_id=request["request_id"],
    )
    queue.publish_snapshot(request["request_id"], manifest)
    placement = PlacementConfig.from_dict({
        "connections": {"source": {"freshness_target_ms": 5_000}},
    })
    return queue, store, request, manifest, placement


def test_production_metadata_survives_restart_and_late_republication(publication, monkeypatch):
    queue, store, request, manifest, _ = publication
    persisted = queue.list_snapshot_manifests()[0]
    assert persisted.dataset_id == "source::sales"
    assert persisted.produced_at_ms == request["created_at_ms"] == 1_000
    monkeypatch.setattr("enterprise.control.work_queue._now_ms", lambda: 30_000)
    queue.publish_snapshot(request["request_id"], replace(manifest, published_at_ms=30_000))
    restarted = DurableWorkQueue(store)
    snapshot = restarted.get_snapshot("sales")
    assert snapshot is not None
    assert snapshot.produced_at_ms == 1_000


def test_site_outage_marks_stale_without_changing_last_readable_generation(publication):
    queue, store, _, _, placement = publication
    original = queue.get_snapshot("sales")
    assert original is not None
    bodies = {ref.object_key: store.get(ref.object_key) for ref in original.splits}
    assert not dataset_freshness(queue, placement, now_ms=6_000)[0]["stale"]
    stale = dataset_freshness(DurableWorkQueue(store), placement, now_ms=6_001)[0]
    assert stale["stale"] and stale["age_seconds"] == 5.001
    assert queue.get_snapshot("sales") == original
    assert bodies == {ref.object_key: store.get(ref.object_key) for ref in original.splits}


@pytest.mark.parametrize("changes", [
    {"produced_at_ms": 30_000},
    {"dataset_id": "another::sales"},
])
def test_publication_cannot_forge_dataset_or_refresh_production_timestamp(publication, changes):
    queue, _, request, manifest, _ = publication
    with pytest.raises(WorkQueueConflict, match="does not match request"):
        queue.publish_snapshot(request["request_id"], replace(manifest, **changes))
    assert queue.list_snapshot_manifests()[0].produced_at_ms == 1_000


def test_legacy_manifest_does_not_invent_production_time(publication):
    queue, store, _, _, placement = publication
    key = store.list(f"{SNAPSHOTS_PREFIX}/")[0].key
    record = json.loads(store.get(key))
    del record["manifest"]["produced_at_ms"]
    store.put(key, json.dumps(record).encode())
    sample = dataset_freshness(queue, placement, now_ms=6_001)[0]
    assert sample["produced_at_ms"] is None
    assert sample["age_seconds"] is None
    assert sample["stale"]


def test_future_timestamp_is_unknown_not_a_negative_fresh_age(publication):
    queue, store, _, _, placement = publication
    key = store.list(f"{SNAPSHOTS_PREFIX}/")[0].key
    record = json.loads(store.get(key))
    record["manifest"]["produced_at_ms"] = 7_000
    store.put(key, json.dumps(record).encode())
    samples = dataset_freshness(queue, placement, now_ms=6_001)
    metrics.reset()
    try:
        metrics.record_dataset_freshness(samples)
        gauges = metrics.snapshot()["gauges"]
        assert gauges["fsp_dataset_age_seconds"] == []
        assert gauges["fsp_dataset_production_timestamp_known"][0]["value"] == 0
        assert gauges["fsp_dataset_stale"][0]["value"] == 1
    finally:
        metrics.reset()


def test_unpublished_dataset_with_target_is_explicitly_unknown_and_stale():
    placement = PlacementConfig.from_dict({
        "tables": {"source::sales": {"freshness_target_ms": 5_000}},
    })
    samples = dataset_freshness(DurableWorkQueue(MemoryStore()), placement, now_ms=6_000)
    assert len(samples) == 1
    assert samples[0]["dataset_id"] == "source::sales"
    assert samples[0]["stale"] and samples[0]["age_seconds"] is None


def test_no_target_preserves_legacy_behavior(publication):
    queue, _, _, _, _ = publication
    assert dataset_freshness(queue, PlacementConfig(), now_ms=6_001) == []


def test_health_reads_only_control_metadata(publication, monkeypatch):
    queue, store, _, _, placement = publication
    original_get = store.get

    def metadata_only(key, **kwargs):
        assert key.startswith(f"{SNAPSHOTS_PREFIX}/")
        return original_get(key, **kwargs)

    monkeypatch.setattr(store, "get", metadata_only)
    assert dataset_freshness(queue, placement, now_ms=6_001)[0]["stale"]


async def test_manager_metrics_and_queue_status_report_staleness(publication, monkeypatch):
    _, store, _, _, _ = publication
    from enterprise.control.manager_app import create_manager_app

    monkeypatch.setenv("AGENT_PLACEMENT_CONFIG", json.dumps({
        "connections": {"source": {"freshness_target_ms": 5_000}},
    }))
    monkeypatch.delenv("FSP_ARTIFACT_STORE_PROFILE", raising=False)
    monkeypatch.setattr("fabric_shortcut_proxy.runtime.artifact_store.get_default_store", lambda: store)
    monkeypatch.setattr("enterprise.control.publication_health.time.time_ns", lambda: 6_001_000_000)
    monkeypatch.setattr(config, "MANAGER_HA", False)
    monkeypatch.setattr(config, "MATERIALIZATION_WORK_QUEUE", False)
    monkeypatch.setattr(config, "ENABLE_GATEWAY", False)
    monkeypatch.setattr(config, "MANAGER_AUTH_ENABLED", True)
    monkeypatch.setattr(config, "MANAGER_AUTH_USERNAME", "operator")
    monkeypatch.setattr(config, "MANAGER_AUTH_PASSWORD", "test-password")
    app = create_manager_app()
    metrics.reset()
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://manager",
            auth=("operator", "test-password"),
        ) as client:
            response = await client.get("/metrics")
            assert response.status_code == 200
            assert 'fsp_dataset_stale{dataset_id="source::sales",location="unknown"} 1.0' in response.text
            response = await client.get("/control/work-queue")
            assert response.status_code == 200
            sample = response.json()["dataset_freshness"][0]
            assert sample["stale"]
            assert sample["produced_at_ms"] == 1_000
    finally:
        metrics.reset()


def test_freshness_metrics_refresh_age_on_scrape_and_remove_retired_datasets(publication):
    queue, _, _, _, placement = publication
    metrics.reset()
    try:
        metrics.record_dataset_freshness(dataset_freshness(queue, placement, now_ms=6_000))
        assert 'fsp_dataset_stale{dataset_id="source::sales",location="unknown"} 0.0' in metrics.render_prometheus()
        metrics.record_dataset_freshness(dataset_freshness(queue, placement, now_ms=6_001))
        assert metrics.snapshot()["gauges"]["fsp_dataset_stale"][0]["value"] == 1
        metrics.record_dataset_freshness([])
        for name, series in metrics.snapshot()["gauges"].items():
            if name.startswith("fsp_dataset_"):
                assert series == []
    finally:
        metrics.reset()


@pytest.mark.parametrize("value", [0, -1, True, "5000"])
def test_invalid_freshness_target_rejected_at_policy_apply(value):
    with pytest.raises(ValueError, match="freshness_target_ms"):
        PlacementConfig.from_dict({"connections": {"source": {"freshness_target_ms": value}}})

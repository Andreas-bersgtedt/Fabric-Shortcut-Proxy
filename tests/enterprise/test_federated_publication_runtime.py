from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import json
import threading
import io

import pytest

from enterprise.control.contract import SnapshotManifest, SplitRef
from enterprise.control.federation import (
    CatalogConflict, FederatedPublicationCoordinator, GlobalCatalog,
    RegionalReplicaVerifier,
)
from enterprise.control.placement import PlacementConfig
from enterprise.control.work_queue import DurableWorkQueue, SNAPSHOTS_PREFIX
from fabric_shortcut_proxy.runtime.artifact_store import MemoryStore
from tests.enterprise.test_catalog_service import _snapshot
from tests.enterprise.test_residency_policy import _config
from tests.enterprise.test_work_queue import _claim, _create, _parquet, _result


def _runtime():
    value = _config()
    value["connections"]["source"] = value["connections"].pop("erp")
    for pool in value["pools"]:
        pool["allowed_connection_ids"] = ["source"]
    placement = PlacementConfig.from_dict(value)
    metadata = MemoryStore()
    stores = {"published": MemoryStore(), "replica": MemoryStore()}
    catalog = GlobalCatalog(metadata, placement)
    catalog.create(
        "source::sales", owner_id="manager-swc",
        owner_location="azure:swedencentral",
        lease_ms=100_000, freshness_target_ms=5_000,
    )
    verifier = RegionalReplicaVerifier(placement, stores)
    primary = FederatedPublicationCoordinator(
        catalog, verifier, stores["published"], owner_id="manager-swc",
        owner_location="azure:swedencentral", storage_profile="published",
        lease_ms=100_000,
    )
    secondary = FederatedPublicationCoordinator(
        catalog, verifier, stores["replica"], owner_id="manager-neu",
        owner_location="azure:northeurope", storage_profile="replica",
        lease_ms=100_000,
    )
    return catalog, metadata, stores, primary, secondary


def _ready_request(queue):
    request, task_id = _create(queue)
    claim = _claim(queue, task_id)
    data = _parquet()
    queue.store.put(claim.output_key, data)
    result = _result(claim, data)
    assert queue.accept_result(result, agent_lease_id="lease-1").ok
    manifest = SnapshotManifest(
        table="sales", epoch=7, table_format="iceberg",
        splits=[SplitRef(claim.output_key, len(data), 3, result.content_hash)],
        generation_id=request["generation_id"],
        generation_fence=request["generation_fence"],
        plan_sha256=request["plan_sha256"],
        published_at_ms=request["created_at_ms"],
        request_id=request["request_id"],
    )
    return request, manifest


def test_queue_publication_racing_handover_has_no_local_visibility():
    catalog, metadata, stores, primary, _ = _runtime()
    queue = DurableWorkQueue(stores["published"])
    queue.configure_federation(primary, {"sales": "source::sales"})
    request, manifest = _ready_request(queue)
    ready, resume = threading.Event(), threading.Event()
    original = metadata.compare_and_swap

    def delayed(key: str, expected: bytes | None, data: bytes) -> bool:
        if json.loads(data)["replicas"]:
            ready.set()
            assert resume.wait(5)
        return original(key, expected, data)

    metadata.compare_and_swap = delayed
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(queue.publish_snapshot, request["request_id"], manifest)
        try:
            assert ready.wait(5)
            catalog.handover(
                "source::sales", owner_id="manager-swc", fence=1,
                new_owner_id="manager-neu", new_location="azure:northeurope",
                lease_ms=100_000,
            )
        finally:
            resume.set()
        with pytest.raises(CatalogConflict, match="concurrently"):
            future.result()
    assert queue.get_snapshot("sales") is None
    retained = queue.get_request(request["request_id"])
    assert retained is not None and not retained.get("published")
    assert list(stores["published"].list(f"{SNAPSHOTS_PREFIX}/")) == []
    assert catalog.get("source::sales")["replicas"] == {}


def test_queue_serves_only_catalog_commit_after_restart_and_replication():
    catalog, _, stores, primary, secondary = _runtime()
    queue = DurableWorkQueue(stores["published"])
    queue.configure_federation(primary, {"sales": "source::sales"})
    request, manifest = _ready_request(queue)
    queue.publish_snapshot(request["request_id"], manifest)
    assert list(stores["published"].list(f"{SNAPSHOTS_PREFIX}/")) == []
    assert primary.replicate_owned() == 1
    assert primary.replicate_owned() == 0
    restarted = DurableWorkQueue(stores["replica"])
    restarted.configure_federation(secondary, {"sales": "source::sales"})
    snapshot = restarted.get_snapshot("sales", 7)
    assert snapshot is not None
    assert snapshot.dataset_id == "source::sales"
    assert snapshot.request_id == request["request_id"]
    assert stores["replica"].get(snapshot.splits[0].object_key) == _parquet()
    assert restarted.get_snapshot("sales", 99) is None
    assert catalog.resolve("source::sales", "secondary")["storage_profile"] == "replica"

def test_catalog_commit_survives_queue_bookkeeping_failure(monkeypatch):
    _, _, stores, primary, _ = _runtime()
    queue = DurableWorkQueue(stores["published"])
    queue.configure_federation(primary, {"sales": "source::sales"})
    request, manifest = _ready_request(queue)
    original = queue._write

    def failed_bookkeeping(key, value):
        if value.get("published"):
            raise OSError("queue bookkeeping unavailable")
        return original(key, value)

    monkeypatch.setattr(queue, "_write", failed_bookkeeping)
    with pytest.raises(OSError, match="bookkeeping"):
        queue.publish_snapshot(request["request_id"], manifest)
    restarted = DurableWorkQueue(stores["published"])
    restarted.configure_federation(primary, {"sales": "source::sales"})
    snapshot = restarted.get_snapshot("sales")
    assert snapshot is not None and snapshot.request_id == request["request_id"]
    assert primary.replicate_owned() == 1


def test_federated_pruning_retains_previous_committed_staging():
    _, _, stores, primary, _ = _runtime()
    queue = DurableWorkQueue(stores["published"])
    queue.configure_federation(primary, {"sales": "source::sales"})
    request, manifest = _ready_request(queue)
    queue.publish_snapshot(request["request_id"], manifest)
    task = queue.get_task(request["task_ids"][0])
    assert task is not None
    output_key = task["task"]["output_key"]
    assert queue.prune_staging(request["generation_id"], [output_key]) == 0
    assert stores["published"].get(manifest.splits[0].object_key) == _parquet()


def test_replication_disconnect_retains_old_snapshot_then_recovers(monkeypatch):
    catalog, _, stores, primary, secondary = _runtime()
    first = replace(_snapshot(stores["published"], 1), dataset_id="source::sales")
    second = replace(_snapshot(stores["published"], 2), dataset_id="source::sales")
    primary.publish(first)
    primary.replicate_owned()
    primary.publish(second)
    snapshot = secondary.snapshot("source::sales", "sales")
    assert snapshot is not None and snapshot.generation_id == "generation-1"
    original = stores["replica"].put_stream_if_absent

    def offline(*_args, **_kwargs):
        raise OSError("simulated replica site disconnected")

    monkeypatch.setattr(stores["replica"], "put_stream_if_absent", offline)
    with pytest.raises(OSError, match="disconnected"):
        primary.replicate_owned()
    old = secondary.snapshot("source::sales", "sales")
    assert old is not None
    assert old.generation_id == "generation-1"
    assert stores["replica"].get(old.splits[0].object_key) == b"generation-1"
    assert catalog.resolve("source::sales", "secondary", now_ms=14_002)["stale"]
    monkeypatch.setattr(stores["replica"], "put_stream_if_absent", original)
    assert primary.replicate_owned() == 1
    snapshot = secondary.snapshot("source::sales", "sales")
    assert snapshot is not None and snapshot.generation_id == "generation-2"


def test_handover_during_replication_cannot_advertise(monkeypatch):
    catalog, _, stores, primary, secondary = _runtime()
    first = replace(_snapshot(stores["published"], 1), dataset_id="source::sales")
    primary.publish(first)
    original = primary.verifier.replicate

    def handover(*args):
        receipt = original(*args)
        catalog.handover(
            "source::sales", owner_id="manager-swc", fence=1,
            new_owner_id="manager-neu", new_location="azure:northeurope",
            lease_ms=100_000,
        )
        return receipt

    monkeypatch.setattr(primary.verifier, "replicate", handover)
    with pytest.raises(CatalogConflict, match="stale"):
        primary.replicate_owned()
    assert secondary.snapshot("source::sales", "sales") is None


@pytest.mark.parametrize("table_format", ["iceberg", "delta"])
def test_immutable_publication_metadata_references_exact_snapshot(monkeypatch, table_format):
    from enterprise.control.materialize_service import _publish_snapshot_objects
    from fabric_shortcut_proxy import config
    from fabric_shortcut_proxy.iceberg.state_store import SnapshotState, SplitDescriptor
    from fabric_shortcut_proxy.runtime import artifact_store
    from fastavro import reader

    store = MemoryStore()
    monkeypatch.setattr(artifact_store, "build_store", lambda *_args, **_kwargs: store)
    monkeypatch.setattr(config, "TABLE_FORMAT", table_format)
    monkeypatch.setattr(config, "ICEBERG_SNAPSHOT_HISTORY", True)
    table = config.TableDef(name="sales", source_table="sales", schema=config.TABLE_SCHEMA)
    data_key = "warehouse/sales/.fsp/staging/task/part.parquet"
    snap = SnapshotState(
        snapshot_id=7, sequence_number=7, watermark_ms=123,
        manifest_list_key="warehouse/sales/metadata/list.avro",
        manifest_file_key="warehouse/sales/metadata/manifest.avro",
        metadata_key="warehouse/sales/metadata/v7.metadata.json",
        version_hint_key="warehouse/sales/metadata/version-hint.text",
        table=table, table_path="warehouse/sales", legacy_table_path="",
        version=7,
        splits=[SplitDescriptor(0, 1, data_key, 123, table, record_count=3, file_size_in_bytes=12)],
    )
    _, keys = _publish_snapshot_objects(snap, "request-1")
    first_bytes = {key: store.get(key) for key in keys}
    assert all("/.fsp/publications/" in key for key in keys)
    assert not store.exists(snap.metadata_key)
    if table_format == "iceberg":
        metadata = json.loads(next(value for key, value in first_bytes.items() if key.endswith(".json")))
        assert len(metadata["snapshots"]) == 1 and metadata["metadata-log"] == []
        list_key = metadata["snapshots"][0]["manifest-list"].split("/", 3)[3]
        entry = next(iter(reader(io.BytesIO(store.get(list_key)))))
        assert isinstance(entry, dict)
        manifest_key = entry["manifest_path"].split("/", 3)[3]
        assert list_key in keys and manifest_key in keys
        data_entry = next(iter(reader(io.BytesIO(store.get(manifest_key)))))
        assert isinstance(data_entry, dict)
        assert data_entry["data_file"]["file_path"].endswith(data_key)
    else:
        assert len(keys) == 1 and keys[0].endswith("/_delta_log/00000000000000000000.json")
        actions = [json.loads(line) for line in store.get(keys[0]).splitlines()]
        assert [action["add"]["path"] for action in actions if "add" in action] == [
            f"s3://{config.BUCKET_NAME}/{data_key}"
        ]
    _, repeated = _publish_snapshot_objects(snap, "request-1")
    assert repeated == keys
    assert {key: store.get(key) for key in keys} == first_bytes
    _, newer = _publish_snapshot_objects(snap, "request-2")
    assert set(keys).isdisjoint(newer)
    assert {key: store.get(key) for key in keys} == first_bytes

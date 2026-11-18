from dataclasses import replace
import hashlib

import pytest

from enterprise.control.edge_journal import EdgeJournal, EdgeMetadata
from enterprise.control.edge_transfer import EdgeTransferConflict, copy_edge_generation
from enterprise.control.federation import CatalogConflict, EdgeReplay
from fabric_shortcut_proxy.runtime.artifact_store import LocalDirStore, ObjectConflict
from tests.enterprise.test_catalog_service import _snapshot
from tests.enterprise.test_federated_publication_runtime import _runtime


def test_local_journal_replays_through_regional_catalog_after_restart(tmp_path):
    catalog, _, stores, primary, secondary = _runtime()
    local = LocalDirStore(str(tmp_path / "edge-data"))
    journal_path = tmp_path / "edge-journal.db"
    journal = EdgeJournal(journal_path, dataset_id="source::sales")
    for generation in range(1, 4):
        manifest = replace(_snapshot(local, generation), dataset_id="source::sales")
        metadata = []
        for key in manifest.metadata_keys:
            data = local.get(key)
            metadata.append(EdgeMetadata(key, len(data), hashlib.sha256(data).hexdigest()))
        journal.append(manifest, metadata=tuple(metadata))
    assert catalog.get("source::sales")["replicas"] == {}

    restarted = EdgeJournal(journal_path, dataset_id="source::sales")
    published = []
    for generation in range(1, 4):
        entry = restarted.next_pending()
        assert entry is not None
        assert entry.sequence == generation
        manifest = entry.manifest
        for item in entry.metadata:
            assert local.verify(item.key, size=item.size_bytes, content_hash=item.sha256)
        assert copy_edge_generation(
            entry, local, stores["published"],
            dataset_id="source::sales", allowed_prefix="published",
        ) == len(manifest.splits) + len(entry.metadata)
        replay = EdgeReplay("site-1", entry.sequence, entry.manifest_sha256)
        if generation == 1:
            with pytest.raises(CatalogConflict, match="another regional Manager"):
                secondary.publish(manifest, edge_replay=replay)
        record = primary.publish(manifest, edge_replay=replay)
        published.append(record["replicas"]["published"]["generation_id"])
        if generation == 1:
            restarted = EdgeJournal(journal_path, dataset_id="source::sales")
            retry = restarted.next_pending()
            assert retry is not None
            assert retry == entry
            assert copy_edge_generation(
                retry, local, stores["published"],
                dataset_id="source::sales", allowed_prefix="published",
            ) == len(manifest.splits) + len(entry.metadata)
            assert primary.publish(retry.manifest, edge_replay=replay) == record
        restarted.acknowledge(entry.sequence, manifest.generation_id, entry.manifest_sha256)
    assert published == ["generation-1", "generation-2", "generation-3"]
    assert restarted.next_pending() is None


@pytest.mark.parametrize("object_type", ["split", "metadata"])
def test_transfer_rejects_changed_objects_before_copy(tmp_path, object_type):
    source = LocalDirStore(str(tmp_path / "source"))
    destination = LocalDirStore(str(tmp_path / "destination"))
    manifest = replace(_snapshot(source, 1), dataset_id="source::sales")
    key = manifest.metadata_keys[0]
    data = source.get(key)
    journal = EdgeJournal(tmp_path / "edge.db", dataset_id="source::sales")
    journal.append(manifest, metadata=(EdgeMetadata(key, len(data), hashlib.sha256(data).hexdigest()),))
    entry = journal.next_pending()
    assert entry is not None
    corrupt_key = key if object_type == "metadata" else manifest.splits[0].object_key
    source.put(corrupt_key, b"changed")
    with pytest.raises(EdgeTransferConflict, match="edge object failed verification"):
        copy_edge_generation(
            entry, source, destination,
            dataset_id="source::sales", allowed_prefix="published",
        )
    assert list(destination.list("published/")) == []
    assert journal.next_pending() == entry


def test_transfer_rejects_changed_record_and_outside_prefix(tmp_path):
    source = LocalDirStore(str(tmp_path / "source"))
    destination = LocalDirStore(str(tmp_path / "destination"))
    manifest = replace(_snapshot(source, 1), dataset_id="source::sales")
    key = manifest.metadata_keys[0]
    data = source.get(key)
    journal = EdgeJournal(tmp_path / "edge.db", dataset_id="source::sales")
    journal.append(manifest, metadata=(EdgeMetadata(key, len(data), hashlib.sha256(data).hexdigest()),))
    entry = journal.next_pending()
    assert entry is not None
    with pytest.raises(EdgeTransferConflict, match="integrity check"):
        copy_edge_generation(
            replace(entry, manifest=replace(entry.manifest, table="another")),
            source, destination, dataset_id="source::sales", allowed_prefix="published",
        )
    with pytest.raises(EdgeTransferConflict, match="outside its allowed prefix"):
        copy_edge_generation(
            entry, source, destination,
            dataset_id="source::sales", allowed_prefix="another",
        )
    with pytest.raises(EdgeTransferConflict, match="another dataset"):
        copy_edge_generation(
            entry, source, destination,
            dataset_id="source::another", allowed_prefix="published",
        )
    assert list(destination.list("published/")) == []


def test_transfer_does_not_replace_a_conflicting_destination_object(tmp_path):
    source = LocalDirStore(str(tmp_path / "source"))
    destination = LocalDirStore(str(tmp_path / "destination"))
    manifest = replace(_snapshot(source, 1), dataset_id="source::sales")
    key = manifest.metadata_keys[0]
    data = source.get(key)
    journal = EdgeJournal(tmp_path / "edge.db", dataset_id="source::sales")
    journal.append(manifest, metadata=(EdgeMetadata(key, len(data), hashlib.sha256(data).hexdigest()),))
    entry = journal.next_pending()
    assert entry is not None
    split_key = manifest.splits[0].object_key
    destination.put(split_key, b"different")
    with pytest.raises(ObjectConflict):
        copy_edge_generation(
            entry, source, destination,
            dataset_id="source::sales", allowed_prefix="published",
        )
    assert destination.get(split_key) == b"different"
    assert journal.next_pending() == entry


def test_transfer_detects_source_change_during_copy(tmp_path, monkeypatch):
    source = LocalDirStore(str(tmp_path / "source"))
    destination = LocalDirStore(str(tmp_path / "destination"))
    manifest = replace(_snapshot(source, 1), dataset_id="source::sales")
    key = manifest.metadata_keys[0]
    data = source.get(key)
    journal = EdgeJournal(tmp_path / "edge.db", dataset_id="source::sales")
    journal.append(manifest, metadata=(EdgeMetadata(key, len(data), hashlib.sha256(data).hexdigest()),))
    entry = journal.next_pending()
    assert entry is not None
    split = manifest.splits[0]
    original = destination.put_stream_if_absent

    def replace_before_copy(key, stream, *, length=None):
        if key == split.object_key:
            source.put(key, b"x" * split.size_bytes)
        return original(key, stream, length=length)

    monkeypatch.setattr(destination, "put_stream_if_absent", replace_before_copy)
    with pytest.raises(EdgeTransferConflict, match="copied object failed verification"):
        copy_edge_generation(
            entry, source, destination,
            dataset_id="source::sales", allowed_prefix="published",
        )
    assert journal.next_pending() == entry

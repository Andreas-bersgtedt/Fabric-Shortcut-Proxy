import base64
from dataclasses import replace
import hashlib
import json

import pytest

from enterprise.control.contract import SnapshotManifest, SplitRef
from enterprise.control.edge_journal import EdgeJournal, EdgeMetadata, encode_edge_record
from enterprise.control.edge_receiver import EdgeBinding, EdgeReplayReceiver
from enterprise.control.edge_transfer import EdgeTransferConflict
from enterprise.control.federation import CatalogConflict
from tests.enterprise.test_federated_publication_runtime import _runtime


def _generation(tmp_path, number):
    generation_id = f"{number:032x}"
    prefix = "warehouse/sales"
    split_key = f"{prefix}/.fsp/edge-staging/{generation_id}/part-0.parquet"
    metadata_prefix = (
        f"{prefix}/.fsp/publications/{hashlib.sha256(generation_id.encode()).hexdigest()}"
    )
    objects = {split_key: f"split-{number}".encode()}
    for name in ("metadata.json", "manifest-list.avro", "manifest.avro", "version-hint.text"):
        objects[f"{metadata_prefix}/{name}"] = f"{name}-{number}".encode()
    data = objects[split_key]
    metadata = tuple(
        EdgeMetadata(key, len(value), hashlib.sha256(value).hexdigest())
        for key, value in objects.items() if key != split_key
    )
    manifest = SnapshotManifest(
        table="sales", epoch=number, table_format="iceberg",
        splits=[SplitRef(split_key, len(data), 1, hashlib.sha256(data).hexdigest())],
        metadata_keys=[item.key for item in metadata],
        generation_id=generation_id, request_id=generation_id,
        dataset_id="source::sales", produced_at_ms=1000 + number,
    )
    journal = EdgeJournal(tmp_path / f"edge-{number}.db", dataset_id="source::sales")
    journal.append(manifest, metadata=metadata)
    entry = journal.next_pending()
    assert entry is not None
    return {
        "sequence": number,
        "manifest_sha256": entry.manifest_sha256,
        "record": json.loads(encode_edge_record(entry.manifest, entry.metadata)),
        "objects": {key: base64.b64encode(value).decode() for key, value in objects.items()},
    }


def test_receiver_publishes_ordered_generations_and_retries(tmp_path):
    catalog, _, stores, primary, _ = _runtime()
    receiver = EdgeReplayReceiver(
        primary, {"site-1": EdgeBinding("source::sales", "sales", "warehouse/sales")}
    )
    for number in range(1, 4):
        payload = _generation(tmp_path, number)
        ack = receiver.receive("site-1", payload)
        assert ack["sequence"] == number
        record = catalog.get("source::sales")
        assert record["edge_replay"]["sequence"] == number
        assert record["replicas"]["published"]["generation_id"] == f"{number:032x}"
        assert receiver.receive("site-1", payload) == ack
        assert catalog.get("source::sales") == record
    assert len(stores["published"].list("warehouse/sales/")) == 15


def test_receiver_rejects_bad_identity_gap_and_tampered_bytes(tmp_path):
    catalog, _, stores, primary, _ = _runtime()
    receiver = EdgeReplayReceiver(
        primary, {"site-1": EdgeBinding("source::sales", "sales", "warehouse/sales")}
    )
    first = _generation(tmp_path, 1)
    with pytest.raises(PermissionError):
        receiver.receive("site-2", first)
    with pytest.raises(CatalogConflict, match="start at one"):
        receiver.receive("site-1", _generation(tmp_path, 2))
    changed = json.loads(json.dumps(first))
    metadata_key = changed["record"]["manifest"]["metadata_keys"][0]
    changed["objects"][metadata_key] = base64.b64encode(b"tampered").decode()
    with pytest.raises(EdgeTransferConflict, match="failed verification"):
        receiver.receive("site-1", changed)
    assert stores["published"].list("warehouse/sales/") == []
    assert catalog.get("source::sales")["replicas"] == {}


def test_receiver_rejects_unauthorized_namespace_and_stale_owner(tmp_path):
    catalog, _, stores, primary, secondary = _runtime()
    binding = EdgeBinding("source::sales", "sales", "warehouse/sales")
    payload = _generation(tmp_path, 1)
    with pytest.raises(ValueError, match="namespace"):
        EdgeReplayReceiver(
            primary, {"site-1": replace(binding, key_prefix="warehouse/other")}
        ).receive("site-1", payload)
    with pytest.raises(CatalogConflict, match="another regional Manager"):
        EdgeReplayReceiver(secondary, {"site-1": binding}).receive("site-1", payload)
    assert stores["published"].list("warehouse/sales/") == []
    assert catalog.get("source::sales")["replicas"] == {}


def test_receiver_does_not_acknowledge_handover_during_copy(tmp_path, monkeypatch):
    catalog, _, stores, primary, _ = _runtime()
    receiver = EdgeReplayReceiver(
        primary, {"site-1": EdgeBinding("source::sales", "sales", "warehouse/sales")}
    )
    original = stores["published"].put_stream_if_absent
    changed = False

    def handover_then_copy(key, stream, *, length=None):
        nonlocal changed
        if not changed:
            changed = True
            catalog.handover(
                "source::sales", owner_id="manager-swc", fence=1,
                new_owner_id="manager-neu", new_location="azure:northeurope",
                lease_ms=100_000,
            )
        return original(key, stream, length=length)

    monkeypatch.setattr(stores["published"], "put_stream_if_absent", handover_then_copy)
    with pytest.raises(CatalogConflict, match="another regional Manager"):
        receiver.receive("site-1", _generation(tmp_path, 1))
    assert catalog.get("source::sales")["replicas"] == {}

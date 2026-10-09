from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import hashlib
import json
import threading

import pytest

from enterprise.control.federation import (
    CatalogConflict, GenerationDescriptor, GlobalCatalog, ObjectReference,
    RegionalReplicaVerifier,
)
from enterprise.control.placement import PlacementConfig, ResidencyPolicyViolation
from fabric_shortcut_proxy.runtime.artifact_store import MemoryStore
from tests.enterprise.test_residency_policy import _config


DATASET = "erp::sales"


def _setup():
    placement = PlacementConfig.from_dict(_config())
    metadata = MemoryStore()
    catalog = GlobalCatalog(metadata, placement)
    catalog.create(
        DATASET, owner_id="manager-a", owner_location="azure:swedencentral",
        lease_ms=100_000, freshness_target_ms=5_000, now_ms=1_000,
    )
    stores = {"published": MemoryStore(), "replica": MemoryStore()}
    return catalog, metadata, stores, RegionalReplicaVerifier(placement, stores)


def _descriptor(stores, sequence=1, *, produced_at_ms=1_000):
    refs = []
    for suffix, body in [
        ("data", b"PRIVATE ROW: customer=42"),
        ("table-metadata", b'{"schema":"test","objects":["data"]}'),
    ]:
        key = f"generation-{sequence}/{suffix}"
        stores["published"].put_stream_if_absent(key, [body])
        refs.append(ObjectReference("published", key, len(body), hashlib.sha256(body).hexdigest()))
    key = f"generation-{sequence}/manifest"
    descriptor = GenerationDescriptor(
        DATASET, f"generation-{sequence}", sequence, produced_at_ms, tuple(refs),
        ObjectReference("published", key, 0, "0" * 64),
    )
    manifest_bytes = json.dumps(descriptor.manifest_document(), sort_keys=True).encode()
    stores["published"].put_stream_if_absent(key, [manifest_bytes])
    return replace(
        descriptor, manifest=ObjectReference(
            "published", key, len(manifest_bytes), hashlib.sha256(manifest_bytes).hexdigest(),
        ),
    )


def _publish(catalog, verifier, descriptor, profile="published", owner="manager-a", fence=1, now=1_000):
    receipt = (
        verifier.verify(descriptor, profile) if profile == "published"
        else verifier.replicate(descriptor, profile)
    )
    return catalog.advertise(receipt, owner_id=owner, fence=fence, now_ms=now)


def test_catalog_contains_only_metadata_and_survives_reconstruction():
    catalog, metadata, stores, verifier = _setup()
    descriptor = _descriptor(stores)
    _publish(catalog, verifier, descriptor)
    raw = metadata.get(metadata.list()[0].key)
    assert b"PRIVATE ROW" not in raw
    assert b'"schema":"test"' not in raw
    assert descriptor.objects[0].sha256.encode() in raw
    restarted = GlobalCatalog(metadata, catalog.placement)
    resolved = restarted.resolve(DATASET, "primary", now_ms=2_000)
    assert resolved["generation"]["generation_id"] == "generation-1"
    assert not resolved["stale"]
    assert resolved["endpoint"] == "https://primary.example.test"


@pytest.mark.parametrize("suffix", ["data", "table-metadata", "manifest"])
def test_incomplete_replica_is_not_advertised_and_older_complete_remains(suffix):
    catalog, _, stores, verifier = _setup()
    _publish(catalog, verifier, _descriptor(stores), "replica")
    newer = _descriptor(stores, 2, produced_at_ms=2_000)
    stores["published"].delete(f"generation-2/{suffix}")
    with pytest.raises(CatalogConflict, match="object verification failed"):
        _publish(catalog, verifier, newer, "replica", now=2_000)
    assert catalog.resolve(DATASET, "secondary", now_ms=3_000)["generation"]["sequence"] == 1


def test_corrupt_destination_retains_older_complete_generation():
    catalog, _, stores, verifier = _setup()
    _publish(catalog, verifier, _descriptor(stores), "replica")
    descriptor = _descriptor(stores, 2, produced_at_ms=2_000)
    stores["replica"].put(descriptor.objects[0].key, b"corrupt")
    from fabric_shortcut_proxy.runtime.artifact_store import ObjectConflict
    with pytest.raises(ObjectConflict):
        _publish(catalog, verifier, descriptor, "replica", now=2_000)
    assert catalog.resolve(DATASET, "secondary", now_ms=3_000)["generation"]["sequence"] == 1


def test_verifier_reads_bytes_even_when_store_metadata_reports_matching_digest(monkeypatch):
    catalog, _, stores, verifier = _setup()
    descriptor = _descriptor(stores)
    ref = descriptor.objects[0]
    stores["published"].put(ref.key, b"X" * ref.size_bytes)
    monkeypatch.setattr(
        stores["published"], "verify",
        lambda key, *, size=None, content_hash=None: True,
    )
    with pytest.raises(CatalogConflict, match="object verification failed"):
        _publish(catalog, verifier, descriptor)
    with pytest.raises(CatalogConflict, match="no complete generation"):
        catalog.resolve(DATASET, "primary", now_ms=2_000)


def test_lagging_replica_serves_its_own_previous_complete_generation():
    catalog, _, stores, verifier = _setup()
    _publish(catalog, verifier, _descriptor(stores), "replica")
    _publish(catalog, verifier, _descriptor(stores, 2, produced_at_ms=2_000), now=2_000)
    assert catalog.resolve(DATASET, "primary", now_ms=3_000)["generation"]["sequence"] == 2
    assert catalog.resolve(DATASET, "secondary", now_ms=3_000)["generation"]["sequence"] == 1


def test_handover_both_directions_fences_previous_owner():
    catalog, _, stores, verifier = _setup()
    receipt = verifier.verify(_descriptor(stores), "published")
    first = catalog.handover(
        DATASET, owner_id="manager-a", fence=1, new_owner_id="manager-b",
        new_location="azure:northeurope", lease_ms=10_000, now_ms=2_000,
    )
    assert first["fence"] == 2
    with pytest.raises(CatalogConflict, match="stale"):
        catalog.advertise(receipt, owner_id="manager-a", fence=1, now_ms=2_000)
    catalog.advertise(receipt, owner_id="manager-b", fence=2, now_ms=2_000)
    second = catalog.handover(
        DATASET, owner_id="manager-b", fence=2, new_owner_id="manager-a",
        new_location="azure:swedencentral", lease_ms=10_000, now_ms=3_000,
    )
    assert second["fence"] == 3
    with pytest.raises(CatalogConflict, match="stale"):
        catalog.advertise(receipt, owner_id="manager-b", fence=2, now_ms=3_000)
    catalog.advertise(receipt, owner_id="manager-a", fence=3, now_ms=3_000)


def test_publication_racing_handover_cannot_overwrite_new_fence():
    catalog, metadata, stores, verifier = _setup()
    receipt = verifier.verify(_descriptor(stores), "published")
    original_cas = metadata.compare_and_swap
    ready = threading.Event()
    finish = threading.Event()

    def delayed(key, expected, data):
        if json.loads(data)["replicas"]:
            ready.set()
            assert finish.wait(5)
        return original_cas(key, expected, data)

    metadata.compare_and_swap = delayed
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(
            catalog.advertise, receipt, owner_id="manager-a", fence=1, now_ms=2_000,
        )
        assert ready.wait(5)
        catalog.handover(
            DATASET, owner_id="manager-a", fence=1, new_owner_id="manager-b",
            new_location="azure:northeurope", lease_ms=10_000, now_ms=2_000,
        )
        finish.set()
        with pytest.raises(CatalogConflict, match="concurrently"):
            future.result()
    metadata.compare_and_swap = original_cas
    assert json.loads(metadata.get(metadata.list()[0].key))["owner_id"] == "manager-b"


def test_expired_owner_cannot_renew_or_publish_and_takeover_increments_fence():
    catalog, _, stores, verifier = _setup()
    receipt = verifier.verify(_descriptor(stores), "published")
    with pytest.raises(CatalogConflict, match="still live"):
        catalog.takeover(
            DATASET, new_owner_id="manager-b", new_location="azure:northeurope",
            lease_ms=10_000, now_ms=50_000,
        )
    with pytest.raises(CatalogConflict):
        catalog.renew(DATASET, owner_id="manager-a", fence=1, lease_ms=10_000, now_ms=101_000)
    with pytest.raises(CatalogConflict):
        catalog.advertise(receipt, owner_id="manager-a", fence=1, now_ms=101_000)
    result = catalog.takeover(
        DATASET, new_owner_id="manager-b", new_location="azure:northeurope",
        lease_ms=10_000, now_ms=101_000,
    )
    assert result["fence"] == 2


def test_freshness_uses_generation_timestamp_not_replication_time():
    catalog, _, stores, verifier = _setup()
    descriptor = _descriptor(stores, produced_at_ms=1_000)
    _publish(catalog, verifier, descriptor, "replica", now=9_000)
    assert not catalog.resolve(DATASET, "secondary", now_ms=6_000)["stale"]
    stale = catalog.resolve(DATASET, "secondary", now_ms=6_001)
    assert stale["stale"]
    assert stale["generation"]["sequence"] == 1
    assert stores["replica"].get(descriptor.objects[0].key) == b"PRIVATE ROW: customer=42"


def test_unknown_endpoint_or_unverified_region_fails_explicitly():
    catalog, _, _, _ = _setup()
    with pytest.raises(ResidencyPolicyViolation):
        catalog.resolve(DATASET, "unknown")
    with pytest.raises(CatalogConflict, match="no complete generation"):
        catalog.resolve(DATASET, "secondary")


def test_replica_generation_cannot_regress_or_diverge_across_regions():
    catalog, _, stores, verifier = _setup()
    older = _descriptor(stores)
    newer = _descriptor(stores, 2, produced_at_ms=2_000)
    _publish(catalog, verifier, newer, now=2_000)
    with pytest.raises(CatalogConflict, match="backwards"):
        _publish(catalog, verifier, older, now=2_000)
    different = replace(newer, generation_id="wrong-generation")
    with pytest.raises(CatalogConflict, match="exactly these objects"):
        verifier.verify(different, "published")


def test_manifest_cannot_omit_or_add_an_object():
    _, _, stores, verifier = _setup()
    descriptor = _descriptor(stores)
    with pytest.raises(CatalogConflict, match="exactly these objects"):
        verifier.verify(replace(descriptor, objects=descriptor.objects[:1]), "published")


def test_modified_verification_receipt_is_rejected():
    catalog, _, stores, verifier = _setup()
    descriptor = _descriptor(stores)
    receipt = verifier.verify(descriptor, "published")
    modified = replace(receipt, descriptor=replace(descriptor, objects=descriptor.objects[:1]))
    with pytest.raises(CatalogConflict, match="not issued"):
        catalog.advertise(modified, owner_id="manager-a", fence=1, now_ms=1_000)
    with pytest.raises(CatalogConflict, match="no complete generation"):
        catalog.resolve(DATASET, "primary")


def test_outside_handover_or_replication_rejected_before_data_write():
    catalog, _, stores, verifier = _setup()
    with pytest.raises(ResidencyPolicyViolation):
        catalog.handover(
            DATASET, owner_id="manager-a", fence=1, new_owner_id="foreign",
            new_location="aws:us-east-1", lease_ms=1_000, now_ms=1_000,
        )
    with pytest.raises(ResidencyPolicyViolation):
        verifier.replicate(_descriptor(stores), "staging")
    assert stores["replica"].list() == []


@pytest.mark.parametrize("size,digest", [(-1, "0" * 64), (True, "0" * 64), (0, "not-a-digest")])
def test_invalid_object_references_rejected(size, digest):
    with pytest.raises(ValueError):
        ObjectReference("published", "g/data", size, digest)

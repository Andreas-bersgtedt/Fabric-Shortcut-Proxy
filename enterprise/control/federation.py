"""Metadata-only catalog primitives and a separate regional replica verifier.

Ownership and replica pointers share one CAS record, so a check of a separate
lease followed by an unguarded publication cannot resurrect an old owner.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from contextlib import closing
import hashlib
import hmac
import json
import re
import secrets
import time
from typing import Mapping

from enterprise.control.placement import PlacementConfig, ResidencyPolicyViolation
from fabric_shortcut_proxy.runtime.artifact_store import ArtifactStore, ObjectNotFound, _normalize_key

_RECEIPT_KEY = secrets.token_bytes(32)


class CatalogConflict(RuntimeError):
    pass


def _encode(value: dict) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


@dataclass(frozen=True)
class ObjectReference:
    store_profile: str
    key: str
    size_bytes: int
    sha256: str

    def __post_init__(self):
        if not self.store_profile or _normalize_key(self.key) != self.key:
            raise ValueError("object reference requires a profile and canonical key")
        if isinstance(self.size_bytes, bool) or not isinstance(self.size_bytes, int) or self.size_bytes < 0:
            raise ValueError("object reference size must be a non-negative integer")
        if not isinstance(self.sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", self.sha256):
            raise ValueError("object reference requires a lowercase SHA-256 digest")

    def to_dict(self) -> dict:
        return {
            "store_profile": self.store_profile, "key": self.key,
            "size_bytes": self.size_bytes, "sha256": self.sha256,
        }


@dataclass(frozen=True)
class GenerationDescriptor:
    dataset_id: str
    generation_id: str
    sequence: int
    produced_at_ms: int
    objects: tuple[ObjectReference, ...]
    manifest: ObjectReference

    def __post_init__(self):
        if not self.dataset_id or not self.generation_id or not self.objects:
            raise ValueError("generation descriptor requires dataset, generation and objects")
        for value in (self.sequence, self.produced_at_ms):
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError("generation sequence and timestamp must be positive integers")
        if len({ref.key for ref in self.objects}) != len(self.objects):
            raise ValueError("generation descriptor repeats an object key")
        if self.manifest.key in {ref.key for ref in self.objects}:
            raise ValueError("generation manifest cannot reference itself")

    @property
    def all_objects(self) -> tuple[ObjectReference, ...]:
        return (*self.objects, self.manifest)

    def manifest_document(self) -> dict:
        return {
            "version": 1, "dataset_id": self.dataset_id,
            "generation_id": self.generation_id, "sequence": self.sequence,
            "produced_at_ms": self.produced_at_ms,
            "objects": [
                {"key": ref.key, "size_bytes": ref.size_bytes, "sha256": ref.sha256}
                for ref in self.objects
            ],
        }

    def to_dict(self) -> dict:
        return {
            "dataset_id": self.dataset_id, "generation_id": self.generation_id,
            "sequence": self.sequence, "produced_at_ms": self.produced_at_ms,
            "objects": [ref.to_dict() for ref in self.objects],
            "manifest": self.manifest.to_dict(),
        }


@dataclass(frozen=True)
class VerifiedReplica:
    descriptor: GenerationDescriptor
    storage_profile: str
    proof: str = field(repr=False)


def _receipt_proof(descriptor: GenerationDescriptor, storage_profile: str) -> str:
    return hmac.new(
        _RECEIPT_KEY,
        _encode({"generation": descriptor.to_dict(), "storage_profile": storage_profile}),
        hashlib.sha256,
    ).hexdigest()


class RegionalReplicaVerifier:
    """Run inside the data residency boundary, not in the global catalog service."""

    def __init__(self, placement: PlacementConfig, stores: Mapping[str, ArtifactStore]):
        self.placement = placement
        self.stores = stores

    @staticmethod
    def _verify_bytes(store: ArtifactStore, ref: ObjectReference) -> None:
        digest = hashlib.sha256()
        size = 0
        try:
            with closing(store.get_stream(ref.key)) as stream:
                for chunk in stream:
                    size += len(chunk)
                    if size > ref.size_bytes:
                        raise CatalogConflict(f"replica object verification failed: {ref.key}")
                    digest.update(chunk)
        except ObjectNotFound as exc:
            raise CatalogConflict(f"replica object verification failed: {ref.key}") from exc
        if size != ref.size_bytes or digest.hexdigest() != ref.sha256:
            raise CatalogConflict(f"replica object verification failed: {ref.key}")

    def verify(
        self, descriptor: GenerationDescriptor, storage_profile: str
    ) -> VerifiedReplica:
        connection, separator, table = descriptor.dataset_id.partition("::")
        if not separator or not connection or not table:
            raise ValueError("dataset ID must use connection_id::source_table")
        policy = self.placement.policy_for(connection, table)
        self.placement.validate_residency(policy, connection_id=connection)
        if not policy.residency_locations or storage_profile not in {
            policy.required_storage_profile, *policy.replica_storage_profiles
        }:
            raise ResidencyPolicyViolation("replica target is not in the dataset publication chain")
        target = self.stores.get(storage_profile)
        if target is None:
            raise ValueError(f"regional store is not configured: {storage_profile}")
        if any(ref.store_profile != storage_profile for ref in descriptor.all_objects):
            raise ValueError("replica descriptor must reference its target store")
        if descriptor.manifest.size_bytes > 4 * 1024 * 1024:
            raise ValueError("generation manifest exceeds the 4 MiB control-metadata limit")
        for ref in descriptor.all_objects:
            self._verify_bytes(target, ref)
        raw_manifest = target.get(descriptor.manifest.key, length=4 * 1024 * 1024 + 1)
        if len(raw_manifest) != descriptor.manifest.size_bytes:
            raise CatalogConflict("generation manifest size changed during verification")
        if hashlib.sha256(raw_manifest).hexdigest() != descriptor.manifest.sha256:
            raise CatalogConflict("generation manifest digest changed during verification")
        if json.loads(raw_manifest) != descriptor.manifest_document():
            raise CatalogConflict("generation manifest does not describe exactly these objects")
        return VerifiedReplica(descriptor, storage_profile, _receipt_proof(descriptor, storage_profile))

    def replicate(
        self, descriptor: GenerationDescriptor, storage_profile: str
    ) -> VerifiedReplica:
        connection, separator, table = descriptor.dataset_id.partition("::")
        if not separator or not connection or not table:
            raise ValueError("dataset ID must use connection_id::source_table")
        policy = self.placement.policy_for(connection, table)
        self.placement.validate_residency(policy, connection_id=connection)
        if storage_profile not in policy.replica_storage_profiles:
            raise ResidencyPolicyViolation("replication target is not a configured replica")
        target = self.stores.get(storage_profile)
        if target is None:
            raise ValueError(f"regional store is not configured: {storage_profile}")
        self.verify(descriptor, policy.required_storage_profile)
        refs = []
        for ref in descriptor.all_objects:
            if ref.store_profile != policy.required_storage_profile:
                raise ResidencyPolicyViolation("replication source is not the published store")
            source = self.stores.get(ref.store_profile)
            if source is None:
                raise ValueError(f"regional source store is not configured: {ref.store_profile}")
            with closing(source.get_stream(ref.key)) as stream:
                target.put_stream_if_absent(ref.key, stream, length=ref.size_bytes)
            refs.append(ObjectReference(storage_profile, ref.key, ref.size_bytes, ref.sha256))
        return self.verify(
            GenerationDescriptor(
                descriptor.dataset_id, descriptor.generation_id, descriptor.sequence,
                descriptor.produced_at_ms, tuple(refs[:-1]), refs[-1],
            ),
            storage_profile,
        )


class GlobalCatalog:
    """Only names, digests, sizes, endpoints, ownership and health cross this seam."""

    def __init__(self, store: ArtifactStore, placement: PlacementConfig):
        self.store = store
        self.placement = placement

    @staticmethod
    def _key(dataset_id: str) -> str:
        return "_control/federation/datasets/" + hashlib.sha256(dataset_id.encode()).hexdigest() + ".json"

    def _read(self, dataset_id: str) -> tuple[bytes, dict]:
        try:
            raw = self.store.get(self._key(dataset_id))
        except ObjectNotFound:
            raise CatalogConflict(f"catalog dataset is not registered: {dataset_id}") from None
        record = json.loads(raw)
        if not isinstance(record, dict) or record.get("version") != 1 or record.get("dataset_id") != dataset_id:
            raise ValueError("invalid durable catalog record")
        return raw, record

    def _policy(self, dataset_id: str):
        connection, separator, table = dataset_id.partition("::")
        if not separator or not connection or not table:
            raise ValueError("dataset ID must use connection_id::source_table")
        policy = self.placement.policy_for(connection, table)
        self.placement.validate_residency(policy, connection_id=connection)
        if not policy.residency_locations:
            raise ResidencyPolicyViolation("federation requires an explicit residency boundary")
        return policy

    def _commit(self, dataset_id: str, expected: bytes | None, record: dict) -> dict:
        if not self.store.compare_and_swap(self._key(dataset_id), expected, _encode(record)):
            raise CatalogConflict("catalog record changed concurrently; reload before retrying")
        return record

    @staticmethod
    def _positive(value: int, name: str) -> None:
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValueError(f"{name} must be a positive integer")

    def create(
        self, dataset_id: str, *, owner_id: str, owner_location: str,
        lease_ms: int, freshness_target_ms: int, now_ms: int | None = None,
    ) -> dict:
        policy = self._policy(dataset_id)
        self._positive(lease_ms, "lease_ms")
        self._positive(freshness_target_ms, "freshness_target_ms")
        if not owner_id:
            raise ValueError("catalog owner must be non-empty")
        if owner_location not in policy.residency_locations:
            raise ResidencyPolicyViolation("owner location is outside the boundary")
        now = _now_ms() if now_ms is None else now_ms
        return self._commit(dataset_id, None, {
            "version": 1, "dataset_id": dataset_id, "owner_id": owner_id,
            "owner_location": owner_location, "fence": 1,
            "expires_at_ms": now + lease_ms, "freshness_target_ms": freshness_target_ms,
            "replicas": {},
            "generations": {},
        })

    @staticmethod
    def _require_owner(record: dict, owner_id: str, fence: int, now: int) -> None:
        if record["owner_id"] != owner_id or record["fence"] != fence or record["expires_at_ms"] <= now:
            raise CatalogConflict("dataset ownership is stale or expired")

    def handover(
        self, dataset_id: str, *, owner_id: str, fence: int,
        new_owner_id: str, new_location: str, lease_ms: int, now_ms: int | None = None,
    ) -> dict:
        policy = self._policy(dataset_id)
        self._positive(lease_ms, "lease_ms")
        if not new_owner_id or new_owner_id == owner_id:
            raise ValueError("handover requires a different non-empty owner")
        if new_location not in policy.residency_locations:
            raise ResidencyPolicyViolation("handover target is outside the boundary")
        now = _now_ms() if now_ms is None else now_ms
        raw, record = self._read(dataset_id)
        self._require_owner(record, owner_id, fence, now)
        record.update({
            "owner_id": new_owner_id, "owner_location": new_location,
            "fence": record["fence"] + 1, "expires_at_ms": now + lease_ms,
        })
        return self._commit(dataset_id, raw, record)

    def takeover(
        self, dataset_id: str, *, new_owner_id: str, new_location: str,
        lease_ms: int, now_ms: int | None = None,
    ) -> dict:
        policy = self._policy(dataset_id)
        self._positive(lease_ms, "lease_ms")
        if not new_owner_id:
            raise ValueError("takeover requires a non-empty owner")
        if new_location not in policy.residency_locations:
            raise ResidencyPolicyViolation("takeover target is outside the boundary")
        now = _now_ms() if now_ms is None else now_ms
        raw, record = self._read(dataset_id)
        if record["expires_at_ms"] > now:
            raise CatalogConflict("dataset owner lease is still live")
        record.update({
            "owner_id": new_owner_id, "owner_location": new_location,
            "fence": record["fence"] + 1, "expires_at_ms": now + lease_ms,
        })
        return self._commit(dataset_id, raw, record)

    def renew(
        self, dataset_id: str, *, owner_id: str, fence: int,
        lease_ms: int, now_ms: int | None = None,
    ) -> dict:
        self._policy(dataset_id)
        self._positive(lease_ms, "lease_ms")
        now = _now_ms() if now_ms is None else now_ms
        raw, record = self._read(dataset_id)
        self._require_owner(record, owner_id, fence, now)
        record["expires_at_ms"] = now + lease_ms
        return self._commit(dataset_id, raw, record)

    def advertise(
        self, receipt: VerifiedReplica, *, owner_id: str, fence: int,
        now_ms: int | None = None,
    ) -> dict:
        if not isinstance(receipt, VerifiedReplica):
            raise TypeError("catalog publication requires a regional verification receipt")
        descriptor = receipt.descriptor
        if not hmac.compare_digest(receipt.proof, _receipt_proof(descriptor, receipt.storage_profile)):
            raise CatalogConflict("replica receipt was not issued by this regional verifier process")
        policy = self._policy(descriptor.dataset_id)
        if receipt.storage_profile not in {
            policy.required_storage_profile, *policy.replica_storage_profiles
        } or any(ref.store_profile != receipt.storage_profile for ref in descriptor.all_objects):
            raise ResidencyPolicyViolation("verified replica does not match the publication chain")
        now = _now_ms() if now_ms is None else now_ms
        if descriptor.produced_at_ms > now:
            raise ValueError("generation timestamp cannot be in the future")
        raw, record = self._read(descriptor.dataset_id)
        self._require_owner(record, owner_id, fence, now)
        current = record["replicas"].get(receipt.storage_profile)
        value = descriptor.to_dict()
        identity = {
            "generation_id": descriptor.generation_id,
            "produced_at_ms": descriptor.produced_at_ms,
            "objects": [
                {"key": ref.key, "size_bytes": ref.size_bytes, "sha256": ref.sha256}
                for ref in descriptor.all_objects
            ],
        }
        sequence_key = str(descriptor.sequence)
        known = record["generations"].get(sequence_key)
        if known is not None and known != identity:
            raise CatalogConflict("generation sequence differs from another regional replica")
        record["generations"][sequence_key] = identity
        if current is not None:
            if current["sequence"] > descriptor.sequence:
                raise CatalogConflict("replica generation would move backwards")
            if current["sequence"] == descriptor.sequence and current != value:
                raise CatalogConflict("generation sequence already identifies different content")
        record["replicas"][receipt.storage_profile] = value
        return self._commit(descriptor.dataset_id, raw, record)

    def resolve(self, dataset_id: str, endpoint_id: str, *, now_ms: int | None = None) -> dict:
        policy = self._policy(dataset_id)
        if endpoint_id not in policy.serving_endpoints:
            raise ResidencyPolicyViolation("endpoint is not assigned to this dataset")
        endpoint = self.placement.serving_endpoints[endpoint_id]
        _, record = self._read(dataset_id)
        replica = record["replicas"].get(endpoint.storage_profile)
        if replica is None:
            raise CatalogConflict("regional replica has no complete generation")
        now = _now_ms() if now_ms is None else now_ms
        return {
            "dataset_id": dataset_id, "endpoint": endpoint.url,
            "location": endpoint.location, "storage_profile": endpoint.storage_profile,
            "generation": replica,
            "stale": now - replica["produced_at_ms"] > record["freshness_target_ms"],
            "freshness_target_ms": record["freshness_target_ms"],
        }

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

from enterprise.control.contract import SnapshotManifest
from enterprise.control.egress import EgressEvent, EgressLedger
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

    @classmethod
    def from_dict(cls, value: dict) -> "ObjectReference":
        return cls(
            value["store_profile"], value["key"],
            value["size_bytes"], value["sha256"],
        )


@dataclass(frozen=True)
class GenerationDescriptor:
    dataset_id: str
    generation_id: str
    sequence: int
    produced_at_ms: int
    objects: tuple[ObjectReference, ...]
    manifest: ObjectReference
    snapshot_key: str = ""

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
        if self.snapshot_key and self.snapshot_key not in {ref.key for ref in self.objects}:
            raise ValueError("snapshot must be one of the verified generation objects")

    @property
    def all_objects(self) -> tuple[ObjectReference, ...]:
        return (*self.objects, self.manifest)

    def manifest_document(self) -> dict:
        value = {
            "version": 1, "dataset_id": self.dataset_id,
            "generation_id": self.generation_id, "sequence": self.sequence,
            "produced_at_ms": self.produced_at_ms,
            "objects": [
                {"key": ref.key, "size_bytes": ref.size_bytes, "sha256": ref.sha256}
                for ref in self.objects
            ],
        }
        if self.snapshot_key:
            value["snapshot_key"] = self.snapshot_key
        return value

    def to_dict(self) -> dict:
        value = {
            "dataset_id": self.dataset_id, "generation_id": self.generation_id,
            "sequence": self.sequence, "produced_at_ms": self.produced_at_ms,
            "objects": [ref.to_dict() for ref in self.objects],
            "manifest": self.manifest.to_dict(),
        }
        if self.snapshot_key:
            value["snapshot_key"] = self.snapshot_key
        return value

    @classmethod
    def from_dict(cls, value: dict) -> "GenerationDescriptor":
        return cls(
            value["dataset_id"], value["generation_id"],
            value["sequence"], value["produced_at_ms"],
            tuple(ObjectReference.from_dict(ref) for ref in value["objects"]),
            ObjectReference.from_dict(value["manifest"]),
            value.get("snapshot_key", ""),
        )


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
        try:
            verified = store.verify(
                ref.key,
                size=ref.size_bytes,
                content_hash=ref.sha256,
            )
        except ObjectNotFound as exc:
            raise CatalogConflict(f"replica object verification failed: {ref.key}") from exc
        if not verified:
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
                descriptor.snapshot_key,
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
        policy = self._policy(dataset_id)
        self.placement.validate_task_tenant(
            str(record.get("tenant_id", "")),
            policy,
        )
        return raw, record

    def get(self, dataset_id: str) -> dict:
        """Return one validated durable catalog record."""
        _, record = self._read(dataset_id)
        return record

    def list_records(self) -> list[dict]:
        """Return all validated catalog records without reading dataset objects."""
        prefix = "_control/federation/datasets/"
        records = []
        for item in self.store.list(prefix):
            raw = self.store.get(item.key)
            record = json.loads(raw)
            if (
                not isinstance(record, dict)
                or record.get("version") != 1
                or not isinstance(record.get("dataset_id"), str)
            ):
                raise ValueError("invalid durable catalog record")
            policy = self._policy(str(record["dataset_id"]))
            self.placement.validate_task_tenant(
                str(record.get("tenant_id", "")),
                policy,
            )
            records.append(record)
        return sorted(records, key=lambda record: record["dataset_id"])

    def _policy(self, dataset_id: str):
        connection, separator, table = dataset_id.partition("::")
        if not separator or not connection or not table:
            raise ValueError("dataset ID must use connection_id::source_table")
        policy = self.placement.policy_for(connection, table)
        self.placement.validate_tenant(
            policy,
            connection_id=connection,
        )
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
            "tenant_id": policy.tenant_id,
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


class FederatedPublicationCoordinator:
    """Verify regional bytes and advertise only while this Manager owns the dataset."""

    def __init__(
        self,
        catalog: GlobalCatalog,
        verifier: RegionalReplicaVerifier,
        store: ArtifactStore,
        *,
        owner_id: str,
        owner_location: str,
        storage_profile: str,
        lease_ms: int,
    ):
        if not owner_id or not owner_location or not storage_profile:
            raise ValueError(
                "federation requires Manager, location and storage profile identifiers"
            )
        GlobalCatalog._positive(lease_ms, "lease_ms")
        self.catalog = catalog
        self.verifier = verifier
        self.store = store
        self.owner_id = owner_id
        self.owner_location = owner_location
        self.storage_profile = storage_profile
        self.lease_ms = lease_ms
        self.egress_ledger = EgressLedger(store)

    @classmethod
    def from_environment(
        cls, store: ArtifactStore, placement: PlacementConfig, *, serving_endpoint_id: str = ""
    ) -> "FederatedPublicationCoordinator":
        import os

        owner_id = os.environ.get("FSP_REGIONAL_MANAGER_ID", "").strip()
        owner_location = os.environ.get("FSP_REGIONAL_MANAGER_LOCATION", "").strip()
        storage_profile = os.environ.get("FSP_ARTIFACT_STORE_PROFILE", "").strip()
        if serving_endpoint_id:
            endpoint = placement.serving_endpoints[serving_endpoint_id]
            owner_id = f"serving:{serving_endpoint_id}"
            owner_location = endpoint.location
        try:
            lease_ms = int(os.environ.get("FSP_CATALOG_OWNER_LEASE_MS", "60000"))
        except ValueError as exc:
            raise ValueError("FSP_CATALOG_OWNER_LEASE_MS must be an integer") from exc
        catalog_store = store
        catalog_account_url = os.environ.get(
            "FSP_CATALOG_STORE_ACCOUNT_URL", ""
        ).strip()
        catalog_container = os.environ.get(
            "FSP_CATALOG_STORE_CONTAINER", ""
        ).strip()
        if bool(catalog_account_url) != bool(catalog_container):
            raise ValueError(
                "FSP_CATALOG_STORE_ACCOUNT_URL and FSP_CATALOG_STORE_CONTAINER "
                "must be configured together"
            )
        if catalog_account_url:
            from fabric_shortcut_proxy.runtime.azure_artifact_store import (
                build_azure_store,
            )

            catalog_store = build_azure_store(
                account_url=catalog_account_url,
                container_name=catalog_container,
                auth_mode=os.environ.get(
                    "FSP_CATALOG_STORE_AUTH_MODE", "managed_identity"
                ),
                tenant_id=os.environ.get("FSP_CATALOG_STORE_TENANT_ID", ""),
                client_id=os.environ.get("FSP_CATALOG_STORE_CLIENT_ID", ""),
                token_file=os.environ.get("FSP_CATALOG_STORE_TOKEN_FILE", ""),
            )
        stores = {storage_profile: store}
        replica_settings = json.loads(
            os.environ.get("FSP_FEDERATION_REPLICA_STORES", "{}")
        )
        if not isinstance(replica_settings, dict):
            raise ValueError("FSP_FEDERATION_REPLICA_STORES must be an object")
        if serving_endpoint_id and replica_settings:
            raise ValueError("regional serving must not configure remote replica stores")
        for profile, settings in replica_settings.items():
            declared = placement.stores.get(profile)
            if profile == storage_profile or declared is None or declared.provider != "azure":
                raise ValueError("replica stores must be distinct declared Azure profiles")
            if not isinstance(settings, dict) or not settings.get("account_url") or not settings.get("container_name"):
                raise ValueError("replica store requires account_url and container_name")
            if settings.get("auth_mode", "managed_identity") not in {"managed_identity", "workload_identity"}:
                raise ValueError("replica stores require managed or workload identity")
            from fabric_shortcut_proxy.runtime.azure_artifact_store import build_azure_store

            stores[profile] = build_azure_store(**settings)
        return cls(
            GlobalCatalog(catalog_store, placement),
            RegionalReplicaVerifier(placement, stores),
            store,
            owner_id=owner_id,
            owner_location=owner_location,
            storage_profile=storage_profile,
            lease_ms=lease_ms,
        )

    def _object_reference(self, key: str) -> ObjectReference:
        digest = hashlib.sha256()
        size = 0
        try:
            with closing(self.store.get_stream(key)) as stream:
                for chunk in stream:
                    size += len(chunk)
                    digest.update(chunk)
        except ObjectNotFound as exc:
            raise CatalogConflict(f"published object is missing: {key}") from exc
        return ObjectReference(
            self.storage_profile, key, size, digest.hexdigest()
        )

    def _descriptor(self, manifest: SnapshotManifest) -> GenerationDescriptor:
        if (
            not manifest.dataset_id
            or not manifest.generation_id
            or manifest.published_at_ms < 1
            or manifest.produced_at_ms < 1
        ):
            raise ValueError("federated publication requires durable generation metadata")
        dataset_hash = hashlib.sha256(manifest.dataset_id.encode()).hexdigest()
        generation_hash = hashlib.sha256(manifest.generation_id.encode()).hexdigest()
        snapshot_bytes = _encode(manifest.to_dict())
        snapshot_hash = hashlib.sha256(snapshot_bytes).hexdigest()
        snapshot_key = (
            f"_control/federation/snapshots/{dataset_hash}/{snapshot_hash}.json"
        )
        self.store.put_stream_if_absent(
            snapshot_key, [snapshot_bytes], length=len(snapshot_bytes)
        )
        objects = tuple(
            [
                ObjectReference(
                    self.storage_profile,
                    split.object_key,
                    split.size_bytes,
                    split.content_hash,
                )
                for split in manifest.splits
            ]
            + [self._object_reference(key) for key in manifest.metadata_keys]
            + [ObjectReference(
                self.storage_profile, snapshot_key,
                len(snapshot_bytes), snapshot_hash,
            )]
        )
        manifest_key = (
            f"_control/federation/manifests/{dataset_hash}/{generation_hash}/{snapshot_hash}.json"
        )
        placeholder = ObjectReference(
            self.storage_profile, manifest_key, 0, "0" * 64
        )
        descriptor = GenerationDescriptor(
            manifest.dataset_id,
            manifest.generation_id,
            manifest.published_at_ms,
            manifest.produced_at_ms,
            objects,
            placeholder,
            snapshot_key,
        )
        manifest_bytes = _encode(descriptor.manifest_document())
        self.store.put_stream_if_absent(
            manifest_key, [manifest_bytes], length=len(manifest_bytes)
        )
        return GenerationDescriptor(
            descriptor.dataset_id,
            descriptor.generation_id,
            descriptor.sequence,
            descriptor.produced_at_ms,
            descriptor.objects,
            ObjectReference(
                self.storage_profile,
                manifest_key,
                len(manifest_bytes),
                hashlib.sha256(manifest_bytes).hexdigest(),
            ),
            snapshot_key,
        )

    def require_owner(self, dataset_id: str) -> dict:
        record = self.catalog.get(dataset_id)
        if (
            record["owner_id"] != self.owner_id
            or record["owner_location"] != self.owner_location
        ):
            raise CatalogConflict("dataset is owned by another regional Manager")
        self.catalog._require_owner(
            record, self.owner_id, int(record["fence"]), _now_ms()
        )
        return record

    def publish(self, manifest: SnapshotManifest) -> dict:
        policy = self.catalog._policy(manifest.dataset_id)
        self.catalog.placement.validate_task_tenant(
            manifest.tenant_id,
            policy,
        )
        record = self.require_owner(manifest.dataset_id)
        descriptor = self._descriptor(manifest)
        receipt = self.verifier.verify(descriptor, self.storage_profile)
        return self.catalog.advertise(
            receipt,
            owner_id=self.owner_id,
            fence=int(record["fence"]),
        )

    def snapshot(self, dataset_id: str, table: str, epoch: int = 0) -> SnapshotManifest | None:
        record = self.catalog.get(dataset_id)
        value = record["replicas"].get(self.storage_profile)
        if value is None:
            return None
        descriptor = GenerationDescriptor.from_dict(value)
        if not descriptor.snapshot_key:
            raise CatalogConflict("regional generation has no serving snapshot")
        ref = next(ref for ref in descriptor.objects if ref.key == descriptor.snapshot_key)
        if ref.store_profile != self.storage_profile:
            raise ResidencyPolicyViolation("snapshot belongs to another regional store")
        if ref.size_bytes > 4 * 1024 * 1024:
            raise ValueError("serving snapshot exceeds the 4 MiB metadata limit")
        raw = self.store.get(ref.key, length=ref.size_bytes + 1)
        if len(raw) != ref.size_bytes or hashlib.sha256(raw).hexdigest() != ref.sha256:
            raise CatalogConflict("regional serving snapshot verification failed")
        manifest = SnapshotManifest.from_dict(json.loads(raw))
        if (
            manifest.dataset_id != dataset_id
            or manifest.table != table
            or manifest.generation_id != descriptor.generation_id
            or manifest.produced_at_ms != descriptor.produced_at_ms
        ):
            raise CatalogConflict("regional serving snapshot identity differs")
        if epoch and manifest.epoch != epoch:
            return None
        return manifest

    def replicate_owned(self) -> int:
        """Reconcile advertised primary generations; catalog state is the durable work list."""
        completed = 0
        for record in self.catalog.list_records():
            if record["owner_id"] != self.owner_id or record["owner_location"] != self.owner_location:
                continue
            policy = self.catalog._policy(record["dataset_id"])
            source = record["replicas"].get(policy.required_storage_profile)
            if source is None:
                continue
            descriptor = GenerationDescriptor.from_dict(source)
            for target in policy.replica_storage_profiles:
                current = record["replicas"].get(target)
                if current is not None and current["sequence"] >= descriptor.sequence:
                    continue
                receipt = self.verifier.replicate(descriptor, target)
                source_profile = self.catalog.placement.stores[
                    policy.required_storage_profile
                ]
                target_profile = self.catalog.placement.stores[target]
                if source_profile.location != target_profile.location:
                    self.egress_ledger.record(
                        EgressEvent(
                            event_id=(
                                f"replicate:{record['dataset_id']}:"
                                f"{descriptor.generation_id}:{target}"
                            ),
                            occurred_at_ms=descriptor.produced_at_ms,
                            tenant_id=str(record.get("tenant_id", "")),
                            dataset_id=str(record["dataset_id"]),
                            pool_id="replication",
                            source_region=source_profile.location,
                            destination_region=target_profile.location,
                            direction="replication",
                            bytes=sum(
                                reference.size_bytes
                                for reference in descriptor.all_objects
                            ),
                        )
                    )
                self.catalog.advertise(
                    receipt,
                    owner_id=self.owner_id,
                    fence=int(record["fence"]),
                )
                completed += 1
        return completed

    def renew_owned(self) -> list[dict]:
        renewed = []
        for record in self.catalog.list_records():
            if (
                record["owner_id"] != self.owner_id
                or record["owner_location"] != self.owner_location
            ):
                continue
            renewed.append(
                self.catalog.renew(
                    record["dataset_id"],
                    owner_id=self.owner_id,
                    fence=int(record["fence"]),
                    lease_ms=self.lease_ms,
                )
            )
        return renewed

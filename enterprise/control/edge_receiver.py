"""Regional processing of one authenticated edge generation."""

from __future__ import annotations

import base64
import binascii
from dataclasses import dataclass, replace
import hashlib
import json
import re
import time

from enterprise.control.contract import SnapshotManifest
from enterprise.control.edge_journal import EdgeJournalEntry, EdgeMetadata, encode_edge_record
from enterprise.control.edge_transfer import copy_edge_generation
from enterprise.control.federation import CatalogConflict, EdgeReplay, FederatedPublicationCoordinator
from fabric_shortcut_proxy.runtime.artifact_store import MemoryStore, _normalize_key

MAX_EDGE_REQUEST_BYTES = 16 * 1024 * 1024


@dataclass(frozen=True)
class EdgeBinding:
    dataset_id: str
    table: str
    key_prefix: str

    def __post_init__(self) -> None:
        if not self.dataset_id or not self.table:
            raise ValueError("edge binding requires a dataset and table")
        if not self.key_prefix or _normalize_key(self.key_prefix) != self.key_prefix:
            raise ValueError("edge binding requires a canonical key prefix")


def decode_edge_request(payload: object) -> tuple[EdgeJournalEntry, dict[str, bytes]]:
    if not isinstance(payload, dict) or set(payload) != {
        "sequence", "manifest_sha256", "record", "objects"
    }:
        raise ValueError("invalid edge request")
    sequence = payload["sequence"]
    digest = payload["manifest_sha256"]
    record = payload["record"]
    objects = payload["objects"]
    if (
        isinstance(sequence, bool) or not isinstance(sequence, int)
        or not 1 <= sequence <= 2**63 - 1
        or not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest)
        or not isinstance(record, dict) or set(record) != {"version", "manifest", "metadata"}
        or record["version"] != 1
        or not isinstance(record["manifest"], dict)
        or not isinstance(record["metadata"], list)
        or not isinstance(objects, dict)
    ):
        raise ValueError("invalid edge request fields")
    try:
        manifest = SnapshotManifest.from_dict(record["manifest"])
        metadata = tuple(EdgeMetadata(**item) for item in record["metadata"])
        encoded = encode_edge_record(manifest, metadata)
        supplied = json.dumps(record, sort_keys=True, separators=(",", ":"), allow_nan=False)
        if encoded != supplied or hashlib.sha256(encoded.encode()).hexdigest() != digest:
            raise ValueError("edge record failed integrity verification")
        decoded = {}
        total = 0
        for key, value in objects.items():
            if not isinstance(key, str) or not isinstance(value, str):
                raise ValueError("edge objects must map keys to base64 strings")
            data = base64.b64decode(value, validate=True)
            total += len(data)
            if total > MAX_EDGE_REQUEST_BYTES:
                raise ValueError("edge objects exceed the request limit")
            decoded[key] = data
    except (KeyError, TypeError, binascii.Error, UnicodeError) as exc:
        raise ValueError("invalid edge record or object encoding") from exc
    expected = {split.object_key for split in manifest.splits} | {
        item.key for item in metadata
    }
    if set(decoded) != expected:
        raise ValueError("edge request must contain exactly the recorded objects")
    return EdgeJournalEntry(sequence, manifest, digest, metadata), decoded


class EdgeReplayReceiver:
    """Verify bytes and publish only through the owning regional coordinator."""

    def __init__(
        self, coordinator: FederatedPublicationCoordinator, bindings: dict[str, EdgeBinding]
    ) -> None:
        if not bindings:
            raise ValueError("edge replay requires at least one identity binding")
        for identity in bindings:
            EdgeReplay(identity, 1, "0" * 64)
        self.coordinator = coordinator
        self.bindings = dict(bindings)

    def receive(self, identity: str, payload: object) -> dict:
        binding = self.bindings.get(identity)
        if binding is None:
            raise PermissionError("edge identity is not authorized")
        entry, objects = decode_edge_request(payload)
        manifest = entry.manifest
        if (
            manifest.dataset_id != binding.dataset_id
            or manifest.table != binding.table
            or manifest.table_format != "iceberg"
            or not re.fullmatch(r"[0-9a-f]{32}", manifest.generation_id)
            or manifest.request_id != manifest.generation_id
            or manifest.generation_fence != 0
            or manifest.published_at_ms != 0
            or len(manifest.splits) != 1
            or len(manifest.metadata_keys) != 4
        ):
            raise ValueError("edge generation does not match the authorized dataset")
        data_prefix = f"{binding.key_prefix}/.fsp/edge-staging/{manifest.generation_id}/"
        metadata_prefix = (
            f"{binding.key_prefix}/.fsp/publications/"
            f"{hashlib.sha256(manifest.generation_id.encode()).hexdigest()}/"
        )
        if (
            manifest.splits[0].object_key != data_prefix + "part-0.parquet"
            or any(
                not item.key.startswith(metadata_prefix)
                or "/" in item.key[len(metadata_prefix):]
                for item in entry.metadata
            )
        ):
            raise ValueError("edge object keys are outside the generation namespace")
        policy = self.coordinator.catalog._policy(binding.dataset_id)
        self.coordinator.catalog.placement.validate_task_tenant(manifest.tenant_id, policy)
        record = self.coordinator.require_owner(binding.dataset_id)
        previous = record.get("edge_replay")
        if previous is not None:
            if not isinstance(previous, dict):
                raise CatalogConflict("invalid edge replay cursor")
            if previous.get("stream_id") != identity:
                raise CatalogConflict("dataset is bound to another edge stream")
            previous_edge_sequence = previous.get("sequence")
            if (
                isinstance(previous_edge_sequence, bool)
                or not isinstance(previous_edge_sequence, int)
                or previous_edge_sequence < 1
            ):
                raise CatalogConflict("invalid edge replay cursor")
            if entry.sequence == previous_edge_sequence:
                if entry.manifest_sha256 != previous.get("manifest_sha256"):
                    raise CatalogConflict("edge replay retry differs from the committed generation")
                publication_sequence = previous.get("publication_sequence")
                if isinstance(publication_sequence, bool) or not isinstance(publication_sequence, int):
                    raise CatalogConflict("committed edge replay lacks its publication sequence")
            elif entry.sequence == previous_edge_sequence + 1:
                publication_sequence = 0
            else:
                raise CatalogConflict("edge replay sequence must advance without gaps")
        elif entry.sequence == 1:
            publication_sequence = 0
        else:
            raise CatalogConflict("edge replay sequence must start at one")
        if manifest.produced_at_ms > time.time_ns() // 1_000_000:
            raise ValueError("generation timestamp cannot be in the future")
        if not publication_sequence:
            current = record["replicas"].get(self.coordinator.storage_profile)
            previous_sequence = int(current["sequence"]) if current else 0
            publication_sequence = max(manifest.produced_at_ms, previous_sequence + 1)
        source = MemoryStore()
        for key, data in objects.items():
            source.put(key, data)
        copy_edge_generation(
            entry, source, self.coordinator.store,
            dataset_id=binding.dataset_id, allowed_prefix=binding.key_prefix,
        )
        self.coordinator.publish(
            replace(manifest, published_at_ms=publication_sequence),
            edge_replay=EdgeReplay(identity, entry.sequence, entry.manifest_sha256),
        )
        return {
            "sequence": entry.sequence,
            "generation_id": manifest.generation_id,
            "manifest_sha256": entry.manifest_sha256,
        }

"""
Build and cache Iceberg metadata.json bytes for the current snapshot.

metadata.json is the Iceberg table's top-level catalog entry. Fabric
fetches it first to discover schema, snapshot id, and the manifest list path.
"""
from __future__ import annotations

import json

import config
from iceberg.schema import iceberg_schema_dict
from iceberg.state_store import SnapshotState


def build_metadata_json(snap: SnapshotState) -> bytes:
    """Return serialised metadata.json bytes. Result is cached on snap."""
    if snap.metadata_bytes is not None:
        return snap.metadata_bytes

    location = f"s3://{config.BUCKET_NAME}/{config.WAREHOUSE_PREFIX}/{snap.table.name}"

    schema = iceberg_schema_dict(schema_id=0, columns=snap.table.schema)

    # Unpartitioned spec (POC has no partitions)
    partition_spec = {
        "spec-id": 0,
        "fields": [],
    }

    # F2 — with history enabled, render this metadata version point-in-time:
    # include every snapshot up to and including this one, plus a snapshot-log
    # and a metadata-log referencing the earlier metadata files. Without history
    # it's a single snapshot (the known-good Fabric path).
    if config.ICEBERG_SNAPSHOT_HISTORY:
        from iceberg.state_store import get_snapshot_history
        history = [s for s in get_snapshot_history(snap.table.name) if s.version <= snap.version]
        if not history:
            history = [snap]
    else:
        history = [snap]

    snapshots = [_snapshot_entry(s) for s in history]
    snapshot_log = [
        {"timestamp-ms": s.watermark_ms, "snapshot-id": s.snapshot_id} for s in history
    ]
    metadata_log = [
        {"timestamp-ms": s.watermark_ms,
         "metadata-file": f"s3://{config.BUCKET_NAME}/{s.metadata_key}"}
        for s in history if s.version < snap.version
    ]

    metadata = {
        "format-version": 2,
        "table-uuid": _stable_uuid(location),
        "location": location,
        "last-sequence-number": snap.sequence_number,
        "last-updated-ms": snap.watermark_ms,
        "last-column-id": max(c.field_id for c in snap.table.schema),
        "current-schema-id": 0,
        "schemas": [schema],
        "partition-specs": [partition_spec],
        "default-spec-id": 0,
        "last-partition-id": 0,
        "sort-orders": [{"order-id": 0, "fields": []}],
        "default-sort-order-id": 0,
        "snapshots": snapshots,
        "current-snapshot-id": snap.snapshot_id,
        "snapshot-log": snapshot_log,
        "metadata-log": metadata_log,
        "refs": {
            "main": {
                "type": "branch",
                "snapshot-id": snap.snapshot_id,
            }
        },
        "statistics": [],
        "partition-statistics": [],
    }

    snap.metadata_bytes = json.dumps(metadata, indent=2).encode()
    return snap.metadata_bytes


def _snapshot_entry(snap: SnapshotState) -> dict:
    """Build the metadata.json ``snapshots`` list entry for one snapshot."""
    num_splits = len(snap.splits)
    total_records = snap.total_records if snap.total_records is not None else 0
    return {
        "snapshot-id": snap.snapshot_id,
        "sequence-number": snap.sequence_number,
        "timestamp-ms": snap.watermark_ms,
        "summary": {
            "operation": "append",
            "added-data-files": str(num_splits),
            "added-records": str(total_records),
            "total-records": str(total_records),
            "total-data-files": str(num_splits),
            "total-delete-files": "0",
        },
        "manifest-list": f"s3://{config.BUCKET_NAME}/{snap.manifest_list_key}",
        "schema-id": 0,
    }


def _stable_uuid(seed: str) -> str:
    """Deterministic UUID-like string from seed (not cryptographic)."""
    import hashlib
    h = hashlib.md5(seed.encode(), usedforsecurity=False).hexdigest()
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"

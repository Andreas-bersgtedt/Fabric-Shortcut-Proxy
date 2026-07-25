"""
Build Iceberg manifest list and manifest file as Avro bytes.

Iceberg v2 manifest list (snap-*.avro): one record per manifest file.
Iceberg v2 manifest file (*-m0.avro):  one record per data file.

We produce exactly one manifest file listing all virtual split Parquet files.

Reference schemas: https://iceberg.apache.org/spec/#manifests
"""
from __future__ import annotations

import io

import fastavro

import config
from iceberg.state_store import SnapshotState


# ---------------------------------------------------------------------------
# Avro schemas (embedded; no external schema registry needed)
# ---------------------------------------------------------------------------

# Manifest list entry (one record per manifest file)
MANIFEST_LIST_SCHEMA = {
    "type": "record",
    "name": "manifest_file",
    "fields": [
        {"name": "manifest_path",        "type": "string",  "field-id": 500},
        {"name": "manifest_length",      "type": "long",    "field-id": 501},
        {"name": "partition_spec_id",    "type": "int",     "field-id": 502},
        {"name": "content",              "type": "int",     "field-id": 517, "default": 0},
        {"name": "sequence_number",      "type": "long",    "field-id": 515, "default": 0},
        {"name": "min_sequence_number",  "type": "long",    "field-id": 516, "default": 0},
        {"name": "added_snapshot_id",    "type": "long",    "field-id": 503},
        {"name": "added_files_count",    "type": "int",     "field-id": 504, "default": 0},
        {"name": "existing_files_count", "type": "int",     "field-id": 505, "default": 0},
        {"name": "deleted_files_count",  "type": "int",     "field-id": 506, "default": 0},
        {"name": "added_rows_count",     "type": "long",    "field-id": 512, "default": 0},
        {"name": "existing_rows_count",  "type": "long",    "field-id": 513, "default": 0},
        {"name": "deleted_rows_count",   "type": "long",    "field-id": 514, "default": 0},
        {
            "name": "partitions",
            "type": {
                "type": "array",
                "items": {
                    "type": "record",
                    "name": "r508",
                    "fields": [
                        {"name": "contains_null",  "type": "boolean", "field-id": 509},
                        {"name": "contains_nan",   "type": ["null", "boolean"], "field-id": 518, "default": None},
                        {"name": "lower_bound",    "type": ["null", "bytes"],   "field-id": 510, "default": None},
                        {"name": "upper_bound",    "type": ["null", "bytes"],   "field-id": 511, "default": None},
                    ],
                },
                "element-id": 508,
            },
            "field-id": 507,
            "default": [],
        },
    ],
}

# Manifest entry (one record per data file)
# NOTE: The optional column-stat map fields (column_sizes, value_counts,
# null_value_counts, nan_value_counts, lower_bounds, upper_bounds) are OMITTED by
# default. They are optional per the Iceberg spec, and omitting them avoids the
# Avro map/array encoding pitfalls that strict Iceberg readers (pyiceberg,
# OneLake) reject; readers simply treat the stats as absent and read the Parquet
# files directly.
#
# F3: when ICEBERG_MANIFEST_STATS is enabled, the stat maps ARE included (see
# _manifest_entry_schema(with_stats=True)). They are encoded as Iceberg's
# array-of-key/value "map" form with a ``field-id`` on every key and value --
# the missing field-ids were exactly what strict readers previously rejected.


def _map_field(name: str, field_id: int, key_id: int, value_id: int, value_type: str) -> dict:
    """An Iceberg map<int, value_type> encoded as an Avro array of key/value records."""
    return {
        "name": name,
        "type": ["null", {
            "type": "array",
            "items": {
                "type": "record",
                "name": f"k{key_id}_v{value_id}",
                "fields": [
                    {"name": "key", "type": "int", "field-id": key_id},
                    {"name": "value", "type": value_type, "field-id": value_id},
                ],
            },
            "logicalType": "map",
        }],
        "field-id": field_id,
        "default": None,
    }


def _data_file_fields(with_stats: bool) -> list[dict]:
    fields: list[dict] = [
        {"name": "content",           "type": "int",    "field-id": 134, "default": 0},  # 0=DATA
        {"name": "file_path",         "type": "string", "field-id": 100},
        {"name": "file_format",       "type": "string", "field-id": 101},  # "PARQUET"
        {
            "name": "partition",
            "type": {"type": "record", "name": "r102", "fields": []},
            "field-id": 102,
        },
        {"name": "record_count",      "type": "long",   "field-id": 103},
        {"name": "file_size_in_bytes","type": "long",   "field-id": 104},
    ]
    if with_stats:
        fields += [
            _map_field("column_sizes",      108, 117, 118, "long"),
            _map_field("value_counts",      109, 119, 120, "long"),
            _map_field("null_value_counts", 110, 121, 122, "long"),
            _map_field("nan_value_counts",  137, 138, 139, "long"),
            _map_field("lower_bounds",      125, 126, 127, "bytes"),
            _map_field("upper_bounds",      128, 129, 130, "bytes"),
        ]
    fields += [
        {"name": "key_metadata",      "type": ["null", "bytes"], "field-id": 131, "default": None},
        {"name": "split_offsets",     "type": ["null", {"type": "array", "items": "long", "element-id": 133}], "field-id": 132, "default": None},
        {"name": "equality_ids",      "type": ["null", {"type": "array", "items": "int",  "element-id": 136}], "field-id": 135, "default": None},
        {"name": "sort_order_id",     "type": ["null", "int"], "field-id": 140, "default": None},
    ]
    return fields


def _manifest_entry_schema(with_stats: bool) -> dict:
    return {
        "type": "record",
        "name": "manifest_entry",
        "fields": [
            {"name": "status",          "type": "int",  "field-id": 0},   # 1 = ADDED
            {"name": "snapshot_id",     "type": ["null", "long"], "field-id": 1, "default": None},
            {"name": "sequence_number", "type": ["null", "long"], "field-id": 3, "default": None},
            {"name": "file_sequence_number", "type": ["null", "long"], "field-id": 4, "default": None},
            {
                "name": "data_file",
                "type": {
                    "type": "record",
                    "name": "r2",
                    "fields": _data_file_fields(with_stats),
                },
                "field-id": 2,
            },
        ],
    }


# Default (no-stats) schema, kept for the known-good path and existing callers.
MANIFEST_ENTRY_SCHEMA = _manifest_entry_schema(with_stats=False)

# Placeholder row count / size – unknown until query executed.
# Manifest readers typically tolerate approximate values in POC scenarios.
PLACEHOLDER_RECORD_COUNT: int = 100_000
PLACEHOLDER_FILE_SIZE: int    = 10 * 1024 * 1024  # 10 MB


def _s3_url(object_key: str) -> str:
    return f"s3://{config.BUCKET_NAME}/{object_key}"


def _avro_bytes(schema: dict, records: list[dict]) -> bytes:
    buf = io.BytesIO()
    parsed = fastavro.parse_schema(schema)
    fastavro.writer(buf, parsed, records)
    return buf.getvalue()


def _kv(pairs) -> list[dict]:
    """Encode ``{key: value}`` pairs as Iceberg's array-of-key/value map form."""
    return [{"key": k, "value": v} for k, v in sorted(pairs)]


def _stat_maps(stats: dict | None) -> dict:
    """Build the six Iceberg stat maps from a split's per-column stats (F3)."""
    if not stats:
        # Flag on but no stats collected: emit empty/absent maps (still valid).
        return {
            "column_sizes": [], "value_counts": [], "null_value_counts": [],
            "nan_value_counts": [], "lower_bounds": [], "upper_bounds": [],
        }
    items = stats.values()
    return {
        "column_sizes":      _kv((s.field_id, s.column_size) for s in items),
        "value_counts":      _kv((s.field_id, s.value_count) for s in items),
        "null_value_counts": _kv((s.field_id, s.null_count) for s in items),
        "nan_value_counts":  [],
        "lower_bounds":      _kv((s.field_id, s.lower) for s in items if s.lower is not None),
        "upper_bounds":      _kv((s.field_id, s.upper) for s in items if s.upper is not None),
    }


# ---------------------------------------------------------------------------
# Public builders
# ---------------------------------------------------------------------------

def build_manifest_file(snap: SnapshotState) -> bytes:
    """Build the single manifest Avro file listing all virtual split files."""
    if snap.manifest_file_bytes is not None:
        return snap.manifest_file_bytes

    with_stats = config.ICEBERG_MANIFEST_STATS

    entries = []
    for split in snap.splits:
        record_count = split.record_count if split.record_count is not None else PLACEHOLDER_RECORD_COUNT
        file_size = split.file_size_in_bytes if split.file_size_in_bytes is not None else PLACEHOLDER_FILE_SIZE
        data_file = {
            "content": 0,  # DATA
            "file_path": _s3_url(split.object_key),
            "file_format": "PARQUET",
            "partition": {},
            "record_count": record_count,
            "file_size_in_bytes": file_size,
            "key_metadata": None,
            "split_offsets": None,
            "equality_ids": None,
            "sort_order_id": None,
        }
        if with_stats:
            data_file.update(_stat_maps(split.stats))
        entries.append({
            "status": 1,  # ADDED
            "snapshot_id": snap.snapshot_id,
            "sequence_number": snap.sequence_number,
            "file_sequence_number": snap.sequence_number,
            "data_file": data_file,
        })

    schema = _manifest_entry_schema(with_stats)
    snap.manifest_file_bytes = _avro_bytes(schema, entries)
    return snap.manifest_file_bytes


def build_manifest_list(snap: SnapshotState) -> bytes:
    """Build the manifest list Avro file (one entry per manifest file)."""
    if snap.manifest_list_bytes is not None:
        return snap.manifest_list_bytes

    # Ensure manifest file bytes exist so we can report accurate length.
    mf_bytes = build_manifest_file(snap)

    total_rows = sum(
        (s.record_count if s.record_count is not None else PLACEHOLDER_RECORD_COUNT)
        for s in snap.splits
    )

    record = {
        "manifest_path": _s3_url(snap.manifest_file_key),
        "manifest_length": len(mf_bytes),
        "partition_spec_id": 0,
        "content": 0,  # DATA
        "sequence_number": snap.sequence_number,
        "min_sequence_number": snap.sequence_number,
        "added_snapshot_id": snap.snapshot_id,
        "added_files_count": len(snap.splits),
        "existing_files_count": 0,
        "deleted_files_count": 0,
        "added_rows_count": total_rows,
        "existing_rows_count": 0,
        "deleted_rows_count": 0,
        "partitions": [],
    }

    snap.manifest_list_bytes = _avro_bytes(MANIFEST_LIST_SCHEMA, [record])
    return snap.manifest_list_bytes

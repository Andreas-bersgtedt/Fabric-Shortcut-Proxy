"""
Unit tests for Iceberg metadata and manifest generation.
"""
from __future__ import annotations

import io
import json
import os

os.environ.setdefault("DB_URL", "sqlite+aiosqlite:///:memory:")
os.environ.setdefault("NUM_SPLITS", "4")
os.environ.setdefault("S3_BUCKET", "test-bucket")

import fastavro
import pytest

import config
from iceberg.state_store import build_snapshot
from iceberg.metadata import build_metadata_json
from iceberg.manifest import build_manifest_list, build_manifest_file


def test_decimal_type_serializes_as_iceberg_string():
    """Iceberg spec: decimal is a PRIMITIVE and must serialize as the string
    'decimal(P, S)', NOT an object. The object form breaks XTable/Fabric
    conversion (e.g. AdventureWorks money/decimal columns)."""
    from config import ColumnDef
    from iceberg.schema import iceberg_schema_dict

    cols = [
        ColumnDef(field_id=1, name="id", iceberg_type="long", nullable=False),
        ColumnDef(field_id=2, name="price", iceberg_type="decimal(19,4)", nullable=True),
    ]
    schema = iceberg_schema_dict(columns=cols)
    price = next(f for f in schema["fields"] if f["name"] == "price")
    assert price["type"] == "decimal(19, 4)"
    assert isinstance(price["type"], str)
    # And it must be valid inside a full metadata.json (JSON-serializable string).
    assert json.dumps(schema)


@pytest.fixture
def snap():
    return build_snapshot(
        table_name="sales",
        num_splits=4,
        bucket="test-bucket",
        warehouse_prefix="warehouse/db",
    )


def test_metadata_json_format_version(snap):
    data = build_metadata_json(snap)
    meta = json.loads(data)
    assert meta["format-version"] == 2


def test_metadata_json_has_snapshot(snap):
    meta = json.loads(build_metadata_json(snap))
    assert meta["current-snapshot-id"] == snap.snapshot_id
    assert len(meta["snapshots"]) == 1


def test_metadata_json_schema_fields(snap):
    meta = json.loads(build_metadata_json(snap))
    schema = meta["schemas"][0]
    field_names = [f["name"] for f in schema["fields"]]
    expected = [col.name for col in config.TABLE_SCHEMA]
    assert field_names == expected


def test_manifest_list_is_valid_avro(snap):
    data = build_manifest_list(snap)
    records = list(fastavro.reader(io.BytesIO(data)))
    assert len(records) == 1
    assert records[0]["partition_spec_id"] == 0
    assert records[0]["added_files_count"] == 4


def test_manifest_file_has_all_splits(snap):
    data = build_manifest_file(snap)
    records = list(fastavro.reader(io.BytesIO(data)))
    assert len(records) == 4
    for record in records:
        assert record["data_file"]["file_format"] == "PARQUET"
        assert record["data_file"]["content"] == 0  # DATA


def test_manifest_file_paths_match_splits(snap):
    data = build_manifest_file(snap)
    records = list(fastavro.reader(io.BytesIO(data)))
    paths = {r["data_file"]["file_path"] for r in records}
    expected = {f"s3://test-bucket/{s.object_key}" for s in snap.splits}
    assert paths == expected

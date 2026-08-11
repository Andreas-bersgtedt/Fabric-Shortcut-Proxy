"""Phase 2 — Open Mirroring landing-zone writer tests.

Verifies the config loader, path model, metadata builders, file numbering, and the
end-to-end publisher against a local filesystem landing zone. No live database is
required; rows are passed directly so the writer is exercised in isolation.
"""
from __future__ import annotations

import io
import json
import os

os.environ.setdefault("DB_URL", "sqlite+aiosqlite:///:memory:")

import pyarrow.parquet as pq
import pytest

from config import ColumnDef
from open_mirror import (
    LandingZonePublisher,
    LocalLandingZone,
    build_landing_parquet,
    format_file_name,
    is_onelake_uri,
    next_file_index,
    open_landing_zone,
    parse_file_index,
    table_relative_path,
    target_from_dict,
)
from open_mirror.config import load_targets
from open_mirror.metadata import build_partner_events, build_table_metadata
from open_mirror.publisher import ROW_MARKER_COLUMN


_COLUMNS = [
    ColumnDef(field_id=1, name="id", iceberg_type="long", nullable=False),
    ColumnDef(field_id=2, name="name", iceberg_type="string", nullable=True),
    ColumnDef(field_id=3, name="status", iceberg_type="string", nullable=True),
]


def _rows(n: int = 3) -> list[dict]:
    return [{"id": i, "name": f"n{i}", "status": "active"} for i in range(1, n + 1)]


def _table():
    return target_from_dict({
        "id": "t1",
        "connection": "default",
        "landing_zone_root": "/tmp/lz",
        "source_type": "SQL",
        "tables": [{
            "name": "sales", "source_table": "dbo.sales", "target_table": "sales",
            "key_column": "id", "schema": "dbo",
        }],
    })


# --- config ----------------------------------------------------------------

def test_target_from_dict_defaults_and_binding():
    target = _table()
    assert target.id == "t1"
    assert target.connection_id == "default"
    assert target.tables[0].target_table == "sales"
    assert target.tables[0].key_columns == ["id"]


def test_load_targets_reads_open_mirror_section(tmp_path):
    path = tmp_path / "config.open_mirror.json"
    path.write_text(json.dumps({
        "open_mirror": {"open_mirror_targets": [{
            "id": "x", "connection": "default",
            "landing_zone_root": "/tmp/lz",
            "tables": [{"name": "t", "source_table": "s.t", "target_table": "t", "key_column": "id"}],
        }]}
    }), encoding="utf-8")
    targets = load_targets(str(path))
    assert len(targets) == 1 and targets[0].id == "x"


def test_load_targets_missing_file_is_empty(tmp_path):
    assert load_targets(str(tmp_path / "nope.json")) == []


# --- path model ------------------------------------------------------------

def test_table_relative_path_with_and_without_schema():
    assert table_relative_path("sales", "dbo") == "dbo.schema/sales"
    assert table_relative_path("sales", None) == "sales"


def test_is_onelake_uri():
    assert is_onelake_uri("https://onelake.dfs.fabric.microsoft.com/ws/db/Files/LandingZone")
    assert not is_onelake_uri("/tmp/lz")


def test_open_landing_zone_rejects_onelake_uri():
    with pytest.raises(NotImplementedError):
        open_landing_zone("https://onelake.dfs.fabric.microsoft.com/ws/db/Files/LandingZone")


def test_local_landing_zone_blocks_traversal(tmp_path):
    lz = LocalLandingZone(str(tmp_path))
    with pytest.raises(ValueError):
        lz.write_bytes("../escape.txt", b"x")


# --- metadata --------------------------------------------------------------

def test_build_table_metadata():
    meta = build_table_metadata(["id"], file_detection_strategy="LastUpdateTimeFileDetection", upsert_default=True)
    assert meta["keyColumns"] == ["id"]
    assert meta["fileDetectionStrategy"] == "LastUpdateTimeFileDetection"
    assert meta["isUpsertDefaultRowMarker"] is True


def test_build_table_metadata_requires_keys():
    with pytest.raises(ValueError):
        build_table_metadata([])


def test_build_partner_events():
    event = build_partner_events("FabricShortcutProxy", source_type="SQL", source_version="2025")
    assert event["partnerName"] == "FabricShortcutProxy"
    assert event["sourceInfo"]["sourceType"] == "SQL"


# --- file numbering --------------------------------------------------------

def test_format_and_parse_file_name():
    assert format_file_name(1) == "00000000000000000001.parquet"
    assert parse_file_index("00000000000000000001.parquet") == 1
    assert parse_file_index("_metadata.json") is None


def test_next_file_index():
    assert next_file_index([]) == 1
    assert next_file_index(["_metadata.json", "00000000000000000003.parquet"]) == 4


# --- parquet ---------------------------------------------------------------

def test_build_landing_parquet_initial_load_has_no_marker():
    data = build_landing_parquet(_rows(2), _COLUMNS)
    table = pq.read_table(io.BytesIO(data))
    assert table.num_rows == 2
    assert ROW_MARKER_COLUMN not in table.schema.names
    assert table.schema.names == ["id", "name", "status"]


def test_build_landing_parquet_incremental_marker_is_last_column():
    data = build_landing_parquet(_rows(2), _COLUMNS, row_markers=[0, 1])
    table = pq.read_table(io.BytesIO(data))
    assert table.schema.names[-1] == ROW_MARKER_COLUMN
    assert table.column(ROW_MARKER_COLUMN).to_pylist() == [0, 1]


def test_build_landing_parquet_marker_length_mismatch():
    with pytest.raises(ValueError):
        build_landing_parquet(_rows(2), _COLUMNS, row_markers=[0])


# --- publisher end-to-end --------------------------------------------------

def test_publish_initial_load_writes_metadata_and_numbered_file(tmp_path):
    target = _table()
    backend = LocalLandingZone(str(tmp_path))
    pub = LandingZonePublisher(backend, target)

    rel = pub.publish_initial_load(target.tables[0], _rows(3), _COLUMNS)
    assert rel == "dbo.schema/sales/00000000000000000001.parquet"

    meta_path = tmp_path / "dbo.schema" / "sales" / "_metadata.json"
    assert json.loads(meta_path.read_text(encoding="utf-8"))["keyColumns"] == ["id"]

    data_path = tmp_path / "dbo.schema" / "sales" / "00000000000000000001.parquet"
    assert pq.read_table(data_path).num_rows == 3


def test_publish_batch_increments_file_numbers(tmp_path):
    target = _table()
    pub = LandingZonePublisher(LocalLandingZone(str(tmp_path)), target)
    table = target.tables[0]

    first = pub.publish_batch(table, _rows(1), _COLUMNS)
    second = pub.publish_batch(table, _rows(1), _COLUMNS, row_markers=[1])
    assert first.endswith("00000000000000000001.parquet")
    assert second.endswith("00000000000000000002.parquet")


def test_ensure_table_metadata_is_idempotent(tmp_path):
    target = _table()
    pub = LandingZonePublisher(LocalLandingZone(str(tmp_path)), target)
    table = target.tables[0]

    pub.ensure_table_metadata(table)
    meta_path = tmp_path / "dbo.schema" / "sales" / "_metadata.json"
    meta_path.write_text(json.dumps({"keyColumns": ["id"], "tag": "kept"}), encoding="utf-8")

    pub.ensure_table_metadata(table)  # must not overwrite an existing metadata file
    assert json.loads(meta_path.read_text(encoding="utf-8"))["tag"] == "kept"


def test_ensure_partner_events_writes_db_level_file(tmp_path):
    target = _table()
    pub = LandingZonePublisher(LocalLandingZone(str(tmp_path)), target)
    rel = pub.ensure_partner_events()
    assert rel == "_partnerEvents.json"
    event = json.loads((tmp_path / "_partnerEvents.json").read_text(encoding="utf-8"))
    assert event["sourceInfo"]["sourceType"] == "SQL"

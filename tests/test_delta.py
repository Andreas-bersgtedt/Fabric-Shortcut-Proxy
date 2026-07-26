"""
Tests for the native Delta Lake output mode (TABLE_FORMAT=delta).

Unit-tests the type mapping + schema string, and integration-tests the S3
router in delta mode (serves _delta_log/*.json commits + Parquet data files,
and does NOT serve Iceberg metadata/manifests).
"""
from __future__ import annotations

import io
import json
import os
import pathlib

import pytest
import pyarrow.parquet as pq

_TEST_DB = pathlib.Path(__file__).parent / "test_delta.db"
os.environ["DB_URL"] = f"sqlite+aiosqlite:///{_TEST_DB.as_posix()}"
os.environ["NUM_SPLITS"] = "4"
os.environ["S3_BUCKET"] = "delta-bucket"

import httpx
import config

config.DB_URL = f"sqlite+aiosqlite:///{_TEST_DB.as_posix()}"
config.NUM_SPLITS = 4
config.BUCKET_NAME = "delta-bucket"

from main import app
from config import ColumnDef
from delta import log as delta_log


# ---------------------------------------------------------------------------
# Pure unit tests
# ---------------------------------------------------------------------------

def test_delta_type_mapping():
    assert delta_log._delta_type("int") == "integer"
    assert delta_log._delta_type("long") == "long"
    assert delta_log._delta_type("double") == "double"
    assert delta_log._delta_type("boolean") == "boolean"
    assert delta_log._delta_type("date") == "date"
    assert delta_log._delta_type("string") == "string"
    assert delta_log._delta_type("binary") == "binary"
    assert delta_log._delta_type("uuid") == "string"
    # Iceberg timestamp (no zone) -> Delta timestamp_ntz; timestamptz -> timestamp
    assert delta_log._delta_type("timestamp") == "timestamp_ntz"
    assert delta_log._delta_type("timestamptz") == "timestamp"
    # decimal preserved (spaces stripped)
    assert delta_log._delta_type("decimal(10, 2)") == "decimal(10,2)"
    # unknown -> string fallback
    assert delta_log._delta_type("interval") == "string"


def test_schema_string_is_valid_struct():
    cols = [
        ColumnDef(field_id=1, name="id", iceberg_type="long", nullable=False),
        ColumnDef(field_id=2, name="name", iceberg_type="string", nullable=True),
    ]
    s = delta_log._schema_string(cols)
    parsed = json.loads(s)
    assert parsed["type"] == "struct"
    assert [f["name"] for f in parsed["fields"]] == ["id", "name"]
    assert parsed["fields"][0]["type"] == "long"
    assert parsed["fields"][0]["nullable"] is False
    assert parsed["fields"][1]["type"] == "string"


def _mk_snap(table, version, hashes):
    """Build a minimal SnapshotState with content-addressed keys (one per hash)."""
    from iceberg.state_store import SnapshotState, SplitDescriptor
    tp = f"{config.WAREHOUSE_PREFIX}/{table.name}"
    snap = SnapshotState(
        snapshot_id=version * 1000, sequence_number=version,
        watermark_ms=1_700_000_000_000 + version,
        manifest_list_key="", manifest_file_key="", metadata_key="",
        version_hint_key="", table=table,
        table_path=tp, legacy_table_path=tp,
        version=version, uuid="x",
    )
    snap.splits = [
        SplitDescriptor(
            split_index=i, num_splits=len(hashes),
            object_key=f"{tp}/data/split-{i}-{h}.parquet",
            watermark_ms=snap.watermark_ms, table=table,
            record_count=10, file_size_in_bytes=123,
        )
        for i, h in enumerate(hashes)
    ]
    return snap


def test_previous_version_files_stay_servable_after_refresh():
    """After AUTO_REFRESH publishes a new version, the PRIOR version's pinned
    data files must still resolve (Fabric may reference them until it re-syncs
    the _delta_log). Regression for "underlying location does not exist".

    Also verifies the commit is a DIFF: an unchanged content-addressed split
    carries forward (no add, no remove) — a full add+remove of the same path
    would net the file out of the table for a replaying reader (data loss)."""
    import iceberg.state_store as ss
    import cache.lru_cache as cache
    from iceberg.state_store import register_snapshot, get_split_by_key
    from config import TableDef

    ss._snapshots.clear(); ss._history.clear()
    cache.unpin_all()
    delta_log.reset()
    try:
        tbl = TableDef(name="RefreshT", source_table="RefreshT",
                       schema=[ColumnDef(field_id=1, name="id",
                                         iceberg_type="long", nullable=False)])
        # split-0 changes (aaaa -> bbbb); split-1 is UNCHANGED (cccc).
        v1 = _mk_snap(tbl, 1, ["aaaaaaaaaaaa", "cccccccccccc"])
        v2 = _mk_snap(tbl, 2, ["bbbbbbbbbbbb", "cccccccccccc"])
        for snap in (v1, v2):
            for s in snap.splits:
                cache.pin_parquet(s.object_key, b"PAR1" + s.object_key.encode())
        register_snapshot(v1)
        register_snapshot(v2)
        delta_log.sync_all()

        commits = delta_log._commits["RefreshT"]
        assert len(commits) == 2
        c1 = [json.loads(l) for l in commits[1].splitlines() if l.strip()]
        adds = [a["add"]["path"] for a in c1 if "add" in a]
        removes = [a["remove"]["path"] for a in c1 if "remove" in a]
        # Only the changed split is added/removed; the unchanged split carries over.
        assert adds == ["data/split-0-bbbbbbbbbbbb.parquet"]
        assert removes == ["data/split-0-aaaaaaaaaaaa.parquet"]
        assert "data/split-1-cccccccccccc.parquet" not in removes

        old_key = v1.splits[0].object_key   # split-0 old
        new_key = v2.splits[0].object_key   # split-0 new
        # Both old and new files resolve (no 404) and are advertised for listing.
        assert get_split_by_key(old_key) is not None
        assert get_split_by_key(new_key) is not None
        objs = delta_log.delta_log_objects()
        assert old_key in objs and new_key in objs
    finally:
        ss._snapshots.clear(); ss._history.clear()
        cache.unpin_all()
        delta_log.reset()


def test_delta_log_keys_follow_snapshot_table_path_in_canonical_layout():
    """Delta commit files must be emitted under the snapshot's active table path.

    Regression: commits were emitted under db/<table>/_delta_log even when data
    files used canonical db/<server>/<database>/<schema>/<object>/data paths.
    """
    import iceberg.state_store as ss
    from config import TableDef

    ss._snapshots.clear(); ss._history.clear()
    delta_log.reset()

    saved_layout = config.OBJECT_PATH_LAYOUT
    try:
        config.OBJECT_PATH_LAYOUT = "canonical"
        tbl = TableDef(name="Address", source_table="SalesLT.Address", schema=config.TABLE_SCHEMA)
        snap = ss.build_table_snapshot(tbl, bucket="delta-bucket", warehouse_prefix=config.WAREHOUSE_PREFIX)
        delta_log.sync_all()
        objs = delta_log.delta_log_objects()

        expected_log_key = f"{snap.table_path}/_delta_log/00000000000000000000.json"
        legacy_log_key = f"{config.WAREHOUSE_PREFIX}/{tbl.name}/_delta_log/00000000000000000000.json"

        assert expected_log_key in objs
        assert legacy_log_key not in objs
    finally:
        config.OBJECT_PATH_LAYOUT = saved_layout
        ss._snapshots.clear(); ss._history.clear()
        delta_log.reset()


# ---------------------------------------------------------------------------
# Router integration in delta mode
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
async def delta_client():
    from demo.seed_db import seed_demo_database
    import db.executor as _executor
    from iceberg.state_store import build_snapshot

    # Set config in the fixture (not at module scope) so cross-module import
    # ordering can't clobber these values before the tests run.
    saved = (
        config.DB_URL,
        config.NUM_SPLITS,
        config.BUCKET_NAME,
        config.TABLE_FORMAT,
        config.OBJECT_PATH_LAYOUT,
        config.ENABLE_LEGACY_PATH_ALIASES,
    )
    config.DB_URL = f"sqlite+aiosqlite:///{_TEST_DB.as_posix()}"
    config.NUM_SPLITS = 4
    config.BUCKET_NAME = "delta-bucket"
    config.TABLE_FORMAT = "delta"
    config.OBJECT_PATH_LAYOUT = "canonical"
    config.ENABLE_LEGACY_PATH_ALIASES = False

    _executor._engine = None
    await seed_demo_database()

    build_snapshot(
        table_name=config.TABLE_NAME,
        num_splits=config.NUM_SPLITS,
        bucket=config.BUCKET_NAME,
        warehouse_prefix=config.WAREHOUSE_PREFIX,
    )

    delta_log.reset()
    delta_log.sync_all()

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
        yield c

    if _executor._engine is not None:
        await _executor._engine.dispose()
        _executor._engine = None
    delta_log.reset()
    (
        config.DB_URL,
        config.NUM_SPLITS,
        config.BUCKET_NAME,
        config.TABLE_FORMAT,
        config.OBJECT_PATH_LAYOUT,
        config.ENABLE_LEGACY_PATH_ALIASES,
    ) = saved
    if _TEST_DB.exists():
        _TEST_DB.unlink(missing_ok=True)


def _extract_keys(xml_bytes: bytes) -> list[str]:
    import xml.etree.ElementTree as ET
    root = ET.fromstring(xml_bytes)
    ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
    return [c.find("s3:Key", ns).text for c in root.findall("s3:Contents", ns)]


async def test_list_serves_delta_log_and_parquet_not_iceberg(delta_client):
    r = await delta_client.get(f"/delta-bucket?list-type=2&prefix={config.WAREHOUSE_PREFIX}/")
    assert r.status_code == 200
    keys = _extract_keys(r.content)
    assert any(k.endswith("_delta_log/00000000000000000000.json") for k in keys)
    assert any(k.endswith(".parquet") for k in keys)
    # Iceberg artifacts must NOT appear in delta mode.
    assert not any(k.endswith("metadata.json") for k in keys)
    assert not any(k.endswith(".avro") for k in keys)
    assert not any("version-hint" in k for k in keys)


async def test_get_commit_zero_has_protocol_metadata_and_adds(delta_client):
    r = await delta_client.get(f"/delta-bucket?list-type=2&prefix={config.WAREHOUSE_PREFIX}/")
    keys = _extract_keys(r.content)
    commit_key = next(k for k in keys if k.endswith("_delta_log/00000000000000000000.json"))

    r2 = await delta_client.get(f"/delta-bucket/{commit_key}")
    assert r2.status_code == 200
    assert r2.headers["content-type"].startswith("application/json")
    actions = [json.loads(line) for line in r2.text.splitlines() if line.strip()]

    protocol = next(a["protocol"] for a in actions if "protocol" in a)
    assert protocol["minReaderVersion"] == 1
    assert protocol["minWriterVersion"] == 2

    meta = next(a["metaData"] for a in actions if "metaData" in a)
    assert meta["format"]["provider"] == "parquet"
    schema = json.loads(meta["schemaString"])
    assert schema["type"] == "struct"
    assert len(schema["fields"]) > 0

    adds = [a["add"] for a in actions if "add" in a]
    assert len(adds) == config.NUM_SPLITS
    for add in adds:
        assert add["path"].startswith("data/")
        assert add["dataChange"] is True
        assert "numRecords" in json.loads(add["stats"])


async def test_get_data_parquet_in_delta_mode(delta_client):
    r = await delta_client.get(f"/delta-bucket?list-type=2&prefix={config.WAREHOUSE_PREFIX}/")
    keys = _extract_keys(r.content)
    parquet_key = next(k for k in keys if k.endswith(".parquet"))

    r2 = await delta_client.get(f"/delta-bucket/{parquet_key}")
    assert r2.status_code == 200
    table = pq.read_table(io.BytesIO(r2.content))
    assert table.num_rows > 0
    assert "id" in table.schema.names


async def test_unknown_delta_log_file_404(delta_client):
    # _last_checkpoint is probed by Delta readers; we don't emit it -> 404.
    key = f"{config.WAREHOUSE_PREFIX}/{config.TABLE_NAME}/_delta_log/_last_checkpoint"
    r = await delta_client.get(f"/delta-bucket/{key}")
    assert r.status_code == 404


async def test_delta_listing_is_canonical_and_hides_legacy_when_aliases_disabled(delta_client):
    r = await delta_client.get(f"/delta-bucket?list-type=2&prefix={config.WAREHOUSE_PREFIX}/")
    assert r.status_code == 200
    keys = _extract_keys(r.content)

    assert keys, "expected at least one listed key in delta mode"
    assert any("/_delta_log/" in k for k in keys)
    assert any("/data/split-" in k for k in keys)

    # Canonical paths should include db/<server>/<database>/<schema>/<object>/...
    # => at least 6 path segments before _delta_log or data.
    roots = []
    for k in keys:
        parts = k.split("/")
        if "_delta_log" in parts:
            roots.append(parts[:parts.index("_delta_log")])
        elif "data" in parts:
            roots.append(parts[:parts.index("data")])
    assert roots and all(len(rp) >= 5 for rp in roots)

    # Legacy db/<table>/... shape would have only 2 segments before data/_delta_log.
    assert not any(len(rp) == 2 for rp in roots)

"""
Tests for the native Delta Lake output mode (TABLE_FORMAT=delta).

Unit-tests the type mapping + schema string, and integration-tests the S3
router in delta mode (serves _delta_log/*.json commits + Parquet data files,
and does NOT serve Iceberg metadata/manifests).
"""
from __future__ import annotations

import io
import json
import pathlib

import pytest
import pyarrow.parquet as pq

_TEST_DB = pathlib.Path(__file__).parent / "test_delta.db"

import httpx
import config

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
    # Both timestamp variants use the broadly supported Delta timestamp type.
    assert delta_log._delta_type("timestamp") == "timestamp"
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


async def test_head_delta_log_directory_is_not_an_object(delta_client):
    r = await delta_client.get(f"/delta-bucket?list-type=2&prefix={config.WAREHOUSE_PREFIX}/")
    keys = _extract_keys(r.content)
    commit_key = next(k for k in keys if k.endswith("_delta_log/00000000000000000000.json"))
    log_directory = commit_key.rsplit("/", 1)[0]

    for suffix in ("", "/"):
        response = await delta_client.head(f"/delta-bucket/{log_directory}{suffix}")
        assert response.status_code == 404


async def test_head_last_checkpoint_is_optional(delta_client):
    r = await delta_client.get(f"/delta-bucket?list-type=2&prefix={config.WAREHOUSE_PREFIX}/")
    keys = _extract_keys(r.content)
    commit_key = next(k for k in keys if k.endswith("_delta_log/00000000000000000000.json"))
    checkpoint_key = commit_key.rsplit("/", 1)[0] + "/_last_checkpoint"

    response = await delta_client.head(f"/delta-bucket/{checkpoint_key}")

    assert response.status_code == 404


async def test_list_last_checkpoint_is_optional(delta_client):
    r = await delta_client.get(f"/delta-bucket?list-type=2&prefix={config.WAREHOUSE_PREFIX}/")
    keys = _extract_keys(r.content)
    commit_key = next(k for k in keys if k.endswith("_delta_log/00000000000000000000.json"))
    checkpoint_key = commit_key.rsplit("/", 1)[0] + "/_last_checkpoint"

    response = await delta_client.get(
        f"/delta-bucket?list-type=2&prefix={checkpoint_key}"
    )

    assert response.status_code == 200
    assert _extract_keys(response.content) == []
    import xml.etree.ElementTree as ET
    root = ET.fromstring(response.content)
    ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
    assert root.findtext("s3:Prefix", namespaces=ns) == checkpoint_key
    assert root.findtext("s3:KeyCount", namespaces=ns) == "0"


async def test_list_legacy_metadata_probe_is_empty(delta_client):
    listing = await delta_client.get(
        f"/delta-bucket?list-type=2&prefix={config.WAREHOUSE_PREFIX}/"
    )
    commit_key = next(
        key for key in _extract_keys(listing.content)
        if key.endswith("_delta_log/00000000000000000000.json")
    )
    table_root = commit_key.split("/_delta_log/", 1)[0]
    metadata_probe = f"{table_root}/_metadata/table.json.gz/"

    response = await delta_client.get(
        "/delta-bucket",
        params={
            "list-type": "2",
            "max-keys": "1000",
            "delimiter": "/",
            "prefix": metadata_probe,
        },
    )

    assert response.status_code == 200
    assert _extract_keys(response.content) == []
    import xml.etree.ElementTree as ET
    root = ET.fromstring(response.content)
    ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
    assert root.findtext("s3:Prefix", namespaces=ns) == metadata_probe
    assert root.findtext("s3:KeyCount", namespaces=ns) == "0"


async def test_delta_log_echoes_fabric_start_after_probe(delta_client):
    listing = await delta_client.get(
        f"/delta-bucket?list-type=2&prefix={config.WAREHOUSE_PREFIX}/"
    )
    commit_key = next(
        key for key in _extract_keys(listing.content)
        if key.endswith("_delta_log/00000000000000000000.json")
    )
    prefix = commit_key.rsplit("/", 1)[0]
    start_after = f"{prefix}00000000000000000000.jsom"
    response = await delta_client.get(
        "/delta-bucket",
        params={
            "list-type": "2",
            "max-keys": "1000",
            "delimiter": "/",
            "prefix": prefix,
            "start-after": start_after,
        },
    )

    assert response.status_code == 200
    assert _extract_keys(response.content) == []
    import xml.etree.ElementTree as ET
    root = ET.fromstring(response.content)
    ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
    assert root.findtext("s3:StartAfter", namespaces=ns) == start_after


async def test_delta_listing_paginates_with_continuation_token(delta_client):
    import xml.etree.ElementTree as ET

    prefix = f"{config.WAREHOUSE_PREFIX}/"
    complete = await delta_client.get(
        "/delta-bucket",
        params={"list-type": "2", "prefix": prefix},
    )
    expected_keys = _extract_keys(complete.content)
    actual_keys: list[str] = []
    continuation_token = None

    for _page_number in range(10):
        params = {"list-type": "2", "prefix": prefix, "max-keys": "2"}
        if continuation_token is not None:
            params["continuation-token"] = continuation_token
        response = await delta_client.get("/delta-bucket", params=params)
        assert response.status_code == 200

        root = ET.fromstring(response.content)
        ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
        page_keys = _extract_keys(response.content)
        actual_keys.extend(page_keys)
        assert root.findtext("s3:KeyCount", namespaces=ns) == str(len(page_keys))
        assert root.findtext("s3:MaxKeys", namespaces=ns) == "2"
        if continuation_token is not None:
            assert root.findtext("s3:ContinuationToken", namespaces=ns) == continuation_token

        if root.findtext("s3:IsTruncated", namespaces=ns) == "false":
            break
        continuation_token = root.findtext("s3:NextContinuationToken", namespaces=ns)
        assert continuation_token
    else:
        pytest.fail("ListObjectsV2 pagination did not terminate")

    assert actual_keys == expected_keys


@pytest.mark.parametrize("max_keys", ["invalid", "-1", "1001"])
async def test_delta_listing_rejects_invalid_max_keys(delta_client, max_keys):
    response = await delta_client.get(
        "/delta-bucket",
        params={"list-type": "2", "max-keys": max_keys},
    )

    assert response.status_code == 400
    assert b"<Code>InvalidArgument</Code>" in response.content


async def test_virtual_delta_listing_materializes_before_log_discovery(monkeypatch, tmp_path):
    """Fabric's first ListObjectsV2 request must publish virtual Delta commit 0."""
    import db.executor as executor
    import iceberg.state_store as state_store
    from iceberg.state_store import build_table_snapshot
    from runtime import materializer

    saved_config = (
        config.DB_URL, config.BUCKET_NAME, config.TABLE_FORMAT,
        config.NUM_SPLITS, config.TABLE_NAME, config.DB_SOURCE_TABLE,
    )
    saved_snapshots = state_store._snapshots.copy()
    saved_history = {name: list(history) for name, history in state_store._history.items()}
    monkeypatch.setattr(config, "DB_URL", f"sqlite+aiosqlite:///{tmp_path / 'virtual-delta.db'}")
    monkeypatch.setattr(config, "BUCKET_NAME", "virtual-delta-bucket")
    monkeypatch.setattr(config, "TABLE_FORMAT", "delta")
    monkeypatch.setattr(config, "MATERIALIZE_MODE", "virtual")
    monkeypatch.setattr(config, "NUM_SPLITS", 1)
    monkeypatch.setattr(config, "TABLE_NAME", "sales")
    monkeypatch.setattr(config, "DB_SOURCE_TABLE", "sales")

    from demo.seed_db import seed_demo_database
    await seed_demo_database()
    executor._engine = None
    materializer._locks.clear()
    snap = build_table_snapshot(config.TABLES[0], config.BUCKET_NAME, config.WAREHOUSE_PREFIX)
    from delta import log as delta_log
    delta_log.reset()
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get(
                f"/{config.BUCKET_NAME}",
                params={"list-type": "2", "prefix": f"{snap.table_path}/_delta_log/"},
            )
        assert response.status_code == 200
        assert "00000000000000000000.json" in response.text
        assert delta_log.get_commit_bytes(
            f"{snap.table_path}/_delta_log/00000000000000000000.json"
        )
    finally:
        if executor._engine is not None:
            await executor._engine.dispose()
            executor._engine = None
        state_store._snapshots.clear()
        state_store._history.clear()
        state_store._snapshots.update(saved_snapshots)
        state_store._history.update(saved_history)
        (
            config.DB_URL, config.BUCKET_NAME, config.TABLE_FORMAT,
            config.NUM_SPLITS, config.TABLE_NAME, config.DB_SOURCE_TABLE,
        ) = saved_config
        delta_log.reset()


async def test_get_commit_zero_trailing_slash_is_normalized(delta_client):
    r = await delta_client.get(f"/delta-bucket?list-type=2&prefix={config.WAREHOUSE_PREFIX}/")
    keys = _extract_keys(r.content)
    commit_key = next(k for k in keys if k.endswith("_delta_log/00000000000000000000.json"))

    r2 = await delta_client.get(f"/delta-bucket/{commit_key}/")
    assert r2.status_code == 200
    assert r2.headers["content-type"].startswith("application/json")
    assert r2.text == (await delta_client.get(f"/delta-bucket/{commit_key}")).text


def _extract_key_etags(xml_bytes: bytes) -> dict[str, str]:
    import xml.etree.ElementTree as ET
    root = ET.fromstring(xml_bytes)
    ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
    return {
        c.find("s3:Key", ns).text: c.find("s3:ETag", ns).text
        for c in root.findall("s3:Contents", ns)
    }


async def test_delta_log_etag_matches_between_list_and_get(delta_client):
    """ListObjectsV2's ETag for a _delta_log commit must equal the ETag the
    same key returns from GET/HEAD (content-hash), not a hash of the key.

    S3A/Ozone-style clients treat a list-vs-get ETag mismatch as a hard
    consistency error (this broke Ozone's xTable/S3A integration before
    Ozone 1.4.1), so the two must agree for Fabric's Direct Lake reader.
    """
    r = await delta_client.get(f"/delta-bucket?list-type=2&prefix={config.WAREHOUSE_PREFIX}/")
    etags = _extract_key_etags(r.content)
    commit_key = next(k for k in etags if k.endswith("_delta_log/00000000000000000000.json"))
    list_etag = etags[commit_key]

    r2 = await delta_client.get(f"/delta-bucket/{commit_key}")
    assert r2.status_code == 200
    get_etag = r2.headers["etag"]

    assert list_etag == get_etag
    # And it must be a real content hash, not a hash of the key string.
    import hashlib
    content_hash = f'"{hashlib.md5(r2.content, usedforsecurity=False).hexdigest()}"'
    assert list_etag == content_hash


async def test_delta_log_etag_matches_between_list_and_head(delta_client):
    r = await delta_client.get(f"/delta-bucket?list-type=2&prefix={config.WAREHOUSE_PREFIX}/")
    etags = _extract_key_etags(r.content)
    commit_key = next(k for k in etags if k.endswith("_delta_log/00000000000000000000.json"))

    r2 = await delta_client.head(f"/delta-bucket/{commit_key}")
    assert r2.status_code == 200
    assert r2.headers["etag"] == etags[commit_key]
    assert r2.headers["last-modified"]


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


@pytest.mark.parametrize("method", ["get", "head"])
async def test_delta_crc_probe_is_absent_without_materializing(
    delta_client, monkeypatch, method
):
    from runtime import materializer

    async def unexpected_materialization(_snap):
        pytest.fail("optional Delta sidecar probe triggered materialization")

    monkeypatch.setattr(materializer, "ensure_snapshot_materialized", unexpected_materialization)
    key = (
        f"{config.WAREHOUSE_PREFIX}/{config.TABLE_NAME}/_delta_log/"
        "00000000000000000000.crc"
    )
    response = await getattr(delta_client, method)(f"/delta-bucket/{key}")

    assert response.status_code == 404


async def test_delta_crc_listing_is_empty_without_materializing(delta_client, monkeypatch):
    from runtime import materializer

    async def unexpected_materialization(_snap):
        pytest.fail("optional Delta sidecar listing triggered materialization")

    monkeypatch.setattr(materializer, "ensure_snapshot_materialized", unexpected_materialization)
    prefix = (
        f"{config.WAREHOUSE_PREFIX}/{config.TABLE_NAME}/_delta_log/"
        "00000000000000000000.crc"
    )
    response = await delta_client.get(
        "/delta-bucket",
        params={"list-type": "2", "prefix": prefix},
    )

    assert response.status_code == 200
    assert _extract_keys(response.content) == []


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

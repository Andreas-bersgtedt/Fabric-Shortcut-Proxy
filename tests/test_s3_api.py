"""
Integration tests for the S3 API frontend.

Spins up the FastAPI app in-process using httpx's ASGITransport.
SQLite file-based database seeded with test rows.
"""
from __future__ import annotations

import json
import pathlib

import pytest
import pyarrow.parquet as pq
import io

_TEST_DB = pathlib.Path(__file__).parent / "test_poc.db"

import httpx
import config

from main import app


@pytest.fixture(scope="module")
async def client():
    # Seed demo DB and build snapshot before the ASGI transport is exercised,
    # because ASGITransport does not fire the FastAPI lifespan automatically.
    import config
    import db.executor as _executor
    from iceberg.state_store import build_snapshot
    from demo.seed_db import seed_demo_database

    saved = (
        config.DB_URL,
        config.NUM_SPLITS,
        config.BUCKET_NAME,
        config.TABLE_NAME,
        config.DB_SOURCE_TABLE,
    )
    config.DB_URL = f"sqlite+aiosqlite:///{_TEST_DB.as_posix()}"
    config.NUM_SPLITS = 4
    config.BUCKET_NAME = "test-bucket"
    config.TABLE_NAME = "sales"
    config.DB_SOURCE_TABLE = "sales"

    await seed_demo_database()

    # Reset the global engine so it picks up the DB_URL we set above.
    _executor._engine = None

    build_snapshot(
        table_name=config.TABLE_NAME,
        num_splits=config.NUM_SPLITS,
        bucket=config.BUCKET_NAME,
        warehouse_prefix=config.WAREHOUSE_PREFIX,
    )

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
        yield c

    # Dispose engine before deleting the file (Windows locks open file handles)
    if _executor._engine is not None:
        await _executor._engine.dispose()
        _executor._engine = None

    (
        config.DB_URL,
        config.NUM_SPLITS,
        config.BUCKET_NAME,
        config.TABLE_NAME,
        config.DB_SOURCE_TABLE,
    ) = saved

    # Clean up test DB file
    if _TEST_DB.exists():
        _TEST_DB.unlink(missing_ok=True)


async def test_list_objects_returns_metadata_and_splits(client):
    r = await client.get(f"/test-bucket?list-type=2&prefix={config.WAREHOUSE_PREFIX}/")
    assert r.status_code == 200
    body = r.text
    assert "metadata.json" in body
    assert ".avro" in body
    assert ".parquet" in body


async def test_head_metadata_json(client):
    # First list to discover the metadata key
    r = await client.get(f"/test-bucket?list-type=2&prefix={config.WAREHOUSE_PREFIX}/")
    assert r.status_code == 200
    # Extract metadata key from XML
    import xml.etree.ElementTree as ET
    root = ET.fromstring(r.content)
    ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
    keys = [c.find("s3:Key", ns).text for c in root.findall("s3:Contents", ns)]
    meta_key = next(k for k in keys if k.endswith("metadata.json"))

    r2 = await client.head(f"/test-bucket/{meta_key}")
    assert r2.status_code == 200
    assert int(r2.headers["content-length"]) > 0


async def test_get_metadata_json_is_valid_iceberg(client):
    r = await client.get("/test-bucket?list-type=2&prefix=")
    keys = _extract_keys(r.content)
    meta_key = next(k for k in keys if k.endswith("metadata.json"))

    r2 = await client.get(f"/test-bucket/{meta_key}")
    assert r2.status_code == 200
    metadata = json.loads(r2.content)
    assert metadata["format-version"] == 2
    assert "current-snapshot-id" in metadata
    assert len(metadata["schemas"]) > 0
    assert len(metadata["snapshots"]) > 0


async def test_get_manifest_list_avro(client):
    r = await client.get("/test-bucket?list-type=2&prefix=")
    keys = _extract_keys(r.content)
    ml_key = next(k for k in keys if "snap-" in k and k.endswith(".avro"))

    r2 = await client.get(f"/test-bucket/{ml_key}")
    assert r2.status_code == 200
    assert len(r2.content) > 0
    # Validate it's readable Avro
    import fastavro
    records = list(fastavro.reader(io.BytesIO(r2.content)))
    assert len(records) == 1  # one manifest file entry
    assert "manifest_path" in records[0]


async def test_get_data_parquet_returns_rows(client):
    r = await client.get("/test-bucket?list-type=2&prefix=")
    keys = _extract_keys(r.content)
    parquet_key = next(k for k in keys if k.endswith(".parquet"))

    r2 = await client.get(f"/test-bucket/{parquet_key}")
    assert r2.status_code == 200
    table = pq.read_table(io.BytesIO(r2.content))
    assert table.num_rows > 0
    assert "id" in table.schema.names
    assert "region" in table.schema.names


async def test_range_request_on_parquet(client):
    r = await client.get("/test-bucket?list-type=2&prefix=")
    keys = _extract_keys(r.content)
    parquet_key = next(k for k in keys if k.endswith(".parquet"))

    # First: get full object
    r_full = await client.get(f"/test-bucket/{parquet_key}")
    full_size = len(r_full.content)

    # Then: request first 1024 bytes
    r_range = await client.get(f"/test-bucket/{parquet_key}", headers={"Range": "bytes=0-1023"})
    assert r_range.status_code == 206
    assert len(r_range.content) == 1024
    assert r_range.headers["content-range"] == f"bytes 0-1023/{full_size}"


async def test_unknown_bucket_returns_404(client):
    r = await client.get("/nonexistent-bucket?list-type=2")
    assert r.status_code == 404


async def test_unknown_key_returns_404(client):
    r = await client.get("/test-bucket/warehouse/db/sales/data/does-not-exist.parquet")
    assert r.status_code == 404


def _extract_keys(xml_bytes: bytes) -> list[str]:
    import xml.etree.ElementTree as ET
    root = ET.fromstring(xml_bytes)
    ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
    return [c.find("s3:Key", ns).text for c in root.findall("s3:Contents", ns)]


# ---------------------------------------------------------------------------
# /_manager on an Agent: bounce to the Manager console (never a SigV4 403).
# ---------------------------------------------------------------------------

async def test_agent_manager_path_redirects_to_manager_console(monkeypatch):
    monkeypatch.setattr(config, "MANAGER_URL", "http://127.0.0.1:9200", raising=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                 base_url="http://agent") as c:
        r = await c.get("/_manager", follow_redirects=False)
        assert r.status_code == 307
        assert r.headers["location"] == "http://127.0.0.1:9200/_manager"
        # sub-paths carry over too
        r2 = await c.get("/_manager/api/fleet", follow_redirects=False)
        assert r2.status_code == 307
        assert r2.headers["location"] == "http://127.0.0.1:9200/_manager/api/fleet"


async def test_agent_manager_path_hint_when_standalone(monkeypatch):
    monkeypatch.setattr(config, "MANAGER_URL", "", raising=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                 base_url="http://agent") as c:
        r = await c.get("/_manager", follow_redirects=False)
        assert r.status_code == 404
        assert "Manager" in r.json()["detail"]


async def test_agent_favicon_serves_brand_icon():
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                 base_url="http://agent") as c:
            response = await c.get("/favicon.ico")
            assert response.status_code == 200
            assert response.headers["content-type"] == "image/png"
            assert response.content.startswith(b"\x89PNG\r\n\x1a\n")

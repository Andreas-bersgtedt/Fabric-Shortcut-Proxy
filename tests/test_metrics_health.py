"""
Tests for operational endpoints (Plan items H1 & H2):
  - /healthz, /readyz
  - /metrics (Prometheus), /_admin/stats (JSON)
  - metrics are actually recorded when objects are served

Uses httpx ASGITransport in-process, mirroring tests/test_s3_api.py.
"""
from __future__ import annotations

import os
import pathlib

import pytest

_TEST_DB = pathlib.Path(__file__).parent / "test_ops.db"
os.environ["DB_URL"] = f"sqlite+aiosqlite:///{_TEST_DB.as_posix()}"
os.environ["NUM_SPLITS"] = "4"
os.environ["S3_BUCKET"] = "ops-bucket"

import httpx
import config

config.DB_URL = f"sqlite+aiosqlite:///{_TEST_DB.as_posix()}"
config.NUM_SPLITS = 4
config.BUCKET_NAME = "ops-bucket"

from main import app
from observability import metrics


@pytest.fixture(scope="module")
async def client():
    from demo.seed_db import seed_demo_database
    await seed_demo_database()

    import db.executor as _executor
    from iceberg.state_store import build_snapshot

    _executor._engine = None
    build_snapshot(
        table_name=config.TABLE_NAME,
        num_splits=config.NUM_SPLITS,
        bucket=config.BUCKET_NAME,
        warehouse_prefix=config.WAREHOUSE_PREFIX,
    )
    metrics.reset()
    # Isolate from other test modules that may have populated the shared caches.
    import cache.lru_cache as _cache
    for _c in (_cache._metadata_cache, _cache._parquet_cache):
        _c._store.clear()
        _c._current_bytes = 0

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c

    if _executor._engine is not None:
        await _executor._engine.dispose()
        _executor._engine = None


async def test_healthz(client):
    r = await client.get("/healthz")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


async def test_readyz(client):
    r = await client.get("/readyz")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ready"
    assert body["checks"] == {"snapshot": True, "database": True}


async def test_metrics_prometheus_format(client):
    # Generate at least one S3 request so the labelled counter exists.
    await client.get(f"/{config.BUCKET_NAME}/?list-type=2")

    r = await client.get("/metrics")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/plain")
    body = r.text
    assert "# TYPE s3_requests_total counter" in body
    assert "sql_query_duration_seconds_count" in body
    assert "process_uptime_seconds" in body


async def test_admin_stats_json(client):
    r = await client.get("/_admin/stats")
    assert r.status_code == 200
    body = r.json()
    assert "counters" in body
    assert "sql_latency" in body
    assert "cache" in body
    assert body["cache"]["parquet"]["max_bytes"] > 0


async def test_metrics_record_on_object_serving(client):
    bucket = config.BUCKET_NAME
    meta_key = f"warehouse/db/{config.TABLE_NAME}/metadata/v1.metadata.json"

    # Serve metadata twice: first read builds+caches, second should be a cache hit.
    await client.get(f"/{bucket}/{meta_key}")
    await client.get(f"/{bucket}/{meta_key}")

    # Serve a data parquet -> triggers SQL + bytes served.
    snap_id_key = None
    listing = await client.get(f"/{bucket}/?list-type=2&prefix=warehouse/db/{config.TABLE_NAME}/data/")
    assert listing.status_code == 200
    # Pull one data key out of the listing XML.
    import re
    m = re.search(r"<Key>([^<]+\.parquet)</Key>", listing.text)
    assert m, "expected a parquet key in the listing"
    snap_id_key = m.group(1)
    data_resp = await client.get(f"/{bucket}/{snap_id_key}")
    assert data_resp.status_code == 200

    stats = (await client.get("/_admin/stats")).json()
    counters = stats["counters"]

    # s3_requests_total has entries for get/list.
    assert "s3_requests_total" in counters
    ops = {tuple(sorted(e["labels"].items())): e["value"] for e in counters["s3_requests_total"]}
    # at least one GET for metadata and one for data recorded
    get_kinds = [dict(k) for k in ops.keys() if dict(k).get("op") == "get"]
    assert any(d.get("kind") == "metadata" for d in get_kinds)
    assert any(d.get("kind") == "data" for d in get_kinds)

    # bytes served is positive
    assert "s3_bytes_served_total" in counters
    assert counters["s3_bytes_served_total"][0]["value"] > 0

    # cache produced at least one hit and one miss on the metadata cache
    cache_events = {
        (dict(e["labels"]).get("cache"), dict(e["labels"]).get("result")): e["value"]
        for e in counters["cache_events_total"]
    }
    assert cache_events.get(("metadata", "hit"), 0) >= 1
    assert cache_events.get(("metadata", "miss"), 0) >= 1

    # at least one SQL query was timed
    assert stats["sql_latency"]["count"] >= 1

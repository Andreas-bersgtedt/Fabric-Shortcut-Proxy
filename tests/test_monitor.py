"""
Monitor dashboard tests — querystats aggregation + the /_monitor API, exercised
in an isolated FastAPI app (independent of main.py's ENABLE_MONITOR mount order).
"""
from __future__ import annotations

import os

os.environ.setdefault("DB_URL", "sqlite+aiosqlite:///:memory:")
os.environ.setdefault("S3_BUCKET", "test-bucket")

import httpx
import pytest
from fastapi import FastAPI

from observability import querystats, trace
from monitor.router import router as monitor_router


@pytest.fixture(autouse=True)
def _isolate():
    querystats.reset()
    trace.reset()
    yield
    querystats.reset()
    trace.reset()


@pytest.fixture
async def client():
    app = FastAPI()
    app.include_router(monitor_router)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                 base_url="http://test") as c:
        yield c


# ---------------------------------------------------------------------------
# querystats
# ---------------------------------------------------------------------------

def test_querystats_aggregates_and_cache_ratio():
    querystats.record_query(table="Product", split_index=0, sql_ms=40, gen_ms=10,
                            total_ms=52, rows=6250, resp_bytes=1000, cache_hit=False)
    querystats.record_query(table="Product", split_index=1, sql_ms=0, gen_ms=0,
                            total_ms=1, rows=6250, resp_bytes=1000, cache_hit=True)
    s = querystats.summary()
    p = s["tables"]["Product"]
    assert p["data_requests"] == 2
    assert p["cache_hits"] == 1
    assert p["cache_hit_ratio"] == 0.5
    assert p["rows_served"] == 12500
    assert p["avg_sql_ms"] == 20.0          # (40 + 0) / 2
    assert p["max_total_ms"] == 52.0
    assert len(s["recent"]) == 2
    assert s["recent"][0]["split"] == 1     # newest first


def test_querystats_percentiles_windowed():
    for i in range(10):
        querystats.record_query(table="T", split_index=i, sql_ms=i * 10, gen_ms=5,
                                total_ms=i * 10 + 5, rows=1, resp_bytes=1, cache_hit=False)
    p = querystats.summary()["tables"]["T"]
    assert p["p95_total_ms"] >= p["p50_total_ms"]
    assert p["max_total_ms"] == 95.0


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------

async def test_index_served(client):
    r = await client.get("/_monitor/")
    assert r.status_code == 200
    assert "Iceberg Proxy Monitor" in r.text


async def test_summary_shape(client):
    querystats.record_query(table="Product", split_index=0, sql_ms=30, gen_ms=8,
                            total_ms=40, rows=100, resp_bytes=500, cache_hit=False)
    r = await client.get("/_monitor/api/summary")
    assert r.status_code == 200
    d = r.json()
    assert "totals" in d and "tables" in d and "recent_queries" in d
    assert "sources" in d
    assert d["table_format"] in ("iceberg", "delta")
    assert d["totals"]["data_requests"] >= 1
    # Per-table rows are snapshot-backed only; recent_queries always reflects
    # raw query activity (no snapshot registered in this isolated app).
    assert d["recent_queries"][0]["table"] == "Product"
    assert d["recent_queries"][0]["sql_ms"] == 30.0
    assert "auto_refresh" in d["refresh"]


async def test_reset_clears(client):
    querystats.record_query(table="X", split_index=0, sql_ms=1, gen_ms=1,
                            total_ms=2, rows=1, resp_bytes=1, cache_hit=False)
    r = await client.post("/_monitor/api/reset")
    assert r.status_code == 200
    assert querystats.summary()["tables"] == {}


async def test_logs_tail_and_search(client):
    from observability.logbuffer import get_buffer

    buf = get_buffer()
    buf.clear()
    buf.append("startup bucket=demo")
    buf.append("\x1b[31merror\x1b[0m sql_query_id=42 failed to connect")
    buf.append("request served table=Product")

    r = await client.get("/_monitor/api/logs")
    assert r.status_code == 200
    d = r.json()
    assert d["total"] == 3
    assert d["capacity"] == 1000
    assert d["lines"][-1] == "request served table=Product"
    # ANSI colour codes are stripped before buffering.
    assert "\x1b" not in "".join(d["lines"])

    r = await client.get("/_monitor/api/logs", params={"q": "sql_query_id"})
    d = r.json()
    assert d["returned"] == 1
    assert "sql_query_id=42" in d["lines"][0]
    assert d["total"] == 3

    r = await client.get("/_monitor/api/logs", params={"limit": 1})
    d = r.json()
    assert d["returned"] == 1
    assert d["lines"] == ["request served table=Product"]
    buf.clear()


async def test_logs_suppress_self_poll(client):
    from observability.logbuffer import get_buffer

    buf = get_buffer()
    buf.clear()
    # The monitor's own log-tail polls must not enter the buffer (else a search
    # echoes itself as a false positive).
    buf.append('httpx HTTP Request: GET http://127.0.0.1:9001/_monitor/api/logs?limit=1000&q=Widget "HTTP/1.1 200 OK"')
    buf.append("request served table=Widget")

    r = await client.get("/_monitor/api/logs", params={"q": "Widget"})
    d = r.json()
    assert d["returned"] == 1
    assert d["lines"] == ["request served table=Widget"]
    buf.clear()


def test_self_poll_suppressed_through_real_logging_handler():
    # End-to-end: an httpx request log emitted through the configured stdlib
    # logging path must not reach the buffer (proves the wiring, not just append).
    import logging

    from observability.logbuffer import get_buffer
    from observability.logging import configure_logging

    configure_logging()
    buf = get_buffer()
    buf.clear()
    logging.getLogger().setLevel(logging.INFO)  # pytest may leave root at WARNING
    logging.getLogger("httpx").info(
        'HTTP Request: GET http://127.0.0.1:9000/_monitor/api/logs?limit=1000&q=zzz "HTTP/1.1 200 OK"'
    )
    logging.getLogger("app").info("real work q=zzz table=Orders")
    lines = buf.tail()
    assert any("real work" in ln for ln in lines)                 # sanity: buffering works
    assert not any("/_monitor/api/logs" in ln for ln in lines)    # self-poll suppressed
    buf.clear()


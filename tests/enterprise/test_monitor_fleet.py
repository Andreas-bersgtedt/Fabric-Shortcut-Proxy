"""
Fleet monitor aggregation tests (control.monitor_agg.merge_summaries) + the
Manager's /_monitor proxy router shape.
"""
from __future__ import annotations

import httpx
import pytest
from fastapi import FastAPI

from enterprise.control.cluster_health import aggregate_health
from enterprise.control.monitor_agg import merge_summaries
from enterprise.control.monitor_proxy import create_monitor_proxy_router, _fleet_agent_base_urls


class _FakeRegistry:
    def __init__(self, records, alive_ids=None):
        self._records = records
        self._alive_ids = set(alive_ids or [])

    def list_public(self):
        return list(self._records)

    def is_alive(self, agent_id):
        return agent_id in self._alive_ids


def _agent_summary(*, table="sales", requests=0, data_requests=0, cache_hit=None,
                   avg_total=0.0, bytes_served=0, parquet_gens=0, version=1,
                   splits=4, total_records=100, connection="default", errors=0):
    return {
        "table_format": "iceberg",
        "uptime_seconds": 10,
        "totals": {
            "tables": 1, "connections": 1, "cache_hit_ratio": cache_hit,
            "parquet_generations": parquet_gens, "bytes_served": bytes_served,
            "source_unavailable": 0, "data_requests": data_requests,
        },
        "refresh": {"auto_refresh": False},
        "cache": {"parquet_pinned": {"entries": splits, "bytes": bytes_served}},
        "sql_latency": {"count": data_requests, "avg_seconds": avg_total / 1000.0, "max_seconds": 0.2},
        "tables": [{
            "table": table, "version": version, "snapshot_id": 1, "splits": splits,
            "total_records": total_records, "connection": connection,
            "requests": requests, "errors": errors, "probe_404s": 0,
            "metadata_reads": 0, "manifest_reads": 0, "delta_log_reads": 0,
            "data_reads": data_requests, "data_requests": data_requests,
            "rows_served": data_requests * 10, "bytes_served": bytes_served,
            "cache_hit_ratio": cache_hit, "avg_sql_ms": avg_total, "p95_sql_ms": avg_total,
            "avg_gen_ms": 0, "avg_total_ms": avg_total, "p95_total_ms": avg_total,
            "max_total_ms": avg_total, "last_read_ts": 1000.0,
        }],
        "recent_queries": [{"table": table, "ts": 1000.0, "total_ms": avg_total}],
    }


def test_merge_empty_returns_valid_shape():
    m = merge_summaries([])
    assert m["totals"]["tables"] == 0
    assert m["tables"] == [] and m["agents"] == 0
    assert m["totals"]["data_requests"] == 0
    assert m["key_vault"] == {}


def test_merge_preserves_key_vault_status():
    kv = {"enabled": True, "vault": "myvault.vault.azure.net", "status": "ok"}
    m = merge_summaries([{"key_vault": {}}, {"key_vault": kv}])
    assert m["key_vault"] == kv


def test_merge_sums_counts_across_agents():
    a = _agent_summary(requests=5, data_requests=4, bytes_served=1000, parquet_gens=2)
    b = _agent_summary(requests=3, data_requests=6, bytes_served=500, parquet_gens=1)
    m = merge_summaries([a, b])
    assert m["agents"] == 2
    assert m["totals"]["tables"] == 1
    assert m["totals"]["data_requests"] == 10
    assert m["totals"]["bytes_served"] == 1500
    assert m["totals"]["parquet_generations"] == 3
    row = m["tables"][0]
    assert row["requests"] == 8            # 5 + 3
    assert row["data_requests"] == 10
    assert row["bytes_served"] == 1500


def test_merge_weights_latency_and_cache_by_data_requests():
    # Agent A: 1 req @ 100ms, hit 0%; Agent B: 9 reqs @ 200ms, hit 100%.
    a = _agent_summary(data_requests=1, avg_total=100.0, cache_hit=0.0)
    b = _agent_summary(data_requests=9, avg_total=200.0, cache_hit=1.0)
    m = merge_summaries([a, b])
    row = m["tables"][0]
    # weighted avg = (100*1 + 200*9)/10 = 190
    assert abs(row["avg_total_ms"] - 190.0) < 1e-6
    # weighted cache hit = (0*1 + 1*9)/10 = 0.9
    assert abs(row["cache_hit_ratio"] - 0.9) < 1e-6
    assert abs(m["totals"]["cache_hit_ratio"] - 0.9) < 1e-6


def test_merge_static_fields_take_representative_value():
    a = _agent_summary(version=1, splits=4, total_records=100)
    b = _agent_summary(version=2, splits=4, total_records=100)
    m = merge_summaries([a, b])
    row = m["tables"][0]
    assert row["version"] == 2              # latest wins
    assert row["splits"] == 4
    assert row["total_records"] == 100
    assert row["connection"] == "default"


def test_merge_distinct_tables_and_connections():
    a = _agent_summary(table="sales", connection="default")
    b = _agent_summary(table="orders", connection="warehouse_pg")
    m = merge_summaries([a, b])
    assert m["totals"]["tables"] == 2
    assert m["totals"]["connections"] == 2
    assert [t["table"] for t in m["tables"]] == ["orders", "sales"]  # sorted


def test_merge_zero_requests_nulls_cache_ratio():
    m = merge_summaries([_agent_summary(data_requests=0, cache_hit=None)])
    assert m["tables"][0]["cache_hit_ratio"] is None


def test_merge_recent_queries_sorted_and_capped():
    summaries = []
    for i in range(70):
        s = _agent_summary()
        s["recent_queries"] = [{"table": "sales", "ts": float(i)}]
        summaries.append(s)
    m = merge_summaries(summaries)
    assert len(m["recent_queries"]) == 60
    assert m["recent_queries"][0]["ts"] == 69.0     # newest first


def test_monitor_discovers_external_registered_agents():
    registry = _FakeRegistry(
        [
            {"agent_id": "live", "host": "192.0.2.10", "bind_host": "192.0.2.10", "port": 9000},
            {"agent_id": "dead", "host": "192.0.2.11", "bind_host": "192.0.2.11", "port": 9000},
        ],
        alive_ids={"live"},
    )

    assert _fleet_agent_base_urls([], registry) == ["http://192.0.2.10:9000"]


def test_cluster_health_includes_external_registered_agents():
    registry = _FakeRegistry(
        [{
            "agent_id": "fsp-materializer-smoke",
            "seconds_since_heartbeat": 0.5,
            "health": {"cpu_pct": 7.0, "mem_bytes": 1234},
            "serving_tables": ["SO_Header"],
        }],
        alive_ids={"fsp-materializer-smoke"},
    )

    snapshot = aggregate_health(registry, [])

    assert snapshot["status"] == "Healthy"
    assert snapshot["agents"][0]["agent_id"] == "fsp-materializer-smoke"
    assert snapshot["agents"][0]["serving_tables"] == ["SO_Header"]
    assert snapshot["resources"]["memory_bytes"] == 1234


# ---- proxy router (no live agents => empty-but-valid) ---------------------

@pytest.fixture
def proxy_app():
    a = FastAPI()
    a.include_router(create_monitor_proxy_router(supervisors=[]))
    return a


async def test_proxy_summary_with_no_agents(proxy_app):
    transport = httpx.ASGITransport(app=proxy_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.get("/_monitor/api/summary")
        assert r.status_code == 200
        d = r.json()
        assert d["agents_total"] == 0
        assert d["totals"]["tables"] == 0
        r2 = await c.post("/_monitor/api/reset")
        assert r2.status_code == 200 and r2.json()["reset_agents"] == 0


async def test_proxy_open_mirror_with_no_agents(proxy_app):
    transport = httpx.ASGITransport(app=proxy_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.get("/_monitor/api/open-mirror")
        assert r.status_code == 200
        d = r.json()
        assert d["agents_total"] == 0
        assert d["targets"] == []
        assert d["totals"]["tables"] == 0


async def test_proxy_logs_with_no_agents(proxy_app):
    from observability.logbuffer import get_buffer

    buf = get_buffer()
    buf.clear()
    buf.append("manager startup ready")
    buf.append("gateway routed request table=Product")

    transport = httpx.ASGITransport(app=proxy_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.get("/_monitor/api/logs")
        assert r.status_code == 200
        d = r.json()
        assert d["agents_total"] == 0
        # Manager's own lines are tagged and returned.
        assert all(ln.startswith("[manager] ") for ln in d["lines"])
        assert any("gateway routed request" in ln for ln in d["lines"])

        r = await c.get("/_monitor/api/logs", params={"q": "gateway"})
        d = r.json()
        assert d["returned"] == 1
        assert "gateway" in d["lines"][0]
    buf.clear()

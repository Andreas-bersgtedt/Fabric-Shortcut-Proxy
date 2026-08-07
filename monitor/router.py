"""
Monitoring dashboard SPA + API (optional; mounted only when ENABLE_MONITOR).

Routes (all under ``/_monitor``):
  GET /_monitor/             -> the single-page dashboard
  GET /_monitor/api/summary  -> consolidated live stats (JSON, polled by the SPA)
  POST /_monitor/api/reset   -> clear trace + query-lag buffers before a fresh run

Read-only. Combines:
  - per-table read/request stats + Fabric-side gaps       (observability.trace)
  - per-table query lag: Fabric -> SQL -> Parquet -> out   (observability.querystats)
  - current snapshot/version per table                     (iceberg.state_store)
  - cache occupancy incl. pinned splits                    (cache.lru_cache)
  - process/SQL metrics                                    (observability.metrics)
"""
from __future__ import annotations

import pathlib
import time

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

import config
import cache.lru_cache as cache
from db.capabilities import capabilities_for_db_url
from iceberg.state_store import get_all_snapshots
from observability import metrics, trace, querystats
from observability.logbuffer import get_buffer
from observability.logging import get_logger

log = get_logger(__name__)

router = APIRouter(prefix="/_monitor")
_HTML_PATH = pathlib.Path(__file__).parent / "index.html"


@router.get("")
@router.get("/")
async def index() -> HTMLResponse:
    return HTMLResponse(_HTML_PATH.read_text(encoding="utf-8"))


def _counter_total(snapshot: dict, name: str) -> float:
    return sum(s["value"] for s in snapshot.get("counters", {}).get(name, []))


@router.get("/api/summary")
async def summary() -> dict:
    """Consolidated live snapshot for the dashboard."""
    m = metrics.snapshot()
    tl = trace.timeline()["tables"]
    qs = querystats.summary(recent_limit=60)
    qtables = qs["tables"]

    # Current snapshot/version per table (latest version wins).
    snap_by_table: dict[str, dict] = {}
    for snap in get_all_snapshots():
        name = snap.table.name
        cur = snap_by_table.get(name)
        if cur is None or getattr(snap, "version", 1) >= cur["version"]:
            snap_by_table[name] = {
                "version": getattr(snap, "version", 1),
                "snapshot_id": snap.snapshot_id,
                "splits": len(snap.splits),
                "total_records": snap.total_records,
                "last_modified_ms": snap.watermark_ms,
                "connection": getattr(snap.table, "connection_id", "default"),
            }

    names = set(snap_by_table) | set(tl) | set(qtables)
    names.discard("-")  # bucket-level / non-table traffic
    # Only surface real virtual tables (those with a snapshot). Everything else
    # in the trace/query maps is Fabric probe noise (schema.json.gz, _delta_log
    # at the db root, etc.) and shouldn't appear as a table row.
    names &= set(snap_by_table)

    source_meta: dict[str, dict] = {}

    def _source_for(connection_id: str) -> dict:
        meta = source_meta.get(connection_id)
        if meta is None:
            caps = capabilities_for_db_url(config.effective_db_url(connection_id))
            meta = {"flavor": caps.flavor, "execution_mode": caps.to_dict()["execution_mode"]}
            source_meta[connection_id] = meta
        return meta

    tables = []
    for name in sorted(names):
        t = tl.get(name, {})
        bk = t.get("by_kind", {}) if t else {}
        q = qtables.get(name, {})
        s = snap_by_table.get(name, {})
        conn_id = s.get("connection", "default")
        src = _source_for(conn_id)
        tables.append({
            "table": name,
            "version": s.get("version"),
            "snapshot_id": s.get("snapshot_id"),
            "connection": conn_id,
            "flavor": src["flavor"],
            "execution_mode": src["execution_mode"],
            "splits": s.get("splits"),
            "total_records": s.get("total_records"),
            # request mix (from the trace)
            "requests": t.get("requests", 0),
            "errors": t.get("errors", 0),
            "probe_404s": t.get("probe_404s", 0),
            "metadata_reads": bk.get("metadata", {}).get("count", 0),
            "version_hint_reads": bk.get("version_hint", {}).get("count", 0),
            "manifest_reads": bk.get("manifest", {}).get("count", 0),
            "delta_log_reads": bk.get("delta_log", {}).get("count", 0),
            "data_reads": bk.get("data", {}).get("count", 0),
            "list_reads": bk.get("list", {}).get("count", 0),
            "fabric_gap_ms_total": t.get("fabric_gap_ms_total", 0),
            "span_seconds": t.get("span_seconds", 0),
            # query lag (from querystats)
            "data_requests": q.get("data_requests", 0),
            "cache_hit_ratio": q.get("cache_hit_ratio"),
            "rows_served": q.get("rows_served", 0),
            "bytes_served": q.get("bytes_served", 0),
            "avg_sql_ms": q.get("avg_sql_ms", 0),
            "p95_sql_ms": q.get("p95_sql_ms", 0),
            "avg_gen_ms": q.get("avg_gen_ms", 0),
            "avg_total_ms": q.get("avg_total_ms", 0),
            "p50_total_ms": q.get("p50_total_ms", 0),
            "p95_total_ms": q.get("p95_total_ms", 0),
            "max_total_ms": q.get("max_total_ms", 0),
            "last_read_ts": q.get("last_ts"),
        })

    return {
        "generated_at": round(time.time(), 3),
        "uptime_seconds": m.get("uptime_seconds"),
        "table_format": config.TABLE_FORMAT,
        "totals": {
            "tables": len(tables),
            "connections": len({t["connection"] for t in tables}),
            "cache_hit_ratio": m.get("cache_hit_ratio"),
            "parquet_generations": _counter_total(m, "parquet_generations_total"),
            "bytes_served": _counter_total(m, "s3_bytes_served_total"),
            "source_unavailable": _counter_total(m, "source_unavailable_total"),
            "data_requests": sum(q.get("data_requests", 0) for q in qtables.values()),
        },
        "refresh": {
            "auto_refresh": config.AUTO_REFRESH,
            "strategy": config.REFRESH_STRATEGY,
            "poll_seconds": config.REFRESH_POLL_SECONDS,
        },
        "cache": cache.stats(),
        "sql_latency": m.get("sql_latency"),
        "tables": tables,
        "sources": [{"connection": cid, **meta} for cid, meta in sorted(source_meta.items())],
        "recent_queries": qs["recent"],
    }


@router.post("/api/reset")
async def reset() -> dict:
    """Clear the trace + query-lag buffers (call before a fresh Fabric run)."""
    trace.reset()
    querystats.reset()
    return {"status": "cleared"}


@router.get("/api/logs")
async def logs(limit: int = 1000, q: str | None = None) -> dict:
    """Tail of the rolling in-memory log buffer, optionally filtered by ``q``.

    Read-only. Returns at most ``buffer.maxlen`` (1000) lines, oldest first.
    ``q`` is a case-insensitive substring filter applied server-side.
    """
    buf = get_buffer()
    limit = max(1, min(limit, buf.maxlen))
    query = (q or "").strip() or None
    lines = buf.tail(limit=limit, query=query)
    return {
        "lines": lines,
        "returned": len(lines),
        "total": len(buf),
        "capacity": buf.maxlen,
        "query": query or "",
        "generated_at": round(time.time(), 3),
    }

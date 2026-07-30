"""
Fleet monitor aggregation.

Merges the per-Agent ``/_monitor/api/summary`` payloads into one fleet-wide view
for the Manager's operator console. Agents each see only the requests the gateway
routed to them, so the Manager must combine them:

  * count-like fields are **summed** (requests, reads, bytes, data_requests, ...);
  * latency/ratio fields are **weighted** by each Agent's ``data_requests``;
  * static per-table fields (version, splits, total_records, connection) take the
    representative value (they are identical across Agents);
  * ``recent_queries`` are concatenated and capped.

Pure + deterministic so it is unit-testable without a live fleet.
"""
from __future__ import annotations

import time

# Per-table fields that are cumulative counts and should be summed across Agents.
_TABLE_SUM = (
    "requests", "errors", "probe_404s",
    "metadata_reads", "version_hint_reads", "manifest_reads", "delta_log_reads",
    "data_reads", "list_reads", "data_requests", "rows_served", "bytes_served",
    "fabric_gap_ms_total",
)
# Per-table latency/ratio fields weighted by the Agent's data_requests.
_TABLE_WAVG = ("avg_sql_ms", "avg_gen_ms", "avg_total_ms", "cache_hit_ratio")
# Per-table fields where the fleet value is the max across Agents.
_TABLE_MAX = ("p95_sql_ms", "p50_total_ms", "p95_total_ms", "max_total_ms", "span_seconds")


def _num(v) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _wavg(pairs: list[tuple]) -> float:
    """Weighted average of ``(value, weight)``; falls back to a plain mean."""
    tw = sum(w for _, w in pairs if w)
    if tw > 0:
        return sum(_num(v) * w for v, w in pairs if w) / tw
    vals = [_num(v) for v, _ in pairs if v is not None]
    return sum(vals) / len(vals) if vals else 0.0


def _merge_cache(caches: list[dict]) -> dict:
    """Recursively sum numeric leaves across Agent cache stats."""
    out: dict = {}
    for c in caches:
        if not isinstance(c, dict):
            continue
        for k, v in c.items():
            if isinstance(v, dict):
                out[k] = _merge_cache([out.get(k, {}), v])
            elif isinstance(v, bool):
                out[k] = out.get(k, v)
            elif isinstance(v, (int, float)):
                out[k] = _num(out.get(k, 0)) + _num(v)
            else:
                out.setdefault(k, v)
    return out


def merge_summaries(summaries: list[dict]) -> dict:
    """Combine per-Agent monitor summaries into one fleet-wide summary."""
    summaries = [s for s in summaries if isinstance(s, dict)]
    if not summaries:
        return {
            "generated_at": round(time.time(), 3),
            "uptime_seconds": None, "table_format": "iceberg",
            "totals": {"tables": 0, "connections": 0, "cache_hit_ratio": None,
                       "parquet_generations": 0, "bytes_served": 0,
                       "source_unavailable": 0, "data_requests": 0},
            "refresh": {}, "cache": {}, "sql_latency": None,
            "tables": [], "recent_queries": [], "agents": 0,
        }

    # ---- per-table merge -------------------------------------------------
    by_name: dict[str, list[dict]] = {}
    for s in summaries:
        for t in s.get("tables", []) or []:
            by_name.setdefault(t.get("table"), []).append(t)

    tables = []
    for name in sorted(n for n in by_name if n):
        rows = by_name[name]
        weights = [(r, _num(r.get("data_requests"))) for r in rows]
        merged = {"table": name}
        for f in _TABLE_SUM:
            merged[f] = sum(_num(r.get(f)) for r in rows)
        for f in _TABLE_WAVG:
            merged[f] = _wavg([(r.get(f), w) for r, w in weights])
        for f in _TABLE_MAX:
            merged[f] = max((_num(r.get(f)) for r in rows), default=0)
        # Static fields — identical across Agents; take a representative value.
        for f in ("version", "snapshot_id", "splits", "total_records"):
            vals = [r.get(f) for r in rows if r.get(f) is not None]
            merged[f] = max(vals) if vals else None
        conns = [r.get("connection") for r in rows if r.get("connection")]
        merged["connection"] = conns[0] if conns else "default"
        last = [r.get("last_read_ts") for r in rows if r.get("last_read_ts")]
        merged["last_read_ts"] = max(last) if last else None
        # cache_hit_ratio has no meaning with zero data_requests.
        if merged.get("data_requests", 0) <= 0:
            merged["cache_hit_ratio"] = None
        tables.append(merged)

    # ---- totals ----------------------------------------------------------
    tot = [s.get("totals", {}) or {} for s in summaries]
    total_dr = sum(_num(t.get("data_requests")) for t in tot)
    totals = {
        "tables": len(tables),
        "connections": len({t.get("connection") for t in tables if t.get("connection")}),
        "cache_hit_ratio": _wavg([(t.get("cache_hit_ratio"), _num(t.get("data_requests"))) for t in tot])
                           if total_dr > 0 else None,
        "parquet_generations": sum(_num(t.get("parquet_generations")) for t in tot),
        "bytes_served": sum(_num(t.get("bytes_served")) for t in tot),
        "source_unavailable": sum(_num(t.get("source_unavailable")) for t in tot),
        "data_requests": total_dr,
    }

    # ---- sql_latency (weighted by count) ---------------------------------
    lats = [s.get("sql_latency") for s in summaries if isinstance(s.get("sql_latency"), dict)]
    sql_latency = None
    if lats:
        cnt = sum(_num(l.get("count")) for l in lats)
        sql_latency = {
            "count": cnt,
            "avg_seconds": _wavg([(l.get("avg_seconds"), _num(l.get("count"))) for l in lats]),
            "max_seconds": max((_num(l.get("max_seconds")) for l in lats), default=0),
        }

    # ---- recent queries (newest first, capped) ---------------------------
    recent: list = []
    for s in summaries:
        recent.extend(s.get("recent_queries", []) or [])
    recent.sort(key=lambda q: (q or {}).get("ts", 0) if isinstance(q, dict) else 0, reverse=True)

    first = summaries[0]
    return {
        "generated_at": round(time.time(), 3),
        "uptime_seconds": max((_num(s.get("uptime_seconds")) for s in summaries), default=0),
        "table_format": first.get("table_format", "iceberg"),
        "totals": totals,
        "refresh": first.get("refresh", {}),
        "cache": _merge_cache([s.get("cache", {}) for s in summaries]),
        "sql_latency": sql_latency,
        "tables": tables,
        "recent_queries": recent[:60],
        "agents": len(summaries),
    }

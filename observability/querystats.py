"""
Per-table query-lag instrumentation (Fabric -> SQL -> Parquet -> Fabric).

The request trace (observability/trace.py) captures every S3 request's total
latency. This module adds the *breakdown* for data (Parquet) requests that the
trace can't see: how long the source SQL took vs. how long Parquet generation
took vs. whether the split was served from cache. That's the "query lag" a
Fabric read incurs: request in -> SQL pushdown -> Parquet build -> bytes out.

In-memory, O(1) per record, thread-safe. Never raises.
"""
from __future__ import annotations

import threading
import time
from collections import deque

import config

_lock = threading.Lock()
_recent: deque[dict] = deque(maxlen=max(200, config.TRACE_BUFFER_SIZE))
_by_table: dict[str, dict] = {}


def _agg(table: str) -> dict:
    return _by_table.setdefault(table, {
        "data_requests": 0, "cache_hits": 0, "rows": 0, "bytes": 0,
        "sql_ms_sum": 0.0, "sql_ms_max": 0.0,
        "gen_ms_sum": 0.0, "gen_ms_max": 0.0,
        "total_ms_sum": 0.0, "total_ms_max": 0.0,
        "last_ts": None,
    })


def record_query(*, table: str, split_index: int, sql_ms: float, gen_ms: float,
                 total_ms: float, rows: int | None, resp_bytes: int,
                 cache_hit: bool) -> None:
    """Record one data-object serve (cache hit or fresh generation)."""
    try:
        now = time.time()
        with _lock:
            a = _agg(table)
            a["data_requests"] += 1
            if cache_hit:
                a["cache_hits"] += 1
            if rows:
                a["rows"] += rows
            a["bytes"] += resp_bytes
            a["sql_ms_sum"] += sql_ms
            a["sql_ms_max"] = max(a["sql_ms_max"], sql_ms)
            a["gen_ms_sum"] += gen_ms
            a["gen_ms_max"] = max(a["gen_ms_max"], gen_ms)
            a["total_ms_sum"] += total_ms
            a["total_ms_max"] = max(a["total_ms_max"], total_ms)
            a["last_ts"] = now
            _recent.append({
                "ts": round(now, 3), "table": table, "split": split_index,
                "cache_hit": cache_hit, "rows": rows, "bytes": resp_bytes,
                "sql_ms": round(sql_ms, 1), "gen_ms": round(gen_ms, 1),
                "total_ms": round(total_ms, 1),
            })
    except Exception:  # instrumentation must never break a request
        pass


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    k = max(0, min(len(s) - 1, int(round((pct / 100.0) * (len(s) - 1)))))
    return round(s[k], 1)


def summary(recent_limit: int = 50) -> dict:
    """Per-table aggregates + a windowed p50/p95 of query lag, plus the most
    recent individual queries (newest first)."""
    with _lock:
        recent = list(_recent)
        tables: dict[str, dict] = {}
        for t, a in _by_table.items():
            n = a["data_requests"] or 1
            win = [r["total_ms"] for r in recent if r["table"] == t]
            sqlwin = [r["sql_ms"] for r in recent if r["table"] == t]
            tables[t] = {
                "data_requests": a["data_requests"],
                "cache_hits": a["cache_hits"],
                "cache_hit_ratio": (round(a["cache_hits"] / a["data_requests"], 3)
                                    if a["data_requests"] else None),
                "rows_served": a["rows"],
                "bytes_served": a["bytes"],
                "avg_sql_ms": round(a["sql_ms_sum"] / n, 1),
                "max_sql_ms": round(a["sql_ms_max"], 1),
                "avg_gen_ms": round(a["gen_ms_sum"] / n, 1),
                "max_gen_ms": round(a["gen_ms_max"], 1),
                "avg_total_ms": round(a["total_ms_sum"] / n, 1),
                "max_total_ms": round(a["total_ms_max"], 1),
                "p50_total_ms": _percentile(win, 50),
                "p95_total_ms": _percentile(win, 95),
                "p95_sql_ms": _percentile(sqlwin, 95),
                "last_ts": a["last_ts"],
            }
        recent_out = recent[-recent_limit:][::-1]
    return {"tables": tables, "recent": recent_out}


def reset() -> None:
    with _lock:
        _recent.clear()
        _by_table.clear()

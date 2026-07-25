"""
In-process metrics registry (Plan item H1).

Dependency-free (stdlib only) Prometheus-style counters plus a latency summary,
and a JSON snapshot used by ``/_admin/stats``. Safe for concurrent access from
uvicorn worker threads.

Metrics exposed:
  - ``s3_requests_total{op,kind}``     S3 requests by operation + object kind
  - ``s3_bytes_served_total``          total object bytes returned to clients
  - ``cache_events_total{cache,result}`` cache hit/miss by cache
  - ``sql_errors_total``               failed/timed-out SQL attempts
  - ``sql_query_duration_seconds``     SQL latency histogram (+ sum/count)
  - ``process_uptime_seconds``         process uptime gauge
"""
from __future__ import annotations

import threading
import time

_START = time.time()
_lock = threading.Lock()

# name -> { label-tuple -> value }
_counters: dict[str, dict[tuple[tuple[str, str], ...], float]] = {}

# SQL latency histogram state
_SQL_BUCKETS: tuple[float, ...] = (
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
)
_sql_count: int = 0
_sql_sum: float = 0.0
_sql_bucket_counts: dict[float, int] = {b: 0 for b in _SQL_BUCKETS}


def _label_key(labels: dict[str, str]) -> tuple[tuple[str, str], ...]:
    return tuple(sorted(labels.items()))


def inc_counter(name: str, value: float = 1.0, **labels: str) -> None:
    """Increment a labelled counter."""
    key = _label_key(labels)
    with _lock:
        series = _counters.setdefault(name, {})
        series[key] = series.get(key, 0.0) + value


# ---------------------------------------------------------------------------
# High-level helpers used across the codebase
# ---------------------------------------------------------------------------

def record_s3_request(op: str, kind: str = "-") -> None:
    inc_counter("s3_requests_total", op=op, kind=kind)


def record_bytes_served(n: int) -> None:
    if n:
        inc_counter("s3_bytes_served_total", float(n))


def record_cache(cache: str, hit: bool) -> None:
    inc_counter("cache_events_total", cache=cache, result="hit" if hit else "miss")


def record_sql(latency_seconds: float, *, error: bool = False) -> None:
    global _sql_count, _sql_sum
    with _lock:
        _sql_count += 1
        _sql_sum += latency_seconds
        for b in _SQL_BUCKETS:
            if latency_seconds <= b:
                _sql_bucket_counts[b] += 1
    if error:
        inc_counter("sql_errors_total")


def classify_key(key: str) -> str:
    """Classify an object key into a coarse metric ``kind`` label."""
    if key.endswith(".metadata.json"):
        return "metadata"
    if key.endswith(".avro"):
        return "manifest"
    if key.endswith(".parquet"):
        return "data"
    if key.endswith("version-hint.text"):
        return "version_hint"
    return "other"


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def _cache_hit_ratio() -> float | None:
    """Overall cache hit ratio across all caches, or None if no lookups yet."""
    series = _counters.get("cache_events_total", {})
    hits = sum(v for lk, v in series.items() if dict(lk).get("result") == "hit")
    total = sum(series.values())
    return round(hits / total, 4) if total else None


def snapshot() -> dict:
    """Return a JSON-serializable snapshot of all metrics."""
    with _lock:
        counters = {
            name: [{"labels": dict(lk), "value": v} for lk, v in series.items()]
            for name, series in _counters.items()
        }
        sql = {
            "count": _sql_count,
            "sum_seconds": round(_sql_sum, 6),
            "avg_seconds": round(_sql_sum / _sql_count, 6) if _sql_count else 0.0,
            "buckets_le": {str(b): c for b, c in _sql_bucket_counts.items()},
        }
        hit_ratio = _cache_hit_ratio()
    return {
        "uptime_seconds": round(time.time() - _START, 3),
        "cache_hit_ratio": hit_ratio,
        "sql_latency": sql,
        "counters": counters,
    }


def render_prometheus() -> str:
    """Render all metrics in Prometheus text exposition format (v0.0.4)."""
    lines: list[str] = []
    with _lock:
        for name, series in _counters.items():
            lines.append(f"# TYPE {name} counter")
            for lk, value in series.items():
                if lk:
                    labels = ",".join(f'{k}="{v}"' for k, v in lk)
                    lines.append(f"{name}{{{labels}}} {value}")
                else:
                    lines.append(f"{name} {value}")

        lines.append("# TYPE sql_query_duration_seconds histogram")
        for b in _SQL_BUCKETS:
            lines.append(f'sql_query_duration_seconds_bucket{{le="{b}"}} {_sql_bucket_counts[b]}')
        lines.append(f'sql_query_duration_seconds_bucket{{le="+Inf"}} {_sql_count}')
        lines.append(f"sql_query_duration_seconds_sum {round(_sql_sum, 6)}")
        lines.append(f"sql_query_duration_seconds_count {_sql_count}")

    lines.append("# TYPE process_uptime_seconds gauge")
    lines.append(f"process_uptime_seconds {round(time.time() - _START, 3)}")
    return "\n".join(lines) + "\n"


def reset() -> None:
    """Clear all metrics (test helper)."""
    global _sql_count, _sql_sum
    with _lock:
        _counters.clear()
        _sql_count = 0
        _sql_sum = 0.0
        for b in _SQL_BUCKETS:
            _sql_bucket_counts[b] = 0

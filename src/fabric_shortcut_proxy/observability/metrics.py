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
  - ``source_read_point_events_total`` read-point lifecycle events
  - ``process_uptime_seconds``         process uptime gauge
"""
from __future__ import annotations

import threading
import time
from typing import TypedDict

_START = time.time()
_lock = threading.Lock()

# name -> { label-tuple -> value }
_counters: dict[str, dict[tuple[tuple[str, str], ...], float]] = {}
_gauges: dict[str, dict[tuple[tuple[str, str], ...], float]] = {}

# SQL latency histogram state
_SQL_BUCKETS: tuple[float, ...] = (
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
)
_sql_count: int = 0
_sql_sum: float = 0.0
_sql_bucket_counts: dict[float, int] = {b: 0 for b in _SQL_BUCKETS}


class _SourceSqlSeries(TypedDict):
    count: int
    sum: float
    errors: int
    buckets: dict[float, int]


_source_sql: dict[str, _SourceSqlSeries] = {}


class DatasetFreshnessSample(TypedDict):
    dataset_id: str
    location: str
    produced_at_ms: int | None
    age_seconds: float | None
    target_seconds: float
    stale: bool


def record_dataset_freshness(samples: list[DatasetFreshnessSample]) -> None:
    gauges: dict[str, dict[tuple[tuple[str, str], ...], float]] = {
        "fsp_dataset_age_seconds": {},
        "fsp_dataset_freshness_target_seconds": {},
        "fsp_dataset_stale": {},
        "fsp_dataset_production_timestamp_known": {},
    }
    for sample in samples:
        labels = _label_key({
            "dataset_id": sample["dataset_id"],
            "location": sample["location"],
        })
        age = sample["age_seconds"]
        if age is not None:
            gauges["fsp_dataset_age_seconds"][labels] = age
        gauges["fsp_dataset_freshness_target_seconds"][labels] = sample["target_seconds"]
        gauges["fsp_dataset_stale"][labels] = float(sample["stale"])
        gauges["fsp_dataset_production_timestamp_known"][labels] = float(age is not None)
    with _lock:
        _gauges.update(gauges)


def _label_key(labels: dict[str, str]) -> tuple[tuple[str, str], ...]:
    return tuple(sorted(labels.items()))


def inc_counter(name: str, value: float = 1.0, **labels: str) -> None:
    """Increment a labelled counter."""
    key = _label_key(labels)
    with _lock:
        series = _counters.setdefault(name, {})
        series[key] = series.get(key, 0.0) + value


def set_gauge(name: str, value: float, **labels: str) -> None:
    """Set a labelled gauge."""
    with _lock:
        _gauges.setdefault(name, {})[_label_key(labels)] = value


def set_location_gauges(name: str, values: dict[str, float]) -> None:
    """Replace all location series for a gauge, dropping locations no longer present."""
    with _lock:
        _gauges[name] = {
            _label_key({"location": location or "unknown"}): value
            for location, value in values.items()
        }


def _agent_location() -> str:
    from fabric_shortcut_proxy import config

    return str(getattr(config, "AGENT_LOCATION", "") or "unknown")


def record_assignment_rejection(location: str, reason: str) -> None:
    inc_counter(
        "fsp_assignment_rejections_total",
        location=location or "unassigned",
        reason=reason or "unspecified",
    )


def record_agent_heartbeat_ages(
    ages_by_agent: dict[tuple[str, str], float],
    timeouts_by_agent: dict[tuple[str, str], float] | None = None,
) -> None:
    with _lock:
        _gauges["fsp_agent_heartbeat_age_seconds"] = {
            _label_key(
                {"agent_id": agent_id, "location": location or "unknown"}
            ): age
            for (location, agent_id), age in ages_by_agent.items()
        }
        if timeouts_by_agent is not None:
            _gauges["fsp_agent_heartbeat_timeout_seconds"] = {
                _label_key({"agent_id": agent_id, "location": location or "unknown"}): timeout
                for (location, agent_id), timeout in timeouts_by_agent.items()
            }


def record_artifact_upload(size_bytes: int, duration_seconds: float) -> None:
    if size_bytes <= 0:
        return
    inc_counter(
        "fsp_artifact_upload_bytes_total",
        float(size_bytes),
        location=_agent_location(),
    )
    if duration_seconds > 0:
        inc_counter(
            "fsp_artifact_upload_duration_seconds_total",
            duration_seconds,
            location=_agent_location(),
        )
        inc_counter(
            "fsp_artifact_uploads_total",
            location=_agent_location(),
        )


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
        location = _agent_location()
        sample = _source_sql.setdefault(
            location,
            {
                "count": 0,
                "sum": 0.0,
                "errors": 0,
                "buckets": {bucket: 0 for bucket in _SQL_BUCKETS},
            },
        )
        sample["count"] += 1
        sample["sum"] += latency_seconds
        sample["errors"] += int(error)
        buckets = sample["buckets"]
        for bucket in _SQL_BUCKETS:
            if latency_seconds <= bucket:
                buckets[bucket] += 1
    if error:
        inc_counter("sql_errors_total")


def record_read_point(event: str, provider: str) -> None:
    inc_counter(
        "source_read_point_events_total",
        event=event,
        provider=provider or "unknown",
    )


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
        gauges = {
            name: [{"labels": dict(lk), "value": v} for lk, v in series.items()]
            for name, series in _gauges.items()
        }
        sql = {
            "count": _sql_count,
            "sum_seconds": round(_sql_sum, 6),
            "avg_seconds": round(_sql_sum / _sql_count, 6) if _sql_count else 0.0,
            "buckets_le": {str(b): c for b, c in _sql_bucket_counts.items()},
        }
        source_sql = {
            location: {
                "count": sample["count"],
                "sum_seconds": round(sample["sum"], 6),
                "errors": sample["errors"],
                "buckets_le": {
                    str(bucket): count
                    for bucket, count in sample["buckets"].items()
                },
            }
            for location, sample in _source_sql.items()
        }
        hit_ratio = _cache_hit_ratio()
    return {
        "uptime_seconds": round(time.time() - _START, 3),
        "cache_hit_ratio": hit_ratio,
        "sql_latency": sql,
        "counters": counters,
        "gauges": gauges,
        "source_sql_latency": source_sql,
    }


def _escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def render_prometheus() -> str:
    """Render all metrics in Prometheus text exposition format (v0.0.4)."""
    lines: list[str] = []
    with _lock:
        for name, series in _counters.items():
            lines.append(f"# TYPE {name} counter")
            for lk, value in series.items():
                if lk:
                    labels = ",".join(
                        f'{k}="{_escape_label(v)}"' for k, v in lk
                    )
                    lines.append(f"{name}{{{labels}}} {value}")
                else:
                    lines.append(f"{name} {value}")

        for name, series in _gauges.items():
            lines.append(f"# TYPE {name} gauge")
            for lk, value in series.items():
                labels = ",".join(
                    f'{k}="{_escape_label(v)}"' for k, v in lk
                )
                lines.append(f"{name}{{{labels}}} {value}")

        lines.append("# TYPE sql_query_duration_seconds histogram")
        for b in _SQL_BUCKETS:
            lines.append(f'sql_query_duration_seconds_bucket{{le="{b}"}} {_sql_bucket_counts[b]}')
        lines.append(f'sql_query_duration_seconds_bucket{{le="+Inf"}} {_sql_count}')
        lines.append(f"sql_query_duration_seconds_sum {round(_sql_sum, 6)}")
        lines.append(f"sql_query_duration_seconds_count {_sql_count}")

        lines.append("# TYPE fsp_source_query_duration_seconds histogram")
        for location, sample in sorted(_source_sql.items()):
            buckets = sample["buckets"]
            for bucket in _SQL_BUCKETS:
                lines.append(
                    "fsp_source_query_duration_seconds_bucket"
                    f'{{location="{_escape_label(location)}",le="{bucket}"}} '
                    f"{buckets[bucket]}"
                )
            lines.append(
                "fsp_source_query_duration_seconds_bucket"
                f'{{location="{_escape_label(location)}",le="+Inf"}} '
                f"{sample['count']}"
            )
            lines.append(
                "fsp_source_query_duration_seconds_sum"
                f'{{location="{_escape_label(location)}"}} {round(sample["sum"], 6)}'
            )
            lines.append(
                "fsp_source_query_duration_seconds_count"
                f'{{location="{_escape_label(location)}"}} {sample["count"]}'
            )

    lines.append("# TYPE process_uptime_seconds gauge")
    lines.append(f"process_uptime_seconds {round(time.time() - _START, 3)}")
    return "\n".join(lines) + "\n"


def reset() -> None:
    """Clear all metrics (test helper)."""
    global _sql_count, _sql_sum
    with _lock:
        _counters.clear()
        _gauges.clear()
        _source_sql.clear()
        _sql_count = 0
        _sql_sum = 0.0
        for b in _SQL_BUCKETS:
            _sql_bucket_counts[b] = 0

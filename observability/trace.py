"""
Request tracing — capture the Fabric read / Iceberg->Delta conversion timeline.

Fabric drives the proxy with a long sequence of S3 requests (list -> version-hint
-> metadata.json -> manifest list -> manifest file -> ranged parquet reads), often
spread over minutes with multi-second *gaps* where Fabric is doing its own work.
This module records each request in a bounded ring buffer so you can reconstruct
that timeline per table: our per-request latency, the inter-request gaps
(Fabric-side think time), 404s (missing blobs), and likely parquet regenerations.

Everything here is in-memory, O(1) per record, and safe for concurrent access.
"""
from __future__ import annotations

import threading
import time
from collections import deque

import config

_lock = threading.Lock()
_buf: deque[dict] = deque(maxlen=config.TRACE_BUFFER_SIZE)
# Per-table timestamp (seconds) of the previous request, to compute gaps.
_last_ts: dict[str, float] = {}
_seq = 0

# A 4xx on one of these object kinds is a genuine error; a 4xx on anything else
# (folder/list/other probes like _delta_log/_last_checkpoint, schema.json.gz,
# trailing slashes) is an EXPECTED S3 probe from Fabric, not a failure.
_REAL_ERROR_KINDS = frozenset({"metadata", "manifest", "data", "version_hint", "delta_log"})


def table_from_key(key: str) -> str:
    """Derive the virtual table name from an object key.

    Keys look like ``warehouse/db/<table>/(data|metadata)/...`` — return
    ``<table>`` (the segment after the warehouse prefix), or ``-`` if it doesn't
    match (bucket-level listings, service probes, etc.).
    """
    prefix = config.WAREHOUSE_PREFIX.rstrip("/") + "/"
    if key.startswith(prefix):
        rest = key[len(prefix):]
        seg = rest.split("/", 1)[0]
        if seg:
            return seg
    return "-"


def classify(key: str) -> str:
    """Coarse object kind for grouping the timeline."""
    if key.endswith(".metadata.json"):
        return "metadata"
    if key.endswith("version-hint.text"):
        return "version_hint"
    if key.endswith(".avro"):
        return "manifest"
    if key.endswith(".parquet"):
        return "data"
    # Native Delta transaction-log commit (NNNNNNNNNNNNNNNNNNNN.json). The
    # _last_checkpoint probe has no .json suffix, so it stays a benign "other".
    if "/_delta_log/" in key and key.endswith(".json"):
        return "delta_log"
    if key == "" or key.endswith("/"):
        return "list"
    return "other"


def record(
    *,
    method: str,
    key: str,
    status: int,
    duration_ms: float,
    resp_bytes: int = 0,
    range_header: str | None = None,
    user_agent: str | None = None,
) -> None:
    """Append one request record. Never raises."""
    if not config.REQUEST_TRACE:
        return
    try:
        global _seq
        table = table_from_key(key)
        kind = classify(key)
        now = time.time()
        with _lock:
            _seq += 1
            prev = _last_ts.get(table)
            gap_ms = round((now - prev) * 1000, 1) if prev is not None else None
            _last_ts[table] = now
            _buf.append({
                "seq": _seq,
                "ts": round(now, 3),
                "table": table,
                "kind": kind,
                "method": method,
                "key": key,
                "status": status,
                "duration_ms": round(duration_ms, 1),
                "gap_ms": gap_ms,
                "bytes": resp_bytes,
                "range": range_header,
                "ua": (user_agent or "")[:80] or None,
            })
    except Exception:  # tracing must never break a request
        pass


def recent(*, table: str | None = None, kind: str | None = None,
           status: int | None = None, limit: int = 200) -> list[dict]:
    """Return the most recent records (newest first) matching the filters."""
    with _lock:
        items = list(_buf)
    if table:
        items = [r for r in items if r["table"] == table]
    if kind:
        items = [r for r in items if r["kind"] == kind]
    if status is not None:
        items = [r for r in items if r["status"] == status]
    items.reverse()
    return items[:limit]


def timeline(table: str | None = None) -> dict:
    """Aggregate the trace into a per-table conversion timeline summary.

    For each table: request count, wall-clock span, total time spent *in the
    proxy* vs. total *gap* time (Fabric-side), a per-kind breakdown, the slowest
    requests, error count, and the biggest inter-request gaps.
    """
    with _lock:
        items = list(_buf)
    if table:
        items = [r for r in items if r["table"] == table]

    by_table: dict[str, list[dict]] = {}
    for r in items:
        by_table.setdefault(r["table"], []).append(r)

    out: dict[str, dict] = {}
    for tname, recs in by_table.items():
        recs.sort(key=lambda r: r["seq"])
        span = round(recs[-1]["ts"] - recs[0]["ts"], 2) if len(recs) > 1 else 0.0
        proxy_ms = round(sum(r["duration_ms"] for r in recs), 1)
        gap_ms = round(sum(r["gap_ms"] for r in recs if r["gap_ms"]), 1)
        kinds: dict[str, dict] = {}
        for r in recs:
            k = kinds.setdefault(r["kind"], {"count": 0, "proxy_ms": 0.0, "bytes": 0, "errors": 0})
            k["count"] += 1
            k["proxy_ms"] = round(k["proxy_ms"] + r["duration_ms"], 1)
            k["bytes"] += r["bytes"]
            if r["status"] >= 400:
                k["errors"] += 1
        # A 404 on a folder/list/other key is an EXPECTED S3 probe (Fabric checks
        # for _delta_log/, schema.json.gz, trailing-slash "folders", etc.), not a
        # real failure. Count only genuine errors: any 5xx, or a 4xx on a real
        # object (metadata/manifest/data/version-hint).
        errors = [
            {"seq": r["seq"], "key": r["key"], "status": r["status"]}
            for r in recs
            if r["status"] >= 500 or (r["status"] >= 400 and r["kind"] in _REAL_ERROR_KINDS)
        ]
        probe_404s = sum(
            1 for r in recs
            if 400 <= r["status"] < 500 and r["kind"] not in _REAL_ERROR_KINDS
        )
        slowest = sorted(recs, key=lambda r: r["duration_ms"], reverse=True)[:5]
        biggest_gaps = sorted(
            (r for r in recs if r["gap_ms"]), key=lambda r: r["gap_ms"], reverse=True
        )[:5]
        out[tname] = {
            "requests": len(recs),
            "span_seconds": span,
            "proxy_ms_total": proxy_ms,
            "fabric_gap_ms_total": gap_ms,
            "errors": len(errors),
            "probe_404s": probe_404s,
            "by_kind": kinds,
            "error_samples": errors[:10],
            "slowest": [
                {"key": r["key"], "duration_ms": r["duration_ms"],
                 "status": r["status"], "range": r["range"]}
                for r in slowest
            ],
            "biggest_gaps": [
                {"before_key": r["key"], "gap_ms": r["gap_ms"], "kind": r["kind"]}
                for r in biggest_gaps
            ],
        }
    return {"tables": out}


def reset() -> None:
    """Clear the buffer (test helper / manual reset before a run)."""
    with _lock:
        _buf.clear()
        _last_ts.clear()

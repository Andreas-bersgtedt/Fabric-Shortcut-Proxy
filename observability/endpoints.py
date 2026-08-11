"""
Operational HTTP endpoints (Plan items H1 & H2).

Kept on a dedicated router that is mounted BEFORE the S3 catch-all router so
these literal paths are not shadowed by ``/{bucket}`` / ``/{bucket}/{key}``.

Endpoints:
  - GET /healthz       liveness  (always 200 while the process is up)
  - GET /readyz        readiness (200 when snapshot built AND source DB reachable)
  - GET /metrics       Prometheus text exposition
  - GET /_admin/stats  JSON metrics + cache occupancy snapshot
"""
from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import JSONResponse, PlainTextResponse

import cache.lru_cache as cache
import config
from db.capabilities import capabilities_for_db_url
from db.executor import ping as db_ping
from iceberg.state_store import get_all_snapshots
from observability import metrics
from observability import trace
from observability.logging import get_logger
from runtime.drain import is_draining

log = get_logger(__name__)

router = APIRouter()


@router.get("/healthz")
async def healthz() -> dict:
    """Liveness: the process is running and can serve HTTP."""
    return {"status": "ok"}


@router.get("/readyz")
async def readyz() -> JSONResponse:
    """Readiness: the Iceberg snapshot is built and the source DB is reachable.

    While draining, report 503 up front so an external load balancer deregisters
    this backend before the process exits (in-flight requests still complete).
    """
    if is_draining():
        return JSONResponse(
            status_code=503,
            content={"status": "draining", "checks": {"draining": True}},
        )
    checks = {"snapshot": False, "database": False}

    try:
        checks["snapshot"] = len(get_all_snapshots()) > 0
    except Exception:  # snapshot not built yet
        checks["snapshot"] = False

    checks["database"] = await db_ping()

    caps = capabilities_for_db_url(config.DB_URL)
    ready = all(checks.values())
    content = {
        "status": "ready" if ready else "not_ready",
        "checks": checks,
        "source": {"flavor": caps.flavor, "execution_mode": caps.to_dict()["execution_mode"]},
    }
    # Object-store tokenizer (issue #12): advertise tokenizing mounts + the
    # per-format capability matrix. Best-effort; never affects readiness.
    try:
        from storage.mounts import tokenizing_mounts
        tok_mounts = tokenizing_mounts()
        if tok_mounts:
            from storage.objectstore_capabilities import capabilities_summary
            content["object_store_tokenizer"] = {
                "mounts": tok_mounts,
                "formats": capabilities_summary(),
            }
    except Exception:  # noqa: BLE001 - observability must never break readiness
        pass
    # Resilient startup: surface any quarantined tables (served set excludes them).
    try:
        from runtime import quarantine
        q = quarantine.snapshot()
        if q:
            content["quarantined_tables"] = {
                name: {"reason": entry["reason"], "attempts": entry["attempts"]}
                for name, entry in q.items()
            }
    except Exception:  # noqa: BLE001 - observability must never break readiness
        pass
    # Key Vault / Entra ID (issue #16): advisory connectivity + cache status. Only
    # shown when configured; never affects readiness (owner directive: never-fail).
    try:
        from security.keyvault import status_snapshot as _kv_status
        kv = _kv_status()
        if kv.get("enabled"):
            content["key_vault"] = kv
    except Exception:  # noqa: BLE001 - observability must never break readiness
        pass
    return JSONResponse(status_code=200 if ready else 503, content=content)


@router.get("/metrics")
async def metrics_endpoint() -> PlainTextResponse:
    """Prometheus text exposition of all in-process metrics."""
    return PlainTextResponse(
        metrics.render_prometheus(),
        media_type="text/plain; version=0.0.4; charset=utf-8",
    )


@router.get("/_admin/stats")
async def admin_stats() -> dict:
    """Human-friendly JSON snapshot of metrics plus cache occupancy."""
    data = metrics.snapshot()
    data["cache"] = cache.stats()
    return data


@router.get("/_admin/quarantine")
async def admin_quarantine() -> dict:
    """Tables quarantined at startup (unreachable/misconfigured source), with the
    reason and retry attempts. The agent serves every healthy table + mount and
    retries these in the background."""
    from runtime import quarantine
    return {"quarantined": quarantine.snapshot()}


@router.get("/_admin/trace")
async def admin_trace(
    table: str | None = None,
    kind: str | None = None,
    status: int | None = None,
    limit: int = 200,
) -> dict:
    """Recent S3 request records (newest first) — the raw Fabric request log.

    Filter by ``table`` (virtual table name), ``kind`` (metadata/manifest/data/
    version_hint/list), or HTTP ``status`` (e.g. 404 to find missing blobs).
    """
    return {"records": trace.recent(table=table, kind=kind, status=status, limit=limit)}


@router.get("/_admin/timeline")
async def admin_timeline(table: str | None = None) -> dict:
    """Per-table Fabric conversion timeline: request counts, wall-clock span,
    time spent in the proxy vs. Fabric-side gaps, per-kind breakdown, slowest
    requests, biggest gaps, and any 4xx/5xx (missing-blob) samples."""
    return trace.timeline(table)


@router.post("/_admin/trace/reset")
async def admin_trace_reset() -> dict:
    """Clear the trace buffer (call right before starting a fresh Fabric run)."""
    trace.reset()
    return {"status": "cleared"}


@router.get("/_admin/objects")
async def admin_objects(table: str | None = None) -> dict:
    """Every virtual object the proxy currently serves, with the size DECLARED in
    the manifest vs. the size currently CACHED. A mismatch (or an uncached data
    split during a long conversion) is the classic 404 BlobNotFound cause."""
    snaps = get_all_snapshots()
    out: list[dict] = []
    for snap in snaps:
        tname = snap.table.name
        if table and tname != table:
            continue
        for s in snap.splits:
            cached = cache.peek_parquet(s.object_key)
            out.append({
                "table": tname,
                "version": getattr(snap, "version", 1),
                "key": s.object_key,
                "declared_size": s.file_size_in_bytes,
                "cached_size": (len(cached) if cached is not None else None),
                "cached": cached is not None,
                "size_drift": (
                    cached is not None and s.file_size_in_bytes is not None
                    and len(cached) != s.file_size_in_bytes
                ),
            })
    return {
        "objects": out,
        "count": len(out),
        "uncached_data_files": sum(1 for o in out if not o["cached"]),
        "size_drift_files": sum(1 for o in out if o["size_drift"]),
    }


# Iceberg types the Fabric SQL analytics endpoint / XTable conversion is known to
# reject or handle poorly — surfaced per column so a failing table is obvious.
_XTABLE_RISKY_TYPES = ("time", "uuid", "fixed", "timestamp")  # timestamp = NTZ


@router.get("/_admin/schemas")
async def admin_schemas(table: str | None = None) -> dict:
    """Resolved Iceberg schema per table (field id/name/type/nullable), plus a
    ``risky_types`` flag for column types Fabric/XTable commonly rejects. Use to
    pinpoint why one table (e.g. Product) fails conversion while others succeed."""
    out: list[dict] = []
    for snap in get_all_snapshots():
        t = snap.table
        if table and t.name != table:
            continue
        fields = [
            {"id": c.field_id, "name": c.name, "type": c.iceberg_type, "nullable": c.nullable}
            for c in (t.schema or [])
        ]
        risky = [
            {"name": f["name"], "type": f["type"]}
            for f in fields
            if any(f["type"].lower().startswith(r) for r in _XTABLE_RISKY_TYPES)
        ]
        out.append({
            "table": t.name,
            "source_table": t.source_table,
            "connection": t.connection_id,
            "key_column": t.key_column,
            "num_splits": t.num_splits,
            "version": getattr(snap, "version", 1),
            "field_count": len(fields),
            "fields": fields,
            "risky_types": risky,
        })
    return {"tables": out}


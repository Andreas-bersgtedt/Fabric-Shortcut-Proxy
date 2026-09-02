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

import asyncio
import io
import pathlib
import time

import pyarrow.parquet as pq
from fastapi import APIRouter
from fastapi.responses import HTMLResponse

import cache.lru_cache as cache
import config
from db.capabilities import capabilities_for_db_url
from iceberg.state_store import get_all_snapshots
from observability import metrics, querystats, trace
from observability.logbuffer import get_buffer
from observability.logging import get_logger
from open_mirror.config import load_targets
from open_mirror.fabric_api import FabricApiError, get_mirroring_status
from open_mirror.landing_zone import open_landing_zone, table_relative_path
from open_mirror.state import load_published_tables, load_state

log = get_logger(__name__)

router = APIRouter(prefix="/_monitor")
_HTML_PATH = pathlib.Path(__file__).parent / "index.html"
_MIRROR_STATUS_CACHE: dict[str, tuple[float, dict]] = {}
_MIRROR_STATUS_CACHE_SECONDS = 15.0


def _landing_zone_rows(target, table) -> int:
    backend = open_landing_zone(target.landing_zone_root)
    root = table_relative_path(table.target_table, table.schema)
    total = 0
    pending = [root]
    while pending:
        directory = pending.pop()
        for name in backend.list_dir(directory):
            path = f"{directory}/{name}"
            if name.lower().endswith(".parquet"):
                total += _parquet_rows(backend.read_bytes(path))
            elif backend.list_dir(path):
                pending.append(path)
    return total


def _parquet_rows(payload: bytes) -> int:
    return pq.ParquetFile(io.BytesIO(payload)).metadata.num_rows


@router.get("")
@router.get("/")
async def index() -> HTMLResponse:
    return HTMLResponse(_HTML_PATH.read_text(encoding="utf-8"))


def _counter_total(snapshot: dict, name: str) -> float:
    return sum(s["value"] for s in snapshot.get("counters", {}).get(name, []))


def _object_store_tokenizer_summary() -> dict:
    """Tokenizing mounts + per-format capabilities (issue #12); {} if none."""
    try:
        from storage.mounts import tokenizing_mounts
        tok = tokenizing_mounts()
        if not tok:
            return {}
        from storage.objectstore_capabilities import capabilities_summary
        return {"mounts": tok, "formats": capabilities_summary()}
    except Exception:  # noqa: BLE001 - the dashboard must never break on this
        return {}


def _keyvault_summary() -> dict:
    """Key Vault / Entra ID status (issue #16); {} when disabled."""
    try:
        from security.keyvault import status_snapshot
        kv = status_snapshot()
        return kv if kv.get("enabled") else {}
    except Exception:  # noqa: BLE001 - the dashboard must never break on this
        return {}


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
        "object_store_tokenizer": _object_store_tokenizer_summary(),
        "key_vault": _keyvault_summary(),
    }


async def open_mirror_summary(*, include_landing_zone_count: bool = False) -> dict:
    """Return Open Mirror status and local publishing statistics."""
    state_dir = getattr(config, "OPEN_MIRROR_STATE_DIR", "./.open_mirror_state")
    targets = []
    for target in load_targets():
        tables = []
        for table in target.tables:
            state_result = load_state(state_dir, target, table)
            state = state_result.state
            landing_zone_rows = None
            if include_landing_zone_count and state_result.status == "valid":
                try:
                    landing_zone_rows = await asyncio.to_thread(
                        _landing_zone_rows, target, table
                    )
                except Exception as exc:  # noqa: BLE001 - optional live count must not break status
                    log.warning("open_mirror_landing_zone_count_failed",
                                target=target.id, table=table.name, error=str(exc))
            published_rows_total = state.published_rows_total if state else 0
            if published_rows_total == 0 and landing_zone_rows is not None:
                # Pre-metrics state files can recover their historical total from
                # the landing-zone Parquet files when an operator requests a scan.
                published_rows_total = landing_zone_rows
            last_batch_rows = state.last_batch_rows if state else 0
            last_published_at = state.last_published_at if state else None
            if state and state.committed:
                last_published_at = last_published_at or state.committed.committed_at
                if include_landing_zone_count and not last_batch_rows:
                    try:
                        backend = open_landing_zone(target.landing_zone_root)
                        last_batch_rows = _parquet_rows(
                            backend.read_bytes(state.committed.file)
                        ) if state.committed.file else 0
                    except Exception as exc:  # noqa: BLE001 - optional live count must not break status
                        log.warning("open_mirror_last_batch_count_failed",
                                    target=target.id, table=table.name, error=str(exc))
            tables.append({
                "table": table.target_table,
                "strategy": table.strategy,
                "state_status": state_result.status,
                "initialized": state.initialized if state else False,
                "pending": bool(state and state.pending),
                "committed_file": state.committed.file if state and state.committed else None,
                "published_rows_total": published_rows_total,
                "last_batch_rows": last_batch_rows,
                "last_published_at": last_published_at,
                "landing_zone_rows": landing_zone_rows,
            })
        now = time.monotonic()
        cached = _MIRROR_STATUS_CACHE.get(target.id)
        if cached and now - cached[0] < _MIRROR_STATUS_CACHE_SECONDS:
            status = cached[1]
        elif target.workspace_id and target.mirrored_database_id and target.landing_zone_root.lower().startswith(
            "https://onelake.dfs.fabric.microsoft.com/"
        ):
            try:
                payload = await asyncio.to_thread(
                    get_mirroring_status, target.workspace_id, target.mirrored_database_id
                )
                value = payload.get("status") or payload.get("mirroringStatus")
                if isinstance(value, dict):
                    value = value.get("status")
                status = {"status": str(value) if value is not None else "unknown", "error": None}
            except (FabricApiError, OSError, RuntimeError) as exc:
                status = {"status": "unavailable", "error": str(exc)}
            _MIRROR_STATUS_CACHE[target.id] = (now, status)
        else:
            status = {"status": "local", "error": None}
        published = load_published_tables(state_dir, target)
        last_published_at = max(
            (table["last_published_at"] for table in tables if table["last_published_at"]),
            default=None,
        )
        targets.append({
            "id": target.id,
            "enabled": target.enabled,
            "landing_zone": target.landing_zone_root,
            "status": status["status"],
            "status_error": status["error"],
            "self_healing": target.self_healing,
            "tables": tables,
            "published_tables": len(published),
            "published_rows": sum(
                table["published_rows_total"] for table in tables
            ),
            "last_published_at": last_published_at,
        })
    return {
        "generated_at": round(time.time(), 3),
        "state_dir": str(pathlib.Path(state_dir).resolve()),
        "targets": targets,
        "totals": {
            "targets": len(targets),
            "enabled_targets": sum(1 for target in targets if target["enabled"]),
            "tables": sum(len(target["tables"]) for target in targets),
            "initialized_tables": sum(
                1 for target in targets for table in target["tables"] if table["initialized"]
            ),
            "pending_tables": sum(
                1 for target in targets for table in target["tables"] if table["pending"]
            ),
            "published_rows": sum(target["published_rows"] for target in targets),
            "last_batch_rows": sum(
                table["last_batch_rows"]
                for target in targets for table in target["tables"]
            ),
            "last_published_at": max(
                (target["last_published_at"] for target in targets
                 if target["last_published_at"]),
                default=None,
            ),
        },
    }


async def open_mirror_cleanup(target_id: str, table_name: str | None = None, execute: bool = False) -> dict:
    """Inspect or execute retention cleanup for one Manager-owned target."""
    from open_mirror.cleanup import cleanup_target

    target = next((item for item in load_targets() if item.id == target_id), None)
    if target is None:
        raise ValueError(f"unknown Open Mirror target {target_id!r}")
    return await asyncio.to_thread(
        cleanup_target, target, table_name=table_name, execute=execute
    )


@router.get("/api/open-mirror")
async def open_mirror(include_landing_zone_count: bool = False) -> dict:
    return await open_mirror_summary(
        include_landing_zone_count=include_landing_zone_count
    )


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

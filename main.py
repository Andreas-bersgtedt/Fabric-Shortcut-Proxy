"""
FastAPI application entry point.

Startup sequence:
  1. Configure structured logging
  2. Seed demo SQLite database (if DB is SQLite and table is empty)
  3. Build initial Iceberg snapshot (state store)
  4. Mount S3 router
"""
from __future__ import annotations

import asyncio
import contextlib

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

import config
from observability.logging import configure_logging, get_logger
from iceberg.state_store import build_all_snapshots
from s3.router import router as s3_router
from s3.auth import verify_signature, SigV4Error
from observability.endpoints import router as ops_router

log = get_logger(__name__)


# Holds the running uvicorn Server (set in __main__) so an in-process control
# command (drain) can request a graceful shutdown without OS signals.
_uvicorn_server = None


def _set_uvicorn_server(server) -> None:
    global _uvicorn_server
    _uvicorn_server = server


def _request_shutdown() -> None:
    """Ask the running server to exit gracefully (used by the drain command)."""
    if _uvicorn_server is not None:
        _uvicorn_server.should_exit = True
        log.info("graceful_shutdown_requested")


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging()
    config.validate_config()

    log.info(
        "startup",
        bucket=config.BUCKET_NAME,
        tables=[t.name for t in config.TABLES],
        db_url=config.redact_db_url(config.DB_URL),
    )

    # Seed demo data if using SQLite
    if "sqlite" in config.DB_URL:
        from demo.seed_db import seed_demo_database
        await seed_demo_database()

    # Resolve each table's schema (reflected from source metadata unless declared
    # explicitly) and split key column, in place, before snapshots are built.
    from db.executor import resolve_tables
    await resolve_tables(config.TABLES)

    # Fail fast (H6) if any source table doesn't expose every declared column.
    if config.VALIDATE_SOURCE_SCHEMA:
        from db.executor import validate_source_schema
        for table in config.TABLES:
            await validate_source_schema(table)

    if config.AUTO_REFRESH:
        # Data-freshness path (content-addressed snapshots + background poller).
        # Each chunk is named by the hash of its rows, so a new snapshot (and new
        # data-file paths) is published only when content actually changes.
        from iceberg import freshness
        for t in config.TABLES:
            candidate = await freshness.materialize_table(
                t, config.BUCKET_NAME, config.WAREHOUSE_PREFIX
            )
            await freshness.publish(candidate)
            await freshness.prime_probe(t)
        freshness.start_poller(
            config.TABLES, config.BUCKET_NAME, config.WAREHOUSE_PREFIX
        )
        log.info(
            "auto_refresh_enabled",
            tables=[t.name for t in config.TABLES],
            poll_seconds=config.REFRESH_POLL_SECONDS,
            strategy=config.REFRESH_STRATEGY,
        )
        # Fabric's Iceberg->Delta conversion + SQL-endpoint sync can't keep up if
        # the source Iceberg table changes more often than ~once per 2 minutes
        # (it sees an inconsistent view and never settles). Warn if the poll
        # interval could republish faster than that.
        if config.REFRESH_POLL_SECONDS < 120:
            log.warning(
                "refresh_poll_too_fast_for_fabric",
                poll_seconds=config.REFRESH_POLL_SECONDS,
                hint="Fabric needs source Iceberg updates less frequent than once "
                     "per ~2 min; set refresh_poll_seconds >= 300 (600 recommended) "
                     "or the SQL endpoint may never converge on the latest version.",
            )
    else:
        runtime_tables = list(config.TABLES)

        # Phase 2 split planner v2 (opt-in): dynamic split-count selection from
        # row-target planning with min/max guardrails.
        if config.SPLIT_TARGET_ROWS > 0:
            from planner.split_planner import choose_table_num_splits
            for t in runtime_tables:
                t.num_splits = await choose_table_num_splits(t)

        # Build the Iceberg snapshot for every configured table (F1 — multi-table).
        snapshots = build_all_snapshots(
            runtime_tables,
            bucket=config.BUCKET_NAME,
            warehouse_prefix=config.WAREHOUSE_PREFIX,
        )
        log.info(
            "snapshots_ready",
            tables=[s.table.name for s in snapshots],
            count=len(snapshots),
        )

        # Phase 4 scale engine: range-based split planning. Assign each split a
        # contiguous key range (from the source MIN/MAX) so materialization reads
        # only its slice off the PK index instead of a full-table modulo scan.
        # Best-effort: falls back to modulo per table on empty/non-integer keys.
        if config.SPLIT_STRATEGY in ("range", "date", "auto") or config.SPLIT_TARGET_ROWS > 0:
            from planner.split_planner import plan_ranges_for_snapshot
            for snap in snapshots:
                await plan_ranges_for_snapshot(snap)

        # Eagerly materialize every split's Parquet bytes so the Iceberg manifest
        # reports ACCURATE record_count and file_size_in_bytes. Iceberg/Parquet
        # readers (and OneLake's Iceberg->Delta virtualization) rely on the declared
        # file size to locate the Parquet footer; placeholder sizes break the read.
        #
        # F5: a warm restart loads bytes from the persistent disk cache and skips SQL
        #     + Parquet regeneration entirely (keys are deterministic).
        # F4: splits are materialized concurrently (bounded by
        #     MAX_CONCURRENT_GENERATIONS) unless CONCURRENT_STARTUP_MATERIALIZATION
        #     is disabled.
        import io as _io
        import cache.lru_cache as _cache
        import pyarrow.parquet as _pq
        from planner.split_planner import build_split_query
        from db.executor import execute_split_query, stream_split_query
        from parquet.generator import rows_to_parquet, stream_rows_to_parquet
        from iceberg.stats import collect_split_stats

        _mat_sem = asyncio.Semaphore(config.MAX_CONCURRENT_GENERATIONS)

        def _owns_split(split) -> bool:
            """Phase 3: this Agent's shard owns (materializes) the split. With a
            single shard every split is owned (single-Agent / known-good path)."""
            n = config.AGENT_SHARD_COUNT
            return n <= 1 or (split.split_index % n == config.AGENT_SHARD_INDEX)

        async def _wait_for_store(key: str) -> bytes | None:
            """Poll the artifact store for a split another shard is generating.
            Runs OUTSIDE the generation semaphore so it never blocks owned work."""
            deadline = asyncio.get_event_loop().time() + config.MATERIALIZE_WAIT_SECONDS
            while asyncio.get_event_loop().time() < deadline:
                await asyncio.sleep(0.25)
                got = _cache.warm_parquet(key)
                if got is not None:
                    return got
            return None

        def _apply_warm(split, warm: bytes) -> int:
            split.file_size_in_bytes = len(warm)
            split.record_count = _pq.read_metadata(_io.BytesIO(warm)).num_rows
            if config.ICEBERG_MANIFEST_STATS:
                split.stats = collect_split_stats(warm, split.table.schema)
            if config.PIN_MATERIALIZED_SPLITS:
                _cache.pin_parquet(split.object_key, warm)
            return split.record_count

        async def _materialize(split) -> int:
            key = split.object_key
            # Fast path: already durable (disk/artifact store) -> zero regeneration.
            warm = _cache.warm_parquet(key)
            if warm is not None:
                return _apply_warm(split, warm)
            # Distributed materialization: a non-owner waits for the owning shard
            # to publish this split to the shared store (no SQL here).
            if not _owns_split(split) and config.ARTIFACT_STORE_SERVING:
                warm = await _wait_for_store(key)
                if warm is not None:
                    return _apply_warm(split, warm)
                log.warning("materialize_wait_timeout_fallback_generate",
                            split_index=split.split_index, table=split.table.name)
            # Generate (owner, or non-owner fallback) under the concurrency guard.
            async with _mat_sem:
                warm = _cache.warm_parquet(key)   # another writer may have won the race
                if warm is not None:
                    return _apply_warm(split, warm)
                sql, params = build_split_query(split)
                if config.STREAMING_PARQUET:
                    # Bounded-memory path: stream row batches straight into Parquet.
                    batches = stream_split_query(
                        sql, params, split_index=split.split_index,
                        batch_rows=config.STREAM_BATCH_ROWS,
                        connection=split.table.connection_id,
                    )
                    pq_bytes, nrows = await stream_rows_to_parquet(
                        batches, split_index=split.split_index, columns=split.table.schema
                    )
                else:
                    rows = await execute_split_query(sql, params, split_index=split.split_index,
                                                     connection=split.table.connection_id)
                    pq_bytes = rows_to_parquet(
                        rows, split_index=split.split_index, columns=split.table.schema
                    )
                    nrows = len(rows)
                if config.PIN_MATERIALIZED_SPLITS:
                    _cache.pin_parquet(split.object_key, pq_bytes)
                else:
                    _cache.put_parquet(split.object_key, pq_bytes)
                split.record_count = nrows
                split.file_size_in_bytes = len(pq_bytes)
                if config.ICEBERG_MANIFEST_STATS:
                    split.stats = collect_split_stats(pq_bytes, split.table.schema)
                return nrows

        for snap in snapshots:
            if config.CONCURRENT_STARTUP_MATERIALIZATION:
                counts = await asyncio.gather(*(_materialize(s) for s in snap.splits))
            else:
                counts = [await _materialize(s) for s in snap.splits]
            snap.total_records = sum(counts)
            log.info("splits_materialized", table=snap.table.name,
                     total_records=snap.total_records, splits=len(snap.splits))

        # Correctness guard for multi-table: Fabric's Iceberg->Delta conversion +
        # SQL-endpoint sync can run for MINUTES. If the in-memory Parquet cache
        # can't hold every materialized split for that whole window (LRU eviction
        # or TTL expiry), an evicted split is regenerated on demand — and Parquet
        # output is NOT byte-identical, so its size no longer matches the size
        # already declared in the manifest. Fabric's ranged reads then fail with
        # 404 BlobNotFound / footer-parse errors. PIN_MATERIALIZED_SPLITS (default)
        # keeps the authoritative bytes pinned so this can't happen; only warn when
        # it's disabled and there's no disk cache to give identical regeneration.
        total_data_bytes = sum(
            s.file_size_in_bytes or 0 for snap in snapshots for s in snap.splits
        )
        if config.PIN_MATERIALIZED_SPLITS:
            log.info(
                "splits_pinned",
                pinned_bytes=total_data_bytes,
                tables=len(snapshots),
            )
        if config.ARTIFACT_STORE_SERVING:
            log.info(
                "artifact_store_serving",
                backend=config.ARTIFACT_STORE_BACKEND,
                dir=config.ARTIFACT_STORE_DIR if config.ARTIFACT_STORE_BACKEND == "local" else None,
                hint="materialized splits are durable in the artifact store; a restart serves from it with zero regeneration",
            )
        elif not config.PARQUET_DISK_CACHE and total_data_bytes > config.PARQUET_CACHE_MAX_BYTES:
            log.warning(
                "parquet_cache_undersized",
                materialized_bytes=total_data_bytes,
                parquet_cache_max_bytes=config.PARQUET_CACHE_MAX_BYTES,
                parquet_cache_ttl_seconds=config.PARQUET_CACHE_TTL_SECONDS,
                hint="Fabric conversion may outlive the cache and regenerate splits "
                     "(size drift -> 404 BlobNotFound). Keep PIN_MATERIALIZED_SPLITS=1, "
                     "enable PARQUET_DISK_CACHE=1, or raise PARQUET_CACHE_MAX_BYTES / TTL.",
            )

    # Native Delta output: build the initial _delta_log commits now (before any
    # freshness pruning could drop version 1). Fabric reads this directly — no
    # Iceberg->Delta conversion layer.
    if config.TABLE_FORMAT == "delta":
        from delta import log as delta_log
        delta_log.sync_all()
        log.info("delta_format_enabled", tables=[t.name for t in config.TABLES])

    # Phase 6: publish a complete servable image (data + metadata) to the store so
    # a stateless/C++ Agent can serve every object as opaque bytes. Requires the
    # store serving tier; default off.
    if config.PUBLISH_SERVING_IMAGE and config.ARTIFACT_STORE_SERVING:
        from runtime.artifact_store import build_store
        from runtime.serving_image import publish_serving_image
        _img_store = build_store(config.ARTIFACT_STORE_BACKEND, local_dir=config.ARTIFACT_STORE_DIR)
        await asyncio.get_event_loop().run_in_executor(None, publish_serving_image, _img_store)

    # Cluster mode (Phase 1): if a Manager is configured, register + heartbeat.
    # Standalone (empty MANAGER_URL) skips this entirely — behavior unchanged.
    app.state.agent_link = None
    if config.MANAGER_URL:
        from runtime.agent_link import AgentLink
        link = AgentLink(on_drain=_request_shutdown)
        await link.start()
        app.state.agent_link = link

    # Phase 5: retention GC — one Agent (shard 0) periodically prunes orphaned
    # Parquet splits (from snapshot versions aged out of history) from the shared
    # store. Idempotent + best-effort; default off.
    app.state.gc_task = None
    if config.RETENTION_GC and config.AGENT_SHARD_INDEX == 0:
        from runtime.artifact_store import build_store
        from runtime.retention import gc_orphaned_data
        _gc_store = build_store(config.ARTIFACT_STORE_BACKEND, local_dir=config.ARTIFACT_STORE_DIR)
        app.state.gc_store = _gc_store

        async def _retention_gc_loop():
            interval = max(1.0, config.RETENTION_GC_INTERVAL_SECONDS)
            try:
                while True:
                    await asyncio.sleep(interval)
                    try:
                        await asyncio.get_event_loop().run_in_executor(
                            None, gc_orphaned_data, _gc_store)
                    except Exception:
                        log.exception("retention_gc_loop_error")
            except asyncio.CancelledError:
                raise

        app.state.gc_task = asyncio.create_task(_retention_gc_loop(), name="retention-gc")
        log.info("retention_gc_enabled", interval_seconds=config.RETENTION_GC_INTERVAL_SECONDS)

    yield  # Application runs here

    log.info("shutdown")
    # Stop the Agent control link (if any) first.
    if getattr(app.state, "agent_link", None) is not None:
        await app.state.agent_link.stop()
    # Stop the retention GC loop (if running).
    if getattr(app.state, "gc_task", None) is not None:
        app.state.gc_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await app.state.gc_task
    # Stop the freshness poller (if running) before disposing the engine.
    if config.AUTO_REFRESH:
        from iceberg import freshness
        await freshness.stop_poller()
    # Dispose DB engines cleanly (async-native and sync-fallback modes).
    from db.executor import dispose_engines
    await dispose_engines()


app = FastAPI(
    title="Fabric Shortcut Proxy (POC)",
    description="Virtual Iceberg-over-S3 proxy that serves SQL pushdown as Parquet",
    version="1.0.0",
    lifespan=lifespan,
)

# Allow CORS for any local tooling
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "HEAD", "PUT", "DELETE", "POST"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# SigV4 authentication (H3) — opt-in via REQUIRE_SIGV4. Health/metrics/admin
# endpoints and CORS preflight (OPTIONS) are exempt. /_manager and /favicon.ico
# are exempt too: the console lives on the Manager, and an Agent bounces /_manager
# there (see below) instead of rejecting it with a confusing SigV4 403.
# ---------------------------------------------------------------------------
_AUTH_EXEMPT_PREFIXES = ("/healthz", "/readyz", "/metrics", "/_admin", "/_config", "/_monitor", "/_manager", "/favicon.ico")


@app.middleware("http")
async def sigv4_auth_middleware(request, call_next):
    if config.REQUIRE_SIGV4 and request.method != "OPTIONS":
        path = request.url.path
        if not any(path == p or path.startswith(p) for p in _AUTH_EXEMPT_PREFIXES):
            try:
                verify_signature(
                    request.method,
                    request.url.path,
                    request.url.query,
                    request.headers,
                    access_key_id=config.ACCESS_KEY_ID,
                    secret_access_key=config.SECRET_ACCESS_KEY,
                )
            except SigV4Error as exc:
                from s3.xml_responses import error_response
                from fastapi.responses import Response as _Response

                log.warning("sigv4_rejected", code=exc.code, path=path)
                return _Response(
                    content=error_response(exc.code, exc.message, path),
                    status_code=403,
                    media_type="application/xml",
                )
    return await call_next(request)


# ---------------------------------------------------------------------------
# Request tracing (capture the Fabric read/convert timeline). Outermost
# middleware so it measures total handling time incl. auth. Skips ops paths.
# ---------------------------------------------------------------------------
_TRACE_EXEMPT_PREFIXES = ("/healthz", "/readyz", "/metrics", "/_admin", "/_config", "/_monitor", "/_manager", "/favicon.ico")


@app.middleware("http")
async def request_trace_middleware(request, call_next):
    if not config.REQUEST_TRACE:
        return await call_next(request)
    path = request.url.path
    if path == "/" or any(path == p or path.startswith(p) for p in _TRACE_EXEMPT_PREFIXES):
        return await call_next(request)
    import time as _time
    t0 = _time.perf_counter()
    response = await call_next(request)
    dur_ms = (_time.perf_counter() - t0) * 1000.0
    # Strip the leading /{bucket}/ to get the object key ("" for a bucket list).
    parts = path.lstrip("/").split("/", 1)
    obj_key = parts[1] if len(parts) > 1 else ""
    try:
        resp_bytes = int(response.headers.get("content-length", 0) or 0)
    except (TypeError, ValueError):
        resp_bytes = 0
    from observability import trace as _trace
    _trace.record(
        method=request.method,
        key=obj_key,
        status=response.status_code,
        duration_ms=dur_ms,
        resp_bytes=resp_bytes,
        range_header=request.headers.get("range"),
        user_agent=request.headers.get("user-agent"),
    )
    return response

# Operational endpoints (health / readiness / metrics). Mounted BEFORE the S3
# catch-all router so literal paths like /metrics and /_admin/stats are not
# captured by the /{bucket} and /{bucket}/{key} routes.
app.include_router(ops_router)

# Optional config-builder admin SPA at /_config (must precede the S3 catch-all).
if config.ENABLE_CONFIG_BUILDER:
    from configbuilder.router import router as config_builder_router
    app.include_router(config_builder_router)
    log.info("config_builder_enabled", path="/_config/")

# Optional monitoring dashboard SPA at /_monitor (must precede the S3 catch-all).
if config.ENABLE_MONITOR:
    from monitor.router import router as monitor_router
    app.include_router(monitor_router)
    log.info("monitor_enabled", path="/_monitor/")

# The operator console lives on the Manager (control port), not on an Agent. If a
# browser points at an Agent's /_manager, bounce it to the Manager's console
# (via MANAGER_URL) instead of returning a confusing SigV4 403. Mounted BEFORE the
# S3 catch-all so /{bucket} doesn't capture it.
@app.get("/_manager")
@app.get("/_manager/{rest:path}")
async def _manager_console_redirect(rest: str = ""):
    from fastapi.responses import RedirectResponse, JSONResponse
    if config.MANAGER_URL:
        suffix = f"/{rest}" if rest else ""
        return RedirectResponse(url=f"{config.MANAGER_URL.rstrip('/')}/_manager{suffix}", status_code=307)
    return JSONResponse(status_code=404, content={
        "error": "not_found",
        "detail": "This is an Agent (S3 data plane). The operator console runs on the "
                  "Manager's control port (default :9200) at /_manager.",
    })


@app.get("/favicon.ico")
async def _favicon():
    from fastapi.responses import Response as _Resp
    return _Resp(status_code=204)


# Mount S3 router at root – all routes are /{bucket}/...
app.include_router(s3_router)


# ---------------------------------------------------------------------------
# Admin endpoint: force snapshot refresh
# ---------------------------------------------------------------------------

@app.post("/_admin/refresh")
async def refresh_snapshot():
    """Rebuild every table's Iceberg snapshot (e.g., after schema/data change).

    With AUTO_REFRESH enabled, this forces a content-addressed materialize +
    publish per table (bypassing the change probe): a new snapshot version is
    published only for tables whose content actually changed.

    With ICEBERG_SNAPSHOT_HISTORY enabled, this instead *advances* each table to
    a new snapshot version (retaining history for time-travel); the shared data
    files are reused, so only the metadata/manifest caches are cleared.
    """
    from cache.lru_cache import _metadata_cache, _parquet_cache

    if config.AUTO_REFRESH:
        from iceberg import freshness
        import iceberg.state_store as state_store
        changed = await freshness.refresh_all(
            config.TABLES, config.BUCKET_NAME, config.WAREHOUSE_PREFIX
        )
        log.info("snapshots_refreshed_auto", changed=changed)
        return {
            "mode": "auto_refresh",
            "changed": changed,
            "tables": [
                {
                    "table": t.name,
                    "version": getattr(state_store._snapshots.get(t.name), "version", None),
                    "snapshot_id": getattr(state_store._snapshots.get(t.name), "snapshot_id", None),
                    "changed": t.name in changed,
                }
                for t in config.TABLES
            ],
        }

    if config.ICEBERG_SNAPSHOT_HISTORY:
        from iceberg.state_store import advance_table_snapshot
        _metadata_cache._store.clear()
        _metadata_cache._current_bytes = 0
        snapshots = [
            advance_table_snapshot(t.name, config.BUCKET_NAME, config.WAREHOUSE_PREFIX)
            for t in config.TABLES
        ]
        log.info("snapshots_advanced",
                 tables=[(s.table.name, s.version) for s in snapshots])
        return {
            "tables": [
                {"table": s.table.name, "version": s.version,
                 "snapshot_id": s.snapshot_id, "splits": len(s.splits)}
                for s in snapshots
            ]
        }

    _metadata_cache._store.clear()
    _metadata_cache._current_bytes = 0
    _parquet_cache._store.clear()
    _parquet_cache._current_bytes = 0

    snapshots = build_all_snapshots(
        config.TABLES,
        bucket=config.BUCKET_NAME,
        warehouse_prefix=config.WAREHOUSE_PREFIX,
    )
    log.info("snapshots_refreshed", tables=[s.table.name for s in snapshots])
    return {
        "tables": [
            {"table": s.table.name, "snapshot_id": s.snapshot_id, "splits": len(s.splits)}
            for s in snapshots
        ]
    }


@app.post("/_admin/gc")
async def admin_gc(dry_run: bool = False):
    """Phase 5: prune orphaned Parquet splits from the shared artifact store.

    Deletes data files no retained snapshot references (from versions aged out of
    history). ``?dry_run=true`` reports the orphans without deleting. Runs against
    the local-dir/shared store regardless of ARTIFACT_STORE_SERVING.
    """
    from runtime.artifact_store import build_store
    from runtime.retention import gc_orphaned_data

    store = getattr(app.state, "gc_store", None) or build_store(
        config.ARTIFACT_STORE_BACKEND, local_dir=config.ARTIFACT_STORE_DIR)
    orphans = await asyncio.get_event_loop().run_in_executor(
        None, lambda: gc_orphaned_data(store, dry_run=dry_run))
    return {"deleted": len(orphans), "dry_run": dry_run, "orphans": orphans[:50]}


@app.post("/_admin/publish-image")
async def admin_publish_image():
    """Phase 6: publish a complete servable image (data + metadata) to the store
    so a stateless/C++ Agent can serve every object as opaque bytes."""
    from runtime.artifact_store import build_store
    from runtime.serving_image import publish_serving_image

    store = build_store(config.ARTIFACT_STORE_BACKEND, local_dir=config.ARTIFACT_STORE_DIR)
    result = await asyncio.get_event_loop().run_in_executor(None, publish_serving_image, store)
    return result


if __name__ == "__main__":
    import uvicorn
    # Run via an explicit Server (app object, not "main:app") so the in-process
    # Agent link can request a graceful drain by setting server.should_exit.
    _uv_config = uvicorn.Config(app, host=config.HOST, port=config.PORT, log_level="info")
    _server = uvicorn.Server(_uv_config)
    _set_uvicorn_server(_server)
    _server.run()

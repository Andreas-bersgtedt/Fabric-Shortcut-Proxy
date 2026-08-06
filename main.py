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
import os
import pathlib
import sys

from fastapi import FastAPI, HTTPException
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


def _begin_drain() -> None:
    """Drain: flip /readyz to 503 so an external LB deregisters this backend, then
    exit after AGENT_DRAIN_GRACE_SECONDS so in-flight requests can finish."""
    from runtime.drain import set_draining
    set_draining(True)
    grace = max(0.0, config.AGENT_DRAIN_GRACE_SECONDS)
    log.info("drain_started", grace_seconds=grace)
    try:
        asyncio.get_event_loop().call_later(grace, _request_shutdown)
    except RuntimeError:
        _request_shutdown()


def _uvicorn_kwargs(tls: dict | None = None) -> dict:
    """uvicorn.Config kwargs. Trusts proxy headers only from FORWARDED_ALLOW_IPS so
    audit logging sees the real client IP (not the LB's) behind an external LB."""
    kw = {
        "host": config.HOST, "port": config.PORT, "log_level": "info",
        "proxy_headers": True, "forwarded_allow_ips": config.FORWARDED_ALLOW_IPS,
    }
    kw.update(tls or {})
    return kw


def _source_connect_hint(exc: Exception) -> str:
    """A concise, credential-redacted message when a source DB can't be reached at startup."""
    conns: dict[str, str] = {}
    for t in config.TABLES:
        cid = getattr(t, "connection_id", "default")
        if cid not in conns:
            conns[cid] = config.redact_db_url(config.effective_db_url(cid))
    lines = "\n".join(f"    - {cid}: {url}" for cid, url in conns.items())
    err = config.redact_db_url(str(exc))[:300]
    return (
        "Cannot reach the source database at startup (schema reflection failed).\n"
        "  Connection(s) used by your tables:\n" + lines + "\n"
        f"  Underlying error: {err}\n"
        "  Likely causes:\n"
        "    - Wrong username/password (SQL Server error 18456 = the server rejected the login).\n"
        "    - A named source is missing its DB_URL_<ID> environment variable (password stripped).\n"
        "    - The database host/port is unreachable or blocked by a firewall.\n"
        "  Fix the credentials via the config builder, the DB_URL env var (Manager.ps1 -DbUrl),\n"
        "  or config.connection.json, then restart."
    )


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
    try:
        await resolve_tables(config.TABLES)
        # Fail fast (H6) if any source table doesn't expose every declared column.
        if config.VALIDATE_SOURCE_SCHEMA:
            from db.executor import validate_source_schema
            for table in config.TABLES:
                await validate_source_schema(table)
    except Exception as exc:  # noqa: BLE001 - turn a raw driver stack trace into a clear message
        hint = _source_connect_hint(exc)
        log.error("source_connect_failed", detail=hint)
        # A bad credential / unreachable source is a PERMANENT config error, not a
        # transient crash. Exit with EX_CONFIG (78) and no traceback so the
        # supervisor stops restarting (see control/supervisor.py) instead of
        # crash-looping the whole fleet. The Manager UI (/_config) stays up to fix it.
        print("\n[startup] " + hint + "\n", file=sys.stderr, flush=True)
        sys.stderr.flush()
        os._exit(78)

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
        if any(t.effective_split_target_rows > 0 for t in runtime_tables):
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
        if any(t.effective_split_strategy in ("range", "date", "auto") for t in runtime_tables) or any(
            t.effective_split_target_rows > 0 for t in runtime_tables
        ):
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

        # Size-weighted split ownership (devplan/shardweight.md). When enabled with
        # >1 shard and a shared store, compute a per-table LPT assignment from the
        # prior run's observed split sizes so each shard gets a balanced share of
        # BYTES, not just count. Falls back to modulo per split when absent.
        _weight_store = None
        _shard_assignment: dict | None = None
        if config.SHARD_STRATEGY == "weighted" and config.AGENT_SHARD_COUNT > 1:
            from planner import shard_weight as _sw
            from runtime.artifact_store import build_store as _build_store
            try:
                _weight_store = _build_store(config.ARTIFACT_STORE_BACKEND,
                                             local_dir=config.ARTIFACT_STORE_DIR)
                _weights = _sw.load_weights(_weight_store)
                _shard_assignment = _sw.build_assignment(snapshots, config.AGENT_SHARD_COUNT, _weights)
                log.info("shard_assignment", strategy="weighted",
                         shard_index=config.AGENT_SHARD_INDEX,
                         shard_count=config.AGENT_SHARD_COUNT,
                         warm=bool(_weights),
                         loads=_sw.shard_loads(_shard_assignment, config.AGENT_SHARD_COUNT, _weights))
            except Exception as exc:  # noqa: BLE001 - never let weighting break startup
                log.warning("shard_weight_disabled", error=str(exc))
                _weight_store = None
                _shard_assignment = None

        def _owns_split(split) -> bool:
            """Phase 3: this Agent's shard owns (materializes) the split. With a
            single shard every split is owned (single-Agent / known-good path).
            When a weighted assignment is active, it decides ownership; otherwise
            (or for an unseen split) fall back to round-robin by split index."""
            n = config.AGENT_SHARD_COUNT
            if n <= 1:
                return True
            if _shard_assignment is not None:
                from planner.shard_weight import stable_key
                owner = _shard_assignment.get(stable_key(split.table.name, split.split_index))
                if owner is not None:
                    return owner == config.AGENT_SHARD_INDEX
            return split.split_index % n == config.AGENT_SHARD_INDEX

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

        if config.MATERIALIZE_MODE in ("lazy", "virtual"):
            log.info(
                "deferred_materialization_enabled",
                mode=config.MATERIALIZE_MODE,
                tables=[s.table.name for s in snapshots],
                hint="splits materialize on first read per table",
            )
        else:
            for snap in snapshots:
                if config.CONCURRENT_STARTUP_MATERIALIZATION:
                    counts = await asyncio.gather(*(_materialize(s) for s in snap.splits))
                else:
                    counts = [await _materialize(s) for s in snap.splits]
                snap.total_records = sum(counts)
                log.info("splits_materialized", table=snap.table.name,
                         total_records=snap.total_records, splits=len(snap.splits))

            # Persist this run's observed split sizes so the next run can balance by
            # bytes (weighted strategy). Shard 0 writes the full map — after the loop
            # every Agent knows all sizes (owned=generated, non-owned=fetched). Idempotent.
            if (config.SHARD_STRATEGY == "weighted" and _weight_store is not None
                    and config.AGENT_SHARD_INDEX == 0):
                from planner.shard_weight import stable_key, save_weights
                sizes = {
                    stable_key(snap.table.name, s.split_index): int(s.file_size_in_bytes)
                    for snap in snapshots for s in snap.splits
                    if s.file_size_in_bytes
                }
                if save_weights(_weight_store, sizes):
                    log.info("shard_weights_saved", entries=len(sizes))

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
    # Iceberg->Delta conversion layer. In lazy mode the commits are built per table
    # on first read (after that table's splits are materialized), so skip startup sync.
    if config.TABLE_FORMAT == "delta":
        from delta import log as delta_log
        if config.MATERIALIZE_MODE not in ("lazy", "virtual"):
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
        try:
            from enterprise.agent_link import AgentLink
        except ImportError as exc:
            raise RuntimeError(
                "MANAGER_URL is set but the enterprise package is not installed. "
                "Install it with: pip install fabric-shortcut-proxy-enterprise"
            ) from exc
        link = AgentLink(on_drain=_begin_drain)
        await link.start()
        app.state.agent_link = link

    # Phase 5: retention GC — one Agent (shard 0) periodically prunes orphaned
    # Parquet splits (from snapshot versions aged out of history) from the shared
    # store. Idempotent + best-effort; default off.
    app.state.gc_task = None
    if config.RETENTION_GC and config.AGENT_SHARD_INDEX == 0:
        from runtime.artifact_store import build_store
        try:
            from enterprise.retention import gc_orphaned_data
        except ImportError as exc:
            raise RuntimeError(
                "RETENTION_GC is enabled but the enterprise package is not installed. "
                "Install it with: pip install fabric-shortcut-proxy-enterprise"
            ) from exc
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
    version="2.1.1",
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


def _bucket_key_from_path(path: str) -> tuple[str, str]:
    """Split ``/{bucket}/{key}`` into (bucket, key); ('', '') for the service root."""
    p = path.lstrip("/")
    if not p:
        return "", ""
    parts = p.split("/", 1)
    return parts[0], (parts[1] if len(parts) > 1 else "")


@app.middleware("http")
async def sigv4_auth_middleware(request, call_next):
    if request.method == "OPTIONS":
        return await call_next(request)
    path = request.url.path
    if any(path == p or path.startswith(p) for p in _AUTH_EXEMPT_PREFIXES):
        return await call_next(request)

    from s3.xml_responses import error_response
    from fastapi.responses import Response as _Response
    from security import access_keys

    bucket, key = _bucket_key_from_path(path)
    mounted = False
    if bucket:
        try:
            from storage.mounts import get_mount
            mounted = get_mount(bucket) is not None
        except Exception:  # noqa: BLE001 - proxy lookup must never break the front door
            mounted = False

    # A secured proxy forces auth on mounted buckets even if the global flag is off.
    require = config.REQUIRE_SIGV4 or (mounted and config.ENFORCE_MOUNT_AUTH)
    if require:
        try:
            identity = verify_signature(
                request.method,
                request.url.path,
                request.url.query,
                request.headers,
                secret_resolver=access_keys.resolve_secret,
            )
        except SigV4Error as exc:
            log.warning("sigv4_rejected", code=exc.code, path=path)
            if mounted:
                from observability import audit
                audit.record(identity="-", client=(request.client.host if request.client else ""),
                             bucket=bucket, key=key, backend="mount", method=request.method,
                             action="auth", status=403, reason=exc.code)
            return _Response(content=error_response(exc.code, exc.message, path),
                             status_code=403, media_type="application/xml")

        denial = access_keys.authorize(identity, bucket, key, request.method)
        if denial is not None:
            log.warning("authz_denied", identity=identity, bucket=bucket, reason=denial)
            if mounted:
                from observability import audit
                audit.record(identity=identity, client=(request.client.host if request.client else ""),
                             bucket=bucket, key=key, backend="mount", method=request.method,
                             action="authz", status=403, reason=denial)
            return _Response(content=error_response("AccessDenied", denial, path),
                             status_code=403, media_type="application/xml")
        request.state.identity = identity
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
    from fastapi.responses import FileResponse
    return FileResponse(
        pathlib.Path(__file__).parent / "docs" / "images" / "FSP_FaviIcon.png",
        media_type="image/png",
    )


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
    try:
        from enterprise.retention import gc_orphaned_data
    except ImportError as exc:
        raise HTTPException(
            status_code=501,
            detail="Retention GC requires the enterprise package "
                   "(pip install fabric-shortcut-proxy-enterprise).",
        ) from exc

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
    _tls = {}
    if config.TLS_CERT_FILE and config.TLS_KEY_FILE:
        _tls = {"ssl_certfile": config.TLS_CERT_FILE, "ssl_keyfile": config.TLS_KEY_FILE}
        log.info("tls_enabled", cert=config.TLS_CERT_FILE)
    elif config.REQUIRE_SIGV4 or config.ENABLE_STORAGE_PROXY:
        # SigV4 read signatures give no confidentiality over plain HTTP.
        log.warning("tls_not_configured",
                    hint="set tls_cert_file + tls_key_file, or terminate TLS at a fronting LB")
    _uv_config = uvicorn.Config(app, **_uvicorn_kwargs(_tls))
    _server = uvicorn.Server(_uv_config)
    _set_uvicorn_server(_server)
    _server.run()

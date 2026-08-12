"""
Manager control application.

Builds the control‑plane FastAPI app (REST transport) backed by a
:class:`~enterprise.control.registry.Registry` + :class:`~enterprise.control.server.ControlService`, and
supervises **N** local Agent child processes via
:class:`~enterprise.control.supervisor.AgentSupervisor` (spawn + heartbeat/exit watch +
restart‑on‑crash). When ``ENABLE_GATEWAY`` is set it also fronts the fleet with a
built‑in round‑robin S3 gateway (:mod:`enterprise.control.gateway`). Run it with
``python -m enterprise.manager``.

Phase 1 = 1 Agent; Phase 3 = N Agents + gateway + sharded materialization; Manager
HA is Phase 5. The Fabric‑facing S3 data plane still lives in the Agents — point
the Fabric shortcut at the gateway (or directly at an Agent).
"""
from __future__ import annotations

import asyncio
import contextlib
import os
import pathlib
import shlex
import sys

from fastapi import FastAPI, Request

import config
from enterprise.control.auth import ManagerAuthMiddleware, manager_auth_active
from enterprise.control.registry import Registry
from enterprise.control.server import ControlService
from enterprise.control.supervisor import AgentSupervisor
from enterprise.control.transport import create_control_router
from observability.logging import configure_logging, get_logger

log = get_logger(__name__)

# This file lives at <repo>/enterprise/control/manager_app.py, so the repo root
# (where the Agent entrypoint main.py lives) is three levels up.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _agent_host_for_link() -> str:
    """The host an Agent should dial to reach this Manager's control port."""
    h = config.CONTROL_HOST
    return "127.0.0.1" if h in ("0.0.0.0", "::", "") else h


def _agent_launch_cmd() -> list[str]:
    """The command that starts one Agent (the existing S3 server, main.py).

    Override with the ``AGENT_LAUNCH_CMD`` env var (shlex‑split) for custom
    packaging / a future C++ Agent binary.
    """
    override = os.environ.get("AGENT_LAUNCH_CMD", "").strip()
    if override:
        return shlex.split(override, posix=(os.name == "posix"))
    return [sys.executable, os.path.join(_REPO_ROOT, "main.py")]


def _agent_env(agent_id: str, *, port: int, shard_index: int, shard_count: int) -> dict[str, str]:
    manager_url = f"http://{_agent_host_for_link()}:{config.CONTROL_PORT}"
    return {
        "MANAGER_URL": manager_url,
        "AGENT_ID": agent_id,
        # Each Agent serves the S3 data plane on its own port (PORT + i).
        "PORT": str(port),
        # Phase 2: supervised Agents serve materialized Parquet from the shared
        # artifact store (durable, restart-safe). Honors an explicit override.
        "ARTIFACT_STORE_SERVING": os.environ.get("ARTIFACT_STORE_SERVING", "1"),
        "ARTIFACT_STORE_DIR": config.ARTIFACT_STORE_DIR,
        # Phase 3: distributed materialization — this Agent's shard of the splits.
        "AGENT_SHARD_INDEX": str(shard_index),
        "AGENT_SHARD_COUNT": str(shard_count),
        # Split-ownership strategy — shared fleet-wide so every shard agrees.
        "SHARD_STRATEGY": config.SHARD_STRATEGY,
        # Materialization mode — shared fleet-wide so every Agent agrees (eager vs
        # lazy). Multi-shard lazy relies on the shared store forced above.
        "MATERIALIZE_MODE": config.MATERIALIZE_MODE,
        # Expose each Agent's monitor API so the Manager's operator console can
        # scrape + aggregate it (the console's Monitor tab lives on the Manager).
        "ENABLE_MONITOR": "1" if (config.ENABLE_MONITOR or config.ENABLE_ADMIN_UI)
                          else os.environ.get("ENABLE_MONITOR", "0"),
    }


def _build_supervisors() -> list[AgentSupervisor]:
    """One supervisor per Agent (count = AGENT_COUNT), each on PORT + i with its
    own materialization shard."""
    count = max(1, config.AGENT_COUNT)
    return [_make_supervisor(i, count) for i in range(count)]


def _make_supervisor(i: int, count: int) -> AgentSupervisor:
    """Build a single Agent supervisor for shard ``i`` of ``count`` (PORT + i)."""
    agent_id = f"agent-{i + 1}"
    return AgentSupervisor(
        _agent_launch_cmd(),
        env=_agent_env(agent_id, port=config.PORT + i, shard_index=i, shard_count=count),
        name=agent_id,
        restart_backoff=config.AGENT_RESTART_BACKOFF_SECONDS,
        max_rapid_restarts=config.AGENT_MAX_RAPID_RESTARTS,
        memory_alert_threshold_mb=config.MEMORY_ALERT_THRESHOLD_MB,
        memory_restart_threshold_mb=config.MEMORY_RESTART_THRESHOLD_MB,
        memory_history_samples=config.MEMORY_HISTORY_SAMPLES,
    )


def create_manager_app() -> FastAPI:
    registry = Registry(heartbeat_ms=config.HEARTBEAT_MS, miss_limit=config.HEARTBEAT_MISS_LIMIT)
    service = ControlService(registry, tables=[t.name for t in config.TABLES])
    supervisors = _build_supervisors()
    gateway = None
    if config.ENABLE_GATEWAY:
        from enterprise.control.gateway import Gateway
        gateway = Gateway(registry)

    # Phase 5 HA: a leader lease over the shared artifact store. Only the primary
    # supervises Agents (+ serves the gateway, which naturally 503s on a standby
    # because no Agents register to it). Default off => always primary.
    lease = None
    if config.MANAGER_HA:
        from enterprise.control.lease import LeaderLease
        from runtime.artifact_store import build_store
        store = build_store(config.ARTIFACT_STORE_BACKEND, local_dir=config.ARTIFACT_STORE_DIR)
        lease = LeaderLease(store, ttl_ms=config.LEADER_LEASE_TTL_MS)

    async def _start_all():
        for s in supervisors:
            await s.start()
        log.info("agents_supervised", agents=[(s.name, s.pid) for s in supervisors])

    async def _stop_all():
        for s in supervisors:
            await s.stop()

    async def _leadership_loop():
        """Acquire/renew the lease; supervise only while primary (Phase 5 HA)."""
        supervising = False
        renew_s = max(0.05, config.LEADER_LEASE_RENEW_MS / 1000.0)
        loop = asyncio.get_event_loop()
        try:
            while True:
                try:
                    leader = await loop.run_in_executor(None, lease.acquire_or_renew)
                except Exception as exc:  # noqa: BLE001
                    log.warning("ha_lease_error", error=str(exc))
                    leader = False
                app.state.is_leader = leader
                if leader and not supervising:
                    log.info("ha_became_primary", owner_id=lease.owner_id)
                    await _start_all()
                    supervising = True
                elif not leader and supervising:
                    log.warning("ha_stepped_down_to_standby", owner_id=lease.owner_id)
                    await _stop_all()
                    supervising = False
                await asyncio.sleep(renew_s)
        except asyncio.CancelledError:
            if supervising:
                await _stop_all()
            raise

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI):
        configure_logging()
        config.validate_config()
        agent_ports = [config.PORT + i for i in range(len(supervisors))]
        log.info("manager_startup", control_host=config.CONTROL_HOST,
                 control_port=config.CONTROL_PORT, agent_count=len(supervisors),
                 agent_ports=agent_ports, gateway=bool(gateway),
                 admin_ui=config.ENABLE_ADMIN_UI, manager_ha=config.MANAGER_HA,
                 manager_auth=manager_auth_active(),
                 tables=[t.name for t in config.TABLES])
        if config.MANAGER_AUTH_ENABLED and not config.MANAGER_AUTH_PASSWORD:
            log.warning("manager_auth_enabled_without_password",
                        hint="set MANAGER_AUTH_PASSWORD to activate the Basic auth gate; running open")
        ha_task = None
        if lease is not None:
            app.state.is_leader = False
            log.info("ha_standby_started", ttl_ms=config.LEADER_LEASE_TTL_MS)
            ha_task = asyncio.create_task(_leadership_loop(), name="ha-leadership")
        else:
            app.state.is_leader = True
            await _start_all()
        # Open Mirroring publish loop (opt-in): push source tables into the Fabric
        # landing zone on a schedule. Fails soft per target/table.
        om_scheduler = None
        if config.OPEN_MIRROR_PUBLISH:
            from open_mirror.scheduler import OpenMirrorScheduler
            om_scheduler = OpenMirrorScheduler()
            om_scheduler.start()
            log.info("open_mirror_publish_enabled",
                     interval_seconds=config.OPEN_MIRROR_INTERVAL_SECONDS,
                     mode=config.OPEN_MIRROR_MODE)
        app.state.open_mirror_scheduler = om_scheduler
        yield
        log.info("manager_shutdown")
        if om_scheduler is not None:
            await om_scheduler.stop()
        if ha_task is not None:
            ha_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await ha_task
            if lease is not None:
                lease.release()
        else:
            await _stop_all()
        if gateway is not None:
            await gateway.aclose()

    app = FastAPI(title="Fabric Shortcut Proxy — Manager", version="2.4.0", lifespan=lifespan)
    app.state.registry = registry
    app.state.supervisors = supervisors
    app.state.lease = lease
    app.state.is_leader = not config.MANAGER_HA
    # Standalone HTTP Basic gate over the operator surface (opt-in; leaves
    # /control + health probes open so the fleet and LBs keep working).
    app.add_middleware(ManagerAuthMiddleware)
    app.include_router(create_control_router(service))

    @app.get("/healthz")
    async def healthz():
        return {"status": "ok", "role": "manager",
                "is_leader": getattr(app.state, "is_leader", True),
                "manager_ha": config.MANAGER_HA,
                "agents_supervised": len(supervisors), "agents_registered": registry.count()}

    @app.get("/readyz")
    async def readyz():
        from fastapi.responses import JSONResponse
        leader = getattr(app.state, "is_leader", True)
        alive = [s for s in supervisors if s.is_alive]
        looped = [s.name for s in supervisors if s.crash_looped]
        # A standby is "ready" as a warm spare even though it supervises nothing.
        ready = (not leader) or (len(alive) >= 1 and not looped)
        return JSONResponse(
            status_code=200 if ready else 503,
            content={
                "status": "ready" if ready else "not-ready",
                "role": "primary" if leader else "standby",
                "agents_alive": len(alive),
                "agents_total": len(supervisors),
                "crash_looped": looped,
                "restarts": {s.name: s.restart_count for s in supervisors},
            },
        )

    @app.get("/favicon.ico")
    async def favicon():
        from fastapi.responses import FileResponse
        return FileResponse(
            pathlib.Path(__file__).parents[2] / "docs" / "images" / "FSP_FaviIcon.png",
            media_type="image/png",
        )

    @app.get("/agents")
    async def agents():
        return {"agents": registry.list_public(), "dead": registry.dead_agents()}

    @app.post("/control/materialize")
    async def control_materialize(request: Request):
        """On-demand materialization for stateless (e.g. C++) Agents under lazy mode.

        An Agent that hits a store miss posts ``{"key": "<object key>"}``; the
        Manager materializes that table into the shared artifact store (data +
        metadata) so the Agent can then serve it. No-op-safe / idempotent.
        """
        from fastapi.responses import JSONResponse
        if config.MATERIALIZE_MODE != "lazy":
            return JSONResponse(status_code=409,
                                content={"ok": False, "error": "materialize_mode is not lazy"})
        try:
            body = await request.json()
        except Exception:
            body = {}
        key = str((body or {}).get("key", "")).strip()
        if not key:
            return JSONResponse(status_code=400, content={"ok": False, "error": "missing key"})
        from enterprise.control import materialize_service
        try:
            result = await materialize_service.materialize_for_key(key)
        except Exception as exc:  # noqa: BLE001 - report, never crash the control plane
            log.exception("control_materialize_failed", key=key)
            return JSONResponse(status_code=500, content={"ok": False, "error": str(exc)})
        return JSONResponse(status_code=200 if result.get("ok") else 404, content=result)

    # Phase 5.1: live fleet scaling — grow/shrink the supervised Agent fleet at
    # runtime and persist agent_count. Mutates `supervisors` IN PLACE so the
    # console + gateway see the new fleet immediately.
    _scale_lock = asyncio.Lock()

    async def _scale_fleet(target: int) -> dict:
        target = int(target)
        if target < 1:
            raise ValueError("count must be >= 1")
        leader = getattr(app.state, "is_leader", True)
        async with _scale_lock:
            cur = len(supervisors)
            applied = leader
            if leader and target > cur:
                for i in range(cur, target):
                    sup = _make_supervisor(i, target)
                    await sup.start()
                    supervisors.append(sup)
                log.info("fleet_scaled_up", frm=cur, to=target)
            elif leader and target < cur:
                for sup in supervisors[target:]:
                    registry.remove(sup.name)     # drop from gateway rotation now
                    await sup.stop()
                del supervisors[target:]
                log.info("fleet_scaled_down", frm=cur, to=target)
            try:
                config.write_config_updates({"agent_count": target})
                persisted = True
            except Exception as exc:  # noqa: BLE001
                log.warning("fleet_scale_persist_failed", error=str(exc))
                persisted = False
            return {"ok": True, "count": len(supervisors), "target": target,
                    "applied": applied, "persisted": persisted,
                    "agents": [s.name for s in supervisors],
                    "note": None if applied else
                            "persisted agent_count; this Manager is a standby — scale via the primary"}

    async def _shutdown_manager() -> dict:
        """Stop all Agents and shut the Manager down gracefully (Phase 5.1)."""
        log.info("manager_shutdown_requested", agents=len(supervisors))

        async def _do():
            await asyncio.sleep(0.3)          # let the HTTP response flush first
            try:
                await _stop_all()             # kill the supervised Agents promptly
            except Exception as exc:          # noqa: BLE001
                log.warning("shutdown_stop_all_error", error=str(exc))
            srv = getattr(app.state, "uvicorn_server", None)
            if srv is not None:
                srv.should_exit = True        # graceful uvicorn exit -> lifespan shutdown
            else:
                import os as _os
                _os._exit(0)                  # no server handle -> hard exit (Agents already stopped)

        asyncio.create_task(_do())
        return {"ok": True, "action": "shutdown", "agents": len(supervisors),
                "note": "stopping all Agents and shutting down the Manager"}

    # Phase 4: /_manager operator console (fleet monitor + start/stop/restart/drain).
    # Gated behind ENABLE_ADMIN_UI; mounted BEFORE the gateway catch-all so its
    # /_manager routes are not shadowed by the gateway's /{bucket} route.
    if config.ENABLE_ADMIN_UI:
        from enterprise.control.admin import create_admin_router
        app.include_router(create_admin_router(
            registry, supervisors, gateway=gateway, token=config.ADMIN_TOKEN,
            scale=_scale_fleet, shutdown=_shutdown_manager,
        ))

    # Phase 5.1: config builder (read current config + push changes) on the Manager,
    # so cluster settings (agent_count etc.) are editable where they apply. Reserved
    # from the gateway catch-all (see enterprise.control.gateway._RESERVED_PREFIXES).
    if config.ENABLE_CONFIG_BUILDER:
        from configbuilder.router import router as config_builder_router
        app.include_router(config_builder_router)

    # Fleet monitor: the operator console's Monitor tab (and the standalone SPA)
    # live on the Manager, but the live stats are per-Agent — this router scrapes
    # every Agent's /_monitor/api/summary and merges them. Mounted BEFORE the
    # gateway catch-all (which also reserves /_monitor) so it isn't shadowed.
    if config.ENABLE_ADMIN_UI or config.ENABLE_MONITOR:
        from enterprise.control.monitor_proxy import create_monitor_proxy_router
        app.include_router(create_monitor_proxy_router(supervisors))

    # Gateway (LB) MUST be included last: its /{bucket} catch-all would otherwise
    # shadow the control/health routes above.
    if gateway is not None:
        from enterprise.control.gateway import create_gateway_router
        app.include_router(create_gateway_router(gateway))

    return app


app = create_manager_app()

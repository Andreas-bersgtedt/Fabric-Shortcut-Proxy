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
import secrets
import shlex
import sys
import time

from fastapi import FastAPI, Request

import config
from enterprise.control.auth import ManagerAuthMiddleware, manager_auth_active
from security.authorization_middleware import AuthorizationMiddleware
from enterprise.control.registry import Registry
from enterprise.control.server import ControlService
from enterprise.control.snapshot_provider import DurableSnapshotProvider
from enterprise.control.task_scheduler import TaskScheduler
from enterprise.control.work_queue import DurableWorkQueue
from enterprise.control.supervisor import AgentSupervisor
from enterprise.control.transport import create_control_router
from observability.logging import configure_logging, get_logger
from security.agent_auth import AgentAuthMiddleware

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


def _agent_env(agent_id: str, *, port: int, shard_index: int, shard_count: int,
               monitor_token: str) -> dict[str, str]:
    manager_url = f"http://{_agent_host_for_link()}:{config.CONTROL_PORT}"
    return {
        "MANAGER_URL": manager_url,
        "AGENT_ID": agent_id,
        "AGENT_TOKEN": os.environ.get("AGENT_TOKEN", ""),
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
        "FSP_INTERNAL_MONITOR_TOKEN": monitor_token,
    }


def _build_supervisors(monitor_token: str = "") -> list[AgentSupervisor]:
    """One supervisor per Agent (count = AGENT_COUNT), each on PORT + i with its
    own materialization shard."""
    if config.MANAGER_SUPERVISION_MODE == "external":
        return []
    count = max(1, config.AGENT_COUNT)
    return [_make_supervisor(i, count, monitor_token) for i in range(count)]


def _make_supervisor(i: int, count: int, monitor_token: str = "") -> AgentSupervisor:
    """Build a single Agent supervisor for shard ``i`` of ``count`` (PORT + i)."""
    agent_id = f"agent-{i + 1}"
    return AgentSupervisor(
        _agent_launch_cmd(),
        env=_agent_env(agent_id, port=config.PORT + i, shard_index=i, shard_count=count,
                       monitor_token=monitor_token),
        name=agent_id,
        restart_backoff=config.AGENT_RESTART_BACKOFF_SECONDS,
        max_rapid_restarts=config.AGENT_MAX_RAPID_RESTARTS,
        memory_alert_threshold_mb=config.MEMORY_ALERT_THRESHOLD_MB,
        memory_restart_threshold_mb=config.MEMORY_RESTART_THRESHOLD_MB,
        memory_history_samples=config.MEMORY_HISTORY_SAMPLES,
    )


def create_manager_app() -> FastAPI:
    if config.AGENT_AUTH_MODE == "compatibility":
        log.warning(
            "agent_auth_compatibility_enabled",
            detail="Manager Basic remains accepted on Agent routes during migration",
        )
    # Locally-supervised Agents get this token freshly via _agent_env(); externally
    # deployed Agents (MANAGER_SUPERVISION_MODE=external) have no such channel, so a
    # random value here would never match theirs — let a shared secret override it.
    monitor_token = os.environ.get("FSP_INTERNAL_MONITOR_TOKEN") or secrets.token_urlsafe(32)
    registry = Registry(
        heartbeat_ms=config.HEARTBEAT_MS,
        miss_limit=config.HEARTBEAT_MISS_LIMIT,
        allowed_hosts=tuple(
            item.strip() for item in config.AGENT_HOST_ALLOWLIST.split(",") if item.strip()
        ),
    )
    from runtime.artifact_store import get_default_store

    store = get_default_store()
    lease = None
    if config.MANAGER_HA:
        from enterprise.control.lease import LeaderLease
        lease = LeaderLease(store, ttl_ms=config.LEADER_LEASE_TTL_MS)

    def _require_active_leader() -> None:
        if lease is not None:
            lease.validate()
            if not getattr(app.state, "primary_ready", False):
                from enterprise.control.lease import StaleLeaderError
                raise StaleLeaderError("Manager primary activation is not complete")

    queue = DurableWorkQueue(
        store,
        leadership_check=lease.validate if lease is not None else None,
    )
    snapshot_provider = DurableSnapshotProvider(queue)
    service = ControlService(
        registry,
        tables=[t.name for t in config.TABLES],
        snapshot_provider=snapshot_provider,
        work_queue=queue,
        leadership_check=_require_active_leader,
    )
    from enterprise.control import materialize_service

    if config.MATERIALIZATION_WORK_QUEUE:
        materialize_service.configure(queue)
    supervisors = _build_supervisors(monitor_token)
    gateway = None
    if config.ENABLE_GATEWAY:
        from enterprise.control.gateway import Gateway
        gateway = Gateway(registry)

    # Phase 5 HA: a leader lease over the shared artifact store. Only the primary
    # supervises Agents and mutates durable control-plane state.
    manager_owner = lease.owner_id if lease is not None else f"manager:{os.getpid()}"
    scheduler: TaskScheduler | None = None
    om_scheduler = None

    def _record_rolling_event(event: str, agent: str, healthy: bool) -> None:
        if lease is None:
            return
        current = lease.read_state("rolling_restart") or {}
        completed = list(current.get("completed_agents") or [])
        if event == "restarted" and agent and agent not in completed:
            completed.append(agent)
        lease.mutate_state(
            "rolling_restart",
            {
                "status": event,
                "agent": agent,
                "healthy": bool(healthy),
                "completed_agents": completed,
                "manager_owner": lease.owner_id,
                "manager_fence": lease.fence,
                "updated_at_ms": int(time.time() * 1000),
            },
        )

    def _recover_rolling_state() -> None:
        if lease is None:
            return
        current = lease.read_state("rolling_restart")
        if isinstance(current, dict) and current.get("status") in {
            "restarting",
            "restarted",
        }:
            recovered = dict(current)
            recovered.update({
                "status": "failed",
                "reason": "leadership_changed",
                "manager_owner": lease.owner_id,
                "manager_fence": lease.fence,
                "updated_at_ms": int(time.time() * 1000),
            })
            lease.mutate_state("rolling_restart", recovered)

    async def _start_scheduler(manager_fence: int) -> None:
        nonlocal scheduler
        await asyncio.to_thread(queue.recover)
        await asyncio.to_thread(
            queue.fence_claims, manager_owner, manager_fence
        )
        await asyncio.to_thread(
            queue.prune_terminal,
            retention_seconds=config.WORK_QUEUE_RETENTION_SECONDS,
        )
        if not config.MATERIALIZATION_WORK_QUEUE:
            return
        scheduler = TaskScheduler(
            queue,
            registry,
            manager_owner=manager_owner,
            manager_fence=manager_fence,
            membership_policy=config.GENERATION_MEMBERSHIP_POLICY,
        )
        app.state.work_scheduler = scheduler
        scheduler.start()

    async def _stop_scheduler() -> None:
        nonlocal scheduler
        if scheduler is not None:
            await scheduler.stop()
            scheduler = None
            app.state.work_scheduler = None

    async def _start_all():
        for s in supervisors:
            await s.start()
        log.info("agents_supervised", agents=[(s.name, s.pid) for s in supervisors])

    async def _stop_all():
        for s in supervisors:
            await s.stop()

    async def _start_open_mirror() -> None:
        nonlocal om_scheduler
        if not config.OPEN_MIRROR_PUBLISH or om_scheduler is not None:
            return
        from open_mirror.scheduler import OpenMirrorScheduler
        om_scheduler = OpenMirrorScheduler(leadership_check=_require_active_leader)
        om_scheduler.start()
        app.state.open_mirror_scheduler = om_scheduler
        log.info(
            "open_mirror_publish_enabled",
            interval_seconds=config.OPEN_MIRROR_INTERVAL_SECONDS,
            mode=config.OPEN_MIRROR_MODE,
        )

    async def _stop_open_mirror() -> None:
        nonlocal om_scheduler
        if om_scheduler is not None:
            await om_scheduler.stop()
            om_scheduler = None
            app.state.open_mirror_scheduler = None

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
                try:
                    await asyncio.to_thread(
                        lease.publish_presence,
                        "primary" if leader else "standby",
                    )
                except Exception as exc:  # noqa: BLE001
                    log.warning("ha_presence_write_error", error=str(exc))
                if leader and not supervising:
                    app.state.is_leader = False
                    app.state.primary_ready = False
                    log.info("ha_became_primary", owner_id=lease.owner_id)
                    try:
                        await asyncio.to_thread(
                            registry.attach_durable_state,
                            lease,
                        )
                        await asyncio.to_thread(_recover_rolling_state)
                        await _start_scheduler(lease.fence)
                        await _start_all()
                        supervising = True
                        app.state.primary_ready = True
                        app.state.is_leader = True
                        await _start_open_mirror()
                    except Exception:
                        log.exception("ha_primary_activation_failed")
                        await _stop_open_mirror()
                        registry.detach_durable_state()
                        lease.release()
                        app.state.is_leader = False
                        app.state.primary_ready = False
                elif not leader and supervising:
                    app.state.is_leader = False
                    app.state.primary_ready = False
                    log.warning("ha_stepped_down_to_standby", owner_id=lease.owner_id)
                    await _stop_open_mirror()
                    await _stop_scheduler()
                    await _stop_all()
                    registry.detach_durable_state()
                    supervising = False
                elif not leader:
                    app.state.is_leader = False
                    app.state.primary_ready = False
                await asyncio.sleep(renew_s)
        except asyncio.CancelledError:
            if supervising:
                app.state.is_leader = False
                app.state.primary_ready = False
                await _stop_open_mirror()
                await _stop_scheduler()
                await _stop_all()
                registry.detach_durable_state()
            raise

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI):
        configure_logging()
        config.validate_config(operator_bind_host=config.CONTROL_HOST)
        if os.environ.get("KUBERNETES_SERVICE_HOST") and config.MANAGER_SUPERVISION_MODE != "external":
            raise RuntimeError(
                "Manager local supervision is disabled in Kubernetes because it would spawn "
                "an unintended Agent child process. Set MANAGER_SUPERVISION_MODE=external "
                "and deploy Agents separately, or omit the Manager deployment."
            )
        agent_ports = [config.PORT + i for i in range(len(supervisors))]
        log.info("manager_startup", control_host=config.CONTROL_HOST,
                 control_port=config.CONTROL_PORT, agent_count=len(supervisors),
                 agent_ports=agent_ports, gateway=bool(gateway),
                 admin_ui=config.ENABLE_ADMIN_UI, manager_ha=config.MANAGER_HA,
                 supervision_mode=config.MANAGER_SUPERVISION_MODE,
                 manager_auth=manager_auth_active(),
                 tables=[t.name for t in config.TABLES])
        if config.MANAGER_AUTH_ENABLED and not config.MANAGER_AUTH_PASSWORD:
            log.warning("manager_auth_enabled_without_password",
                        hint="set MANAGER_AUTH_PASSWORD before exposing the Manager")
        ha_task = None
        if lease is not None:
            app.state.is_leader = False
            app.state.primary_ready = False
            log.info("ha_standby_started", ttl_ms=config.LEADER_LEASE_TTL_MS)
            ha_task = asyncio.create_task(_leadership_loop(), name="ha-leadership")
        else:
            app.state.is_leader = True
            app.state.primary_ready = True
            await _start_scheduler(1)
            if supervisors:
                await _start_all()
            await _start_open_mirror()
        app.state.open_mirror_scheduler = om_scheduler
        yield
        log.info("manager_shutdown")
        if ha_task is not None:
            ha_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await ha_task
            if lease is not None:
                lease.release()
                lease.remove_presence()
        else:
            await _stop_open_mirror()
            await _stop_scheduler()
            if supervisors:
                await _stop_all()
        if gateway is not None:
            await gateway.aclose()

    app = FastAPI(title="Fabric Shortcut Proxy: Manager", version="2.10.0", lifespan=lifespan)
    app.state.registry = registry
    app.state.supervisors = supervisors
    app.state.lease = lease
    app.state.work_queue = queue
    app.state.work_scheduler = scheduler
    app.state.is_leader = not config.MANAGER_HA
    app.state.primary_ready = not config.MANAGER_HA
    app.state.open_mirror_scheduler = None
    # Standalone HTTP Basic gate over the operator surface. Health probes remain
    # open; authenticated Agent credentials are sent for internal control calls.
    app.add_middleware(AuthorizationMiddleware)
    app.add_middleware(ManagerAuthMiddleware)
    app.add_middleware(AgentAuthMiddleware)
    app.include_router(create_control_router(service))

    @app.get("/healthz")
    async def healthz():
        ha_status = await asyncio.to_thread(lease.status) if lease is not None else None
        return {"status": "degraded" if ha_status and ha_status["degraded"] else "ok",
                "role": "manager",
                "is_leader": getattr(app.state, "is_leader", True),
                "manager_ha": config.MANAGER_HA,
                "ha": ha_status,
                "agents_supervised": len(supervisors), "agents_registered": registry.count()}

    @app.get("/metrics")
    async def manager_metrics():
        from fastapi.responses import PlainTextResponse
        from observability.metrics import render_prometheus

        return PlainTextResponse(
            render_prometheus(),
            media_type="text/plain; version=0.0.4; charset=utf-8",
        )

    @app.get("/readyz")
    async def readyz():
        from fastapi.responses import JSONResponse
        leader = getattr(app.state, "is_leader", True)
        alive = [s for s in supervisors if s.is_alive]
        looped = [s.name for s in supervisors if s.crash_looped]
        # A standby is "ready" as a warm spare even though it supervises nothing.
        managed_agents_ready = (
            registry.count() >= 1
            if config.MANAGER_SUPERVISION_MODE == "external"
            else len(alive) >= 1 and not looped
        )
        queue_status = None
        queue_ready = True
        ha_status = await asyncio.to_thread(lease.status) if lease is not None else None
        ha_ready = not ha_status or (
            ha_status["available"] and not ha_status["expired"]
        )
        if config.MATERIALIZATION_WORK_QUEUE:
            try:
                detailed_queue = await asyncio.to_thread(queue.status)
                membership = detailed_queue.get("membership") or {}
                queue_status = {
                    key: detailed_queue[key]
                    for key in (
                        "requests",
                        "tasks",
                        "published_snapshots",
                        "states",
                        "queue_depth",
                        "active_claims",
                        "oldest_queued_age_ms",
                    )
                }
                queue_status["membership"] = (
                    {
                        "policy": membership.get("policy"),
                        "active_workers": sum(
                            worker.get("state") == "active"
                            for worker in membership.get("workers", [])
                        ),
                        "unassigned_work": membership.get("unassigned_work", 0),
                        "reassignment_count": membership.get(
                            "reassignment_count", 0
                        ),
                        "completed_tasks": membership.get("completed_tasks", 0),
                        "total_tasks": membership.get("total_tasks", 0),
                        "progress": membership.get("progress", 0.0),
                    }
                    if membership else None
                )
            except Exception:
                queue_ready = False
                log.exception("work_queue_readiness_failed")
        ready = ((not leader) or managed_agents_ready) and queue_ready and ha_ready
        return JSONResponse(
            status_code=200 if ready else 503,
            content={
                "status": "ready" if ready else "not-ready",
                "role": "primary" if leader else "standby",
                "agents_alive": len(alive),
                "agents_total": len(supervisors),
                "agents_registered": registry.count(),
                "work_queue": queue_status,
                "ha": ha_status,
                "supervision_mode": config.MANAGER_SUPERVISION_MODE,
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
        if config.MATERIALIZATION_WORK_QUEUE and not getattr(
            app.state, "is_leader", True
        ):
            return JSONResponse(
                status_code=409,
                content={"ok": False, "error": "Manager is not primary"},
            )
        try:
            _require_active_leader()
        except Exception as exc:
            return JSONResponse(
                status_code=503,
                headers={"Retry-After": "1"},
                content={"ok": False, "error": "Manager is not primary", "detail": str(exc)},
            )
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
        except TimeoutError:
            log.warning("control_materialize_timeout", key=key)
            return JSONResponse(
                status_code=504,
                content={"ok": False, "error": "materialization timed out"},
            )
        except Exception:  # noqa: BLE001 - report, never crash the control plane
            log.exception("control_materialize_failed", key=key)
            return JSONResponse(
                status_code=500,
                content={"ok": False, "error": "materialization failed"},
            )
        reason = str(result.get("reason", ""))
        status = (
            200
            if result.get("ok")
            else 404
            if reason == "unknown_key"
            else 503
            if reason == "generation_unavailable"
            else 409
            if reason in {"cancelled", "generation_fenced"}
            else 502
        )
        return JSONResponse(status_code=status, content=result)

    @app.get("/control/work-queue")
    async def control_work_queue_status():
        status = await asyncio.to_thread(queue.status)
        active_scheduler = getattr(app.state, "work_scheduler", None)
        status["scheduler"] = (
            active_scheduler.status() if active_scheduler is not None else None
        )
        status["role"] = (
            "primary" if getattr(app.state, "is_leader", True) else "standby"
        )
        return status

    @app.post("/control/work-queue/requests/{request_id}/cancel")
    async def cancel_work_request(request_id: str, request: Request):
        from fastapi.responses import JSONResponse
        from observability.audit import record_queue_operation

        try:
            await asyncio.to_thread(_require_active_leader)
            changed = await asyncio.to_thread(
                queue.cancel_request, request_id, "operator_cancelled"
            )
        except Exception as exc:
            return JSONResponse(
                status_code=503,
                headers={"Retry-After": "1"},
                content={"ok": False, "error": "Manager is not primary", "detail": str(exc)},
            )
        user = getattr(request.state, "user", None)
        record_queue_operation(
            request_id=request.headers.get("x-request-id", "")[:128]
            or secrets.token_hex(16),
            identity=getattr(user, "user_id", "") or "manager-operator",
            operation="cancel",
            target_id=request_id,
            status=200 if changed else 404,
            outcome="changed" if changed else "not_found",
        )
        log.info(
            "work_queue_operator_cancel",
            request_id=request_id,
            changed=changed,
        )
        return JSONResponse(
            status_code=200 if changed else 404,
            content={"ok": changed, "request_id": request_id},
        )

    @app.post("/control/work-queue/tasks/{task_id}/retry")
    async def retry_work_task(task_id: str, request: Request):
        from fastapi.responses import JSONResponse
        from observability.audit import record_queue_operation

        try:
            await asyncio.to_thread(_require_active_leader)
            changed = await asyncio.to_thread(queue.retry_task, task_id)
        except Exception as exc:
            return JSONResponse(
                status_code=503,
                headers={"Retry-After": "1"},
                content={"ok": False, "error": "Manager is not primary", "detail": str(exc)},
            )
        user = getattr(request.state, "user", None)
        record_queue_operation(
            request_id=request.headers.get("x-request-id", "")[:128]
            or secrets.token_hex(16),
            identity=getattr(user, "user_id", "") or "manager-operator",
            operation="retry",
            target_id=task_id,
            status=200 if changed else 409,
            outcome="changed" if changed else "rejected",
        )
        log.info(
            "work_queue_operator_retry",
            task_id=task_id,
            changed=changed,
        )
        return JSONResponse(
            status_code=200 if changed else 409,
            content={"ok": changed, "task_id": task_id},
        )

    # Phase 5.1: live fleet scaling — grow/shrink the supervised Agent fleet at
    # runtime and persist agent_count. Mutates `supervisors` IN PLACE so the
    # console + gateway see the new fleet immediately.
    _scale_lock = asyncio.Lock()

    async def _scale_fleet(target: int) -> dict:
        target = int(target)
        if target < 1:
            raise ValueError("count must be >= 1")
        _require_active_leader()
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
            leadership_check=_require_active_leader,
            rolling_event=_record_rolling_event,
        ))

    # Phase 5.1: config builder (read current config + push changes) on the Manager,
    # so cluster settings (agent_count etc.) are editable where they apply. Reserved
    # from the gateway catch-all (see enterprise.control.gateway._RESERVED_PREFIXES).
    if config.ENABLE_CONFIG_BUILDER:
        from configbuilder.router import router as config_builder_router
        app.include_router(config_builder_router)

    # The re-identification endpoint is intentionally absent unless both its
    # module profile and restart-bound system setting are active.
    from reidentification.gate import enabled as reidentification_enabled
    if reidentification_enabled():
        from reidentification.router import router as reidentification_router
        app.include_router(reidentification_router)

    # Fleet monitor: the operator console's Monitor tab (and the standalone SPA)
    # live on the Manager, but the live stats are per-Agent — this router scrapes
    # every Agent's /_monitor/api/summary and merges them. Mounted BEFORE the
    # gateway catch-all (which also reserves /_monitor) so it isn't shadowed.
    if config.ENABLE_ADMIN_UI or config.ENABLE_MONITOR:
        from enterprise.control.monitor_proxy import create_monitor_proxy_router
        app.include_router(create_monitor_proxy_router(supervisors, registry, monitor_token))

    # Gateway (LB) MUST be included last: its /{bucket} catch-all would otherwise
    # shadow the control/health routes above.
    if gateway is not None:
        from enterprise.control.gateway import create_gateway_router
        app.include_router(create_gateway_router(gateway))

    return app


app = create_manager_app()

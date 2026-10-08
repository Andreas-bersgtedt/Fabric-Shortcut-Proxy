"""
Agent link — the runtime's connection to the Manager's control plane (Phase 1).

When ``MANAGER_URL`` is set, the Agent registers with the Manager on startup and
then heartbeats on a fixed cadence, reporting the tables/epochs it serves and
receiving any queued Manager→Agent commands (currently just ``drain``). If
``MANAGER_URL`` is empty the link is never created and the server behaves exactly
like the pre‑cluster standalone process.

Transport‑agnostic: it talks through a :class:`~enterprise.control.transport.ControlClient`
(REST today, gRPC later). Robust by design — a Manager outage never crashes the
Agent; heartbeats retry and re‑register on a stale lease.
"""
from __future__ import annotations

import asyncio
import os
import platform
import socket
from typing import Callable

from fabric_shortcut_proxy import config
from enterprise.control.contract import RegisterRequest, HeartbeatRequest, AgentHealth
from enterprise.control.transport import ControlClient, RestControlClient, StaleLeaseError
from fabric_shortcut_proxy.observability.logging import get_logger

try:
    import psutil
except ImportError:  # pragma: no cover - enterprise dependencies include psutil
    psutil = None

log = get_logger(__name__)

_APP_VERSION = "3.0.1"


def _default_agent_id() -> str:
    if config.AGENT_ID:
        return config.AGENT_ID
    try:
        host = socket.gethostname()
    except Exception:
        host = "agent"
    return f"{host}:{config.PORT}"


def _os_name() -> str:
    s = platform.system().lower()
    return {"windows": "windows", "linux": "linux", "darwin": "darwin"}.get(s, s or "unknown")


def _serving_state() -> tuple[list[str], dict[str, int]]:
    """Return (tables, {table: epoch}) currently served, from the state store."""
    try:
        from fabric_shortcut_proxy.iceberg.state_store import get_all_snapshots
        snaps = get_all_snapshots()
        epochs: dict[str, int] = {}
        for s in snaps:
            name = s.table.name
            epochs[name] = max(epochs.get(name, 0), int(getattr(s, "version", 1)))
        return sorted(epochs), epochs
    except Exception:
        return [], {}


def _agent_health() -> AgentHealth:
    """Return current process resource usage for the Manager heartbeat."""
    if psutil is None:
        return AgentHealth()
    process = psutil.Process(os.getpid())
    return AgentHealth(
        cpu_pct=float(process.cpu_percent(interval=None)),
        mem_bytes=int(process.memory_info().rss),
    )


class AgentLink:
    """Registers + heartbeats to the Manager; dispatches inbound commands."""

    def __init__(
        self,
        *,
        client: ControlClient | None = None,
        agent_id: str | None = None,
        heartbeat_ms: int | None = None,
        on_drain: Callable[[], None] | None = None,
    ) -> None:
        self.agent_id = agent_id or _default_agent_id()
        self.heartbeat_ms = heartbeat_ms or config.HEARTBEAT_MS
        self._client = client or RestControlClient(config.MANAGER_URL, agent_id=self.agent_id)
        self._on_drain = on_drain
        self._lease_id: str | None = None
        self._running = False
        self._task: asyncio.Task | None = None
        self._materialize_tasks: dict[str, asyncio.Task] = {}
        self._completed_materialize_deliveries: set[tuple[str, int, str]] = set()

    async def start(self) -> None:
        self._running = True
        await self._register(retries=5)
        self._task = asyncio.create_task(self._loop(), name="agent-heartbeat")
        log.info("agent_link_started", agent_id=self.agent_id, manager=config.MANAGER_URL or "(injected)")

    async def stop(self) -> None:
        self._running = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        for task in self._materialize_tasks.values():
            task.cancel()
        if self._materialize_tasks:
            await asyncio.gather(
                *self._materialize_tasks.values(), return_exceptions=True
            )
        self._materialize_tasks.clear()
        try:
            await self._client.aclose()
        except Exception:
            pass

    async def _register(self, *, retries: int = 1) -> bool:
        req = RegisterRequest(
            agent_id=self.agent_id, host=config.HOST, port=config.PORT,
            os=_os_name(), version=_APP_VERSION,
            capacity_hint=max(
                1,
                int(getattr(config, "SOURCE_MAX_CONCURRENCY", 0) or 1),
            ),
            advertise_host=config.AGENT_ADVERTISE_HOST,
            capabilities=["materializer"],
            pool_id=config.AGENT_POOL_ID,
            location=config.AGENT_LOCATION,
            shard_index=config.AGENT_SHARD_INDEX,
        )
        backoff = 0.5
        for attempt in range(1, retries + 1):
            try:
                resp = await self._client.register(req)
                self._lease_id = resp.lease_id
                if resp.heartbeat_ms:
                    self.heartbeat_ms = resp.heartbeat_ms
                log.info("agent_registered_ok", agent_id=self.agent_id, lease=resp.lease_id[:8])
                return True
            except Exception as e:
                log.warning("agent_register_failed", agent_id=self.agent_id,
                            attempt=attempt, error=str(e))
                if attempt < retries and self._running:
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 5.0)
        return False

    async def _loop(self) -> None:
        interval = self.heartbeat_ms / 1000.0
        try:
            while self._running:
                await asyncio.sleep(interval)
                if not self._running:
                    break
                if self._lease_id is None:
                    await self._register()
                    continue
                try:
                    tables, epochs = _serving_state()
                    health = _agent_health()
                    health.inflight += len(self._materialize_tasks)
                    hb = HeartbeatRequest(
                        agent_id=self.agent_id, lease_id=self._lease_id,
                        health=health, serving_tables=tables, epochs=epochs,
                        active_task_ids=sorted(self._materialize_tasks),
                    )
                    cmds = await self._client.heartbeat(hb)
                    for cmd in cmds:
                        await self._handle_command(cmd)
                except StaleLeaseError:
                    log.info("agent_lease_stale_reregister", agent_id=self.agent_id)
                    self._lease_id = None
                    await self._register()
                except Exception as e:
                    # Manager blip — keep trying, never crash the Agent.
                    log.debug("agent_heartbeat_error", agent_id=self.agent_id, error=str(e))
        except asyncio.CancelledError:
            raise

    async def _run_materialize(self, task) -> None:
        from enterprise.materialize_worker import execute_task

        try:
            result = await execute_task(task, self.agent_id)
            for attempt in range(3):
                try:
                    ack = await self._client.report_task_result(result)
                    log.info(
                        "materialize_task_reported",
                        task_id=task.task_id,
                        state=ack.state,
                        accepted=ack.ok,
                    )
                    return
                except Exception as exc:
                    if attempt == 2:
                        log.error(
                            "materialize_result_report_failed",
                            task_id=task.task_id,
                            error=str(exc),
                        )
                        return
                    await asyncio.sleep(0.5 * (attempt + 1))
        finally:
            delivery = (task.task_id, task.attempt, task.claim_token)
            if len(self._completed_materialize_deliveries) >= 1024:
                self._completed_materialize_deliveries.pop()
            self._completed_materialize_deliveries.add(delivery)
            self._materialize_tasks.pop(task.task_id, None)

    async def _handle_command(self, cmd) -> None:
        if cmd.kind == "drain":
            log.info("agent_drain_requested", agent_id=self.agent_id)
            if self._on_drain is not None:
                try:
                    self._on_drain()
                except Exception:
                    log.exception("agent_drain_handler_error")
            for task in self._materialize_tasks.values():
                task.cancel()
        elif cmd.kind == "materialize" and cmd.materialize is not None:
            task_id = cmd.materialize.task_id
            if not task_id:
                log.warning("materialize_command_missing_task_id")
                return
            if task_id in self._materialize_tasks:
                log.info("materialize_command_duplicate", task_id=task_id)
                return
            delivery = (
                task_id,
                cmd.materialize.attempt,
                cmd.materialize.claim_token,
            )
            if delivery in self._completed_materialize_deliveries:
                log.info("materialize_command_already_completed", task_id=task_id)
                return
            self._materialize_tasks[task_id] = asyncio.create_task(
                self._run_materialize(cmd.materialize),
                name=f"materialize-{task_id[:12]}",
            )
        else:
            log.debug("agent_command_ignored", agent_id=self.agent_id, kind=cmd.kind)

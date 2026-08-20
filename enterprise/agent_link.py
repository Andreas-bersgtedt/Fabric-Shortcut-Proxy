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
import platform
import socket
from typing import Callable

import config
from enterprise.control.contract import RegisterRequest, HeartbeatRequest, AgentHealth
from enterprise.control.transport import ControlClient, RestControlClient, StaleLeaseError
from observability.logging import get_logger

log = get_logger(__name__)

_APP_VERSION = "2.5.2"


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
        from iceberg.state_store import get_all_snapshots
        snaps = get_all_snapshots()
        epochs: dict[str, int] = {}
        for s in snaps:
            name = s.table.name
            epochs[name] = max(epochs.get(name, 0), int(getattr(s, "version", 1)))
        return sorted(epochs), epochs
    except Exception:
        return [], {}


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
        self._client = client or RestControlClient(config.MANAGER_URL)
        self._on_drain = on_drain
        self._lease_id: str | None = None
        self._running = False
        self._task: asyncio.Task | None = None

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
        try:
            await self._client.aclose()
        except Exception:
            pass

    async def _register(self, *, retries: int = 1) -> bool:
        req = RegisterRequest(
            agent_id=self.agent_id, host=config.HOST, port=config.PORT,
            os=_os_name(), version=_APP_VERSION,
            capacity_hint=(0),
            advertise_host=config.AGENT_ADVERTISE_HOST,
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
                    hb = HeartbeatRequest(
                        agent_id=self.agent_id, lease_id=self._lease_id,
                        health=AgentHealth(), serving_tables=tables, epochs=epochs,
                    )
                    cmds = await self._client.heartbeat(hb)
                    for cmd in cmds:
                        self._handle_command(cmd)
                except StaleLeaseError:
                    log.info("agent_lease_stale_reregister", agent_id=self.agent_id)
                    self._lease_id = None
                    await self._register()
                except Exception as e:
                    # Manager blip — keep trying, never crash the Agent.
                    log.debug("agent_heartbeat_error", agent_id=self.agent_id, error=str(e))
        except asyncio.CancelledError:
            raise

    def _handle_command(self, cmd) -> None:
        if cmd.kind == "drain":
            log.info("agent_drain_requested", agent_id=self.agent_id)
            if self._on_drain is not None:
                try:
                    self._on_drain()
                except Exception:
                    log.exception("agent_drain_handler_error")
        else:
            log.debug("agent_command_ignored", agent_id=self.agent_id, kind=cmd.kind)

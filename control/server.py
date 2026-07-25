"""
ControlService — the Manager's :class:`~control.transport.ControlServer` impl.

Phase 1 scope: Agent lifecycle only (register / heartbeat / assignment). Snapshot
distribution and the materialization work‑queue are stubs here — they light up in
Phase 2 (shared store + publish) and Phase 3 (distributed materialization). Keeping
them on the interface now means the transport + callers don't change later.
"""
from __future__ import annotations

from typing import Callable

from control.contract import (
    RegisterRequest, RegisterResponse, HeartbeatRequest, ControlCommand,
    Assignment, SnapshotManifest, TaskResult, Ack,
)
from control.registry import Registry
from observability.logging import get_logger

log = get_logger(__name__)

# A hook the Manager can inject to answer GetSnapshot (Phase 2). Returns the
# published manifest for (table, epoch) or None. ``epoch == 0`` means "current".
SnapshotProvider = Callable[[str, int], "SnapshotManifest | None"]


class ControlService:
    """Implements the ControlServer interface over a :class:`Registry`."""

    def __init__(
        self,
        registry: Registry,
        *,
        tables: list[str] | None = None,
        snapshot_provider: SnapshotProvider | None = None,
    ) -> None:
        self._registry = registry
        self._tables = list(tables or [])
        self._snapshot_provider = snapshot_provider

    # -- Agent lifecycle -----------------------------------------------------

    def register(self, req: RegisterRequest) -> RegisterResponse:
        resp = self._registry.register(req)
        log.info("agent_registered", agent_id=req.agent_id, host=req.host,
                 port=req.port, os=req.os, version=req.version)
        return resp

    def heartbeat(self, req: HeartbeatRequest) -> list[ControlCommand]:
        # May raise LeaseError -> the transport maps it to HTTP 409.
        return self._registry.heartbeat(req)

    def get_assignment(self, agent_id: str) -> Assignment:
        # Phase 1: a single Agent serves every configured table.
        return Assignment(agent_id=agent_id, tables=list(self._tables))

    def get_snapshot(self, table: str, epoch: int) -> SnapshotManifest | None:
        if self._snapshot_provider is None:
            return None            # Phase 2 wires this up
        return self._snapshot_provider(table, epoch)

    def report_task_result(self, res: TaskResult) -> Ack:
        # Phase 3 (materialization work‑queue) consumes these; accept + log for now.
        log.info("task_result", agent_id=res.agent_id, table=res.table,
                 epoch=res.epoch, split_index=res.split_index, ok=res.ok,
                 error=res.error or None)
        return Ack(ok=True)

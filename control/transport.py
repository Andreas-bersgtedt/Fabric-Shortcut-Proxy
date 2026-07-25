"""
Control‑plane transport seam (SCALE_ARCHITECTURE_PLAN.md §14 Phase 1, decision #1).

Open decision #1 is resolved **REST‑first, gRPC‑ready**. Callers depend only on the
:class:`ControlServer` (Manager side) and :class:`ControlClient` (Agent side)
interfaces; the concrete transport is swappable. This module ships the REST
implementation:
  - :func:`create_control_router` adapts any ``ControlServer`` to FastAPI routes.
  - :class:`RestControlClient` is the httpx client the Agent uses.

A future ``GrpcControlClient`` + gRPC server implement the same two interfaces with
no change to the Manager or Agent logic.

Model: **Agent‑pull.** The Agent POSTs heartbeats; the Manager returns any queued
``ControlCommand``s in the response body. Command latency ≤ one heartbeat interval,
which is fine for drain/reload/publish.
"""
from __future__ import annotations

import abc
from typing import Protocol, runtime_checkable

from fastapi import APIRouter, Request, Response
from fastapi.responses import JSONResponse

from control.contract import (
    RegisterRequest, RegisterResponse, HeartbeatRequest, ControlCommand,
    Assignment, SnapshotManifest, TaskResult, Ack,
)
from control.registry import LeaseError

# Path prefix for the REST control plane.
CONTROL_PREFIX = "/control"


class StaleLeaseError(Exception):
    """Client‑side: the Manager rejected our lease (HTTP 409) — re‑register."""


# ---------------------------------------------------------------------------
# Interfaces
# ---------------------------------------------------------------------------

@runtime_checkable
class ControlServer(Protocol):
    """The Manager‑side control surface (implemented by ``control.server.ControlService``)."""

    def register(self, req: RegisterRequest) -> RegisterResponse: ...
    def heartbeat(self, req: HeartbeatRequest) -> list[ControlCommand]: ...  # raises LeaseError
    def get_assignment(self, agent_id: str) -> Assignment: ...
    def get_snapshot(self, table: str, epoch: int) -> SnapshotManifest | None: ...
    def report_task_result(self, res: TaskResult) -> Ack: ...


class ControlClient(abc.ABC):
    """The Agent‑side client. One concrete impl per transport (REST now, gRPC later)."""

    @abc.abstractmethod
    async def register(self, req: RegisterRequest) -> RegisterResponse: ...
    @abc.abstractmethod
    async def heartbeat(self, req: HeartbeatRequest) -> list[ControlCommand]: ...
    @abc.abstractmethod
    async def get_assignment(self, agent_id: str) -> Assignment: ...
    @abc.abstractmethod
    async def get_snapshot(self, table: str, epoch: int = 0) -> SnapshotManifest | None: ...
    @abc.abstractmethod
    async def report_task_result(self, res: TaskResult) -> Ack: ...
    @abc.abstractmethod
    async def aclose(self) -> None: ...


# ---------------------------------------------------------------------------
# REST server adapter (Manager side)
# ---------------------------------------------------------------------------

def create_control_router(server: ControlServer):
    """Return a FastAPI ``APIRouter`` exposing ``server`` over REST under ``/control``."""
    router = APIRouter(prefix=CONTROL_PREFIX, tags=["control"])

    @router.post("/register")
    async def register(request: Request):
        body = await request.json()
        resp = server.register(RegisterRequest.from_dict(body))
        return resp.to_dict()

    @router.post("/heartbeat")
    async def heartbeat(request: Request):
        body = await request.json()
        try:
            cmds = server.heartbeat(HeartbeatRequest.from_dict(body))
        except LeaseError as e:
            return JSONResponse(status_code=409, content={"error": "stale_lease", "detail": str(e)})
        return {"commands": [c.to_dict() for c in cmds]}

    @router.get("/assignment/{agent_id}")
    async def get_assignment(agent_id: str):
        return server.get_assignment(agent_id).to_dict()

    @router.get("/snapshot/{table}")
    async def get_snapshot(table: str, epoch: int = 0):
        snap = server.get_snapshot(table, epoch)
        if snap is None:
            return Response(status_code=404)
        return snap.to_dict()

    @router.post("/task-result")
    async def task_result(request: Request):
        body = await request.json()
        return server.report_task_result(TaskResult.from_dict(body)).to_dict()

    return router


# ---------------------------------------------------------------------------
# REST client (Agent side)
# ---------------------------------------------------------------------------

class RestControlClient(ControlClient):
    """httpx implementation of :class:`ControlClient`.

    ``manager_url`` is the Manager's control base URL, e.g. ``http://127.0.0.1:9200``.
    An optional ``transport`` lets tests bind to an in‑process ASGI app.
    """

    def __init__(self, manager_url: str, *, timeout: float = 10.0, transport=None) -> None:
        import httpx
        self._client = httpx.AsyncClient(
            base_url=manager_url.rstrip("/"), timeout=timeout, transport=transport,
        )

    async def register(self, req: RegisterRequest) -> RegisterResponse:
        r = await self._client.post(f"{CONTROL_PREFIX}/register", json=req.to_dict())
        r.raise_for_status()
        return RegisterResponse.from_dict(r.json())

    async def heartbeat(self, req: HeartbeatRequest) -> list[ControlCommand]:
        r = await self._client.post(f"{CONTROL_PREFIX}/heartbeat", json=req.to_dict())
        if r.status_code == 409:
            raise StaleLeaseError(r.json().get("detail", "stale lease"))
        r.raise_for_status()
        return [ControlCommand.from_dict(c) for c in r.json().get("commands", [])]

    async def get_assignment(self, agent_id: str) -> Assignment:
        r = await self._client.get(f"{CONTROL_PREFIX}/assignment/{agent_id}")
        r.raise_for_status()
        return Assignment.from_dict(r.json())

    async def get_snapshot(self, table: str, epoch: int = 0) -> SnapshotManifest | None:
        r = await self._client.get(f"{CONTROL_PREFIX}/snapshot/{table}", params={"epoch": epoch})
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return SnapshotManifest.from_dict(r.json())

    async def report_task_result(self, res: TaskResult) -> Ack:
        r = await self._client.post(f"{CONTROL_PREFIX}/task-result", json=res.to_dict())
        r.raise_for_status()
        return Ack.from_dict(r.json())

    async def aclose(self) -> None:
        await self._client.aclose()

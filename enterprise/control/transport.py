"""
Control‑plane transport seam (docs/SCALE_ARCHITECTURE_PLAN.md §14 Phase 1, decision #1).

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
import asyncio
import os
from typing import Protocol, runtime_checkable

from fastapi import APIRouter, Request, Response
from fastapi.responses import JSONResponse

from enterprise.control.contract import (
    RegisterRequest, RegisterResponse, HeartbeatRequest, ControlCommand,
    Assignment, SnapshotManifest, TaskResult, Ack,
)
from enterprise.control.registry import LeaseError
from enterprise.control.lease import LeaseStoreError, StaleLeaderError
from fabric_shortcut_proxy.security.agent_auth import AGENT_ID_HEADER, AGENT_TOKEN_HEADER, agent_authentication_required

# Path prefix for the REST control plane.
CONTROL_PREFIX = "/control"


class StaleLeaseError(Exception):
    """Client‑side: the Manager rejected our lease (HTTP 409) — re‑register."""


# ---------------------------------------------------------------------------
# Interfaces
# ---------------------------------------------------------------------------

@runtime_checkable
class ControlServer(Protocol):
    """The Manager‑side control surface (implemented by ``enterprise.control.server.ControlService``)."""

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
        try:
            parsed = RegisterRequest.from_dict(body)
            if parsed.agent_id != getattr(request.state, "agent_id", parsed.agent_id):
                request.state.agent_auth_failure_reason = "identity_mismatch"
                return agent_authentication_required()
            resp = server.register(parsed)
        except (StaleLeaderError, LeaseStoreError) as exc:
            return JSONResponse(
                status_code=503,
                headers={"Retry-After": "1"},
                content={"error": "not_primary", "detail": str(exc)},
            )
        except ValueError as exc:
            return JSONResponse(status_code=400, content={"error": "invalid_registration", "detail": str(exc)})
        return resp.to_dict()

    @router.post("/heartbeat")
    async def heartbeat(request: Request):
        body = await request.json()
        try:
            parsed = HeartbeatRequest.from_dict(body)
            if parsed.agent_id != getattr(request.state, "agent_id", parsed.agent_id):
                request.state.agent_auth_failure_reason = "identity_mismatch"
                return agent_authentication_required()
            cmds = await asyncio.to_thread(
                server.heartbeat, parsed
            )
        except (StaleLeaderError, LeaseStoreError) as exc:
            return JSONResponse(
                status_code=503,
                headers={"Retry-After": "1"},
                content={"error": "not_primary", "detail": str(exc)},
            )
        except LeaseError as e:
            return JSONResponse(status_code=409, content={"error": "stale_lease", "detail": str(e)})
        return {"commands": [c.to_dict() for c in cmds]}

    @router.get("/assignment/{agent_id}")
    async def get_assignment(agent_id: str, request: Request):
        if agent_id != getattr(request.state, "agent_id", agent_id):
            request.state.agent_auth_failure_reason = "identity_mismatch"
            return agent_authentication_required()
        return server.get_assignment(agent_id).to_dict()

    @router.get("/snapshot/{table}")
    async def get_snapshot(table: str, epoch: int = 0):
        snap = await asyncio.to_thread(server.get_snapshot, table, epoch)
        if snap is None:
            return Response(status_code=404)
        return snap.to_dict()

    @router.post("/task-result")
    async def task_result(request: Request):
        body = await request.json()
        parsed = TaskResult.from_dict(body)
        if parsed.agent_id != getattr(request.state, "agent_id", parsed.agent_id):
            request.state.agent_auth_failure_reason = "identity_mismatch"
            return agent_authentication_required()
        try:
            result = await asyncio.to_thread(
                server.report_task_result, parsed
            )
        except (StaleLeaderError, LeaseStoreError) as exc:
            return JSONResponse(
                status_code=503,
                headers={"Retry-After": "1"},
                content={"error": "not_primary", "detail": str(exc)},
            )
        return result.to_dict()

    return router


# ---------------------------------------------------------------------------
# REST client (Agent side)
# ---------------------------------------------------------------------------

class RestControlClient(ControlClient):
    """httpx implementation of :class:`ControlClient`.

    ``manager_url`` is the Manager's control base URL, e.g. ``http://127.0.0.1:9200``.
    An optional ``transport`` lets tests bind to an in‑process ASGI app.
    """

    def __init__(
        self,
        manager_url: str,
        *,
        agent_id: str = "",
        timeout: float = 10.0,
        transport=None,
    ) -> None:
        import httpx
        self._agent_id = agent_id
        self._client = httpx.AsyncClient(
            base_url=manager_url.rstrip("/"), timeout=timeout, transport=transport,
        )

    async def register(self, req: RegisterRequest) -> RegisterResponse:
        self._agent_id = req.agent_id
        r = await self._client.post(f"{CONTROL_PREFIX}/register", json=req.to_dict(),
                                    headers=self._auth_headers(req.agent_id))
        r.raise_for_status()
        return RegisterResponse.from_dict(r.json())

    async def heartbeat(self, req: HeartbeatRequest) -> list[ControlCommand]:
        r = await self._client.post(f"{CONTROL_PREFIX}/heartbeat", json=req.to_dict(),
                                    headers=self._auth_headers(req.agent_id))
        if r.status_code == 409:
            raise StaleLeaseError(r.json().get("detail", "stale lease"))
        r.raise_for_status()
        return [ControlCommand.from_dict(c) for c in r.json().get("commands", [])]

    async def get_assignment(self, agent_id: str) -> Assignment:
        r = await self._client.get(f"{CONTROL_PREFIX}/assignment/{agent_id}",
                                   headers=self._auth_headers(agent_id))
        r.raise_for_status()
        return Assignment.from_dict(r.json())

    async def get_snapshot(self, table: str, epoch: int = 0) -> SnapshotManifest | None:
        r = await self._client.get(f"{CONTROL_PREFIX}/snapshot/{table}",
                                   params={"epoch": epoch}, headers=self._auth_headers())
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return SnapshotManifest.from_dict(r.json())

    async def report_task_result(self, res: TaskResult) -> Ack:
        r = await self._client.post(f"{CONTROL_PREFIX}/task-result", json=res.to_dict(),
                                    headers=self._auth_headers(res.agent_id))
        r.raise_for_status()
        return Ack.from_dict(r.json())

    async def aclose(self) -> None:
        await self._client.aclose()

    def _auth_headers(self, agent_id: str = "") -> dict[str, str]:
        identity = agent_id or self._agent_id
        headers = {AGENT_ID_HEADER: identity} if identity else {}
        token = os.environ.get("AGENT_TOKEN", "")
        if token:
            headers[AGENT_TOKEN_HEADER] = token
        return headers

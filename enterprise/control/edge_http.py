"""Authenticated regional HTTP boundary for edge replay."""

from __future__ import annotations

import asyncio
import json

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from enterprise.control.edge_receiver import EdgeReplayReceiver, MAX_EDGE_REQUEST_BYTES
from enterprise.control.edge_transfer import EdgeTransferConflict
from enterprise.control.federation import CatalogConflict
from fabric_shortcut_proxy.runtime.artifact_store import ObjectConflict


def create_edge_router(receiver: EdgeReplayReceiver) -> APIRouter:
    router = APIRouter()
    lock = asyncio.Lock()

    @router.post("/control/edge/replay")
    async def replay(request: Request):
        if request.url.scheme != "https":
            return JSONResponse(status_code=403, content={"error": "HTTPS required"})
        identity = getattr(request.state, "agent_id", "")
        if getattr(request.state, "agent_auth", "") not in {"identity", "entra", "mtls"}:
            return JSONResponse(status_code=403, content={"error": "identity credential required"})
        if identity not in receiver.bindings:
            return JSONResponse(status_code=403, content={"error": "edge identity is not authorized"})
        try:
            length = int(request.headers.get("content-length", "0"))
        except ValueError:
            return JSONResponse(status_code=400, content={"error": "invalid content length"})
        if length < 0:
            return JSONResponse(status_code=400, content={"error": "invalid content length"})
        if length > MAX_EDGE_REQUEST_BYTES:
            return JSONResponse(status_code=413, content={"error": "edge request too large"})
        async with lock:
            body = bytearray()
            async for chunk in request.stream():
                if len(body) + len(chunk) > MAX_EDGE_REQUEST_BYTES:
                    return JSONResponse(status_code=413, content={"error": "edge request too large"})
                body.extend(chunk)
            try:
                payload = json.loads(body)
                result = await asyncio.to_thread(receiver.receive, identity, payload)
            except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as exc:
                return JSONResponse(status_code=400, content={"error": str(exc)})
            except PermissionError:
                return JSONResponse(status_code=403, content={"error": "edge identity is not authorized"})
            except (CatalogConflict, EdgeTransferConflict, ObjectConflict) as exc:
                return JSONResponse(status_code=409, content={"error": str(exc)})
        return result

    return router

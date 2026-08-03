"""
S3 gateway — the Manager's built-in load balancer (docs/SCALE_ARCHITECTURE_PLAN.md §4.4, Phase 3).

Fronts the Agent fleet with a single Fabric-facing S3 endpoint. It round-robins
GET/HEAD/List requests across the **ready** (heartbeating) Agents from the
:class:`~enterprise.control.registry.Registry` and streams the response back (range-aware).
Agents are interchangeable (they serve from the shared artifact store), so no
sticky sessions are needed.

For a self-contained POC this replaces an external nginx/HAProxy; a production
deployment can still put a real L7 LB in front of the Agents instead (the Agents
are unchanged either way).
"""
from __future__ import annotations

import threading

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, StreamingResponse
from starlette.background import BackgroundTask

from enterprise.control.registry import Registry
from observability.logging import get_logger

log = get_logger(__name__)

# Response headers we must NOT copy verbatim from the upstream Agent.
_HOP_BY_HOP = {"transfer-encoding", "connection", "keep-alive", "content-encoding"}

# Paths that belong to the Manager itself and must NEVER be proxied to an Agent.
# The gateway's ``/{bucket}`` catch-all would otherwise forward e.g. /_manager
# (when the console is disabled) or /favicon.ico to an Agent, whose S3 SigV4 auth
# rejects them with a confusing ``AccessDenied`` 403. None are valid S3 buckets
# (bucket names can't start with ``_``), so reserving them is safe.
_RESERVED_PREFIXES = ("/_manager", "/_config", "/_monitor", "/control", "/healthz", "/readyz", "/agents")


def _is_reserved(path: str) -> bool:
    if path == "/favicon.ico":
        return True
    return any(path == p or path.startswith(p + "/") for p in _RESERVED_PREFIXES)


def _dial_host(host: str) -> str:
    """A wildcard bind address isn't dial-able; use loopback on a single box.
    Agents on other hosts advertise a routable address (AGENT_ADVERTISE_HOST),
    which the registry returns as ``host``, so this remap only hits the same-box
    wildcard case."""
    return "127.0.0.1" if host in ("0.0.0.0", "::", "") else host


class Gateway:
    """Round-robin reverse proxy over the live Agent fleet."""

    def __init__(self, registry: Registry, *, timeout: float = 60.0, transport=None) -> None:
        self._registry = registry
        self._timeout = timeout
        self._transport = transport
        self._client = None            # lazily built on first proxy
        self._rr = 0
        self._lock = threading.Lock()

    def _get_client(self):
        if self._client is None:
            import httpx
            self._client = httpx.AsyncClient(timeout=self._timeout, transport=self._transport)
        return self._client

    def _targets(self) -> list[tuple[str, int]]:
        dead = set(self._registry.dead_agents())
        out: list[tuple[str, int]] = []
        for a in self._registry.list_public():
            if a["agent_id"] in dead:
                continue
            out.append((_dial_host(a["host"]), int(a["port"])))
        return out

    def pick(self) -> tuple[str, int] | None:
        targets = self._targets()
        if not targets:
            return None
        with self._lock:
            i = self._rr % len(targets)
            self._rr = (self._rr + 1) % max(1, len(targets))
        return targets[i]

    async def proxy(self, request: Request):
        path = request.url.path
        if _is_reserved(path):
            hint = (
                "the operator console is disabled — start the Manager with "
                "ENABLE_ADMIN_UI=1 (Manager.ps1 -AdminUi)"
                if path == "/_manager" or path.startswith("/_manager/")
                else "this is a Manager control endpoint, not an S3 object"
            )
            return JSONResponse(status_code=404,
                                content={"error": "not_found", "path": path, "detail": hint})
        target = self.pick()
        if target is None:
            return JSONResponse(status_code=503, content={"error": "no_ready_agent"})
        host, port = target
        url = f"http://{host}:{port}{request.url.path}"
        if request.url.query:
            url += "?" + request.url.query
        fwd_headers = {
            k: v for k, v in request.headers.items()
            if k.lower() not in ("host", "content-length")
        }
        body = None
        if request.method not in ("GET", "HEAD"):
            body = await request.body()
        try:
            client = self._get_client()
            req = client.build_request(request.method, url, headers=fwd_headers, content=body)
            resp = await client.send(req, stream=True)
        except Exception as exc:
            log.warning("gateway_upstream_error", target=f"{host}:{port}", error=str(exc))
            return JSONResponse(status_code=502, content={"error": "bad_gateway", "detail": str(exc)})
        resp_headers = {
            k: v for k, v in resp.headers.items() if k.lower() not in _HOP_BY_HOP
        }
        return StreamingResponse(
            resp.aiter_raw(),
            status_code=resp.status_code,
            headers=resp_headers,
            background=BackgroundTask(resp.aclose),
        )

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()


def create_gateway_router(gateway: Gateway) -> APIRouter:
    """Return a router that proxies S3 GET/HEAD to the fleet. Mount it LAST on the
    Manager app so its ``/{bucket}`` catch-all never shadows the control routes."""
    router = APIRouter(tags=["gateway"])

    @router.api_route("/", methods=["GET", "HEAD"])
    async def _root(request: Request):
        return await gateway.proxy(request)

    @router.api_route("/{bucket}", methods=["GET", "HEAD"])
    async def _bucket(bucket: str, request: Request):
        return await gateway.proxy(request)

    @router.api_route("/{bucket}/{key:path}", methods=["GET", "HEAD"])
    async def _object(bucket: str, key: str, request: Request):
        return await gateway.proxy(request)

    return router

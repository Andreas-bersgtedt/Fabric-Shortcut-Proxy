"""
Manager-side ``/_monitor`` — the operator console's Monitor tab, fleet-aware.

The Agents own the live serving stats (each sees only the requests the gateway
routed to it), so the Manager can't compute them locally. This router scrapes
every live Agent's ``/_monitor/api/summary`` and merges them
(:func:`enterprise.control.monitor_agg.merge_summaries`) into one fleet view, and fans a
reset out to all Agents. It also serves the standalone monitor SPA so
``/_monitor/`` works on the control port too.

Mounted only when the operator console (or monitor) is enabled; requires Agents
to expose their monitor API (the Manager sets ``ENABLE_MONITOR=1`` on supervised
Agents — see :mod:`enterprise.control.manager_app`).
"""
from __future__ import annotations

import asyncio
import pathlib

import httpx
from fastapi import APIRouter
from fastapi.responses import HTMLResponse, JSONResponse

import config
from enterprise.control.monitor_agg import merge_summaries
from observability.logging import get_logger

log = get_logger(__name__)

# monitor/index.html lives in the Lite core (repo root), three levels up from
# <repo>/enterprise/control/monitor_proxy.py.
_HTML_PATH = pathlib.Path(__file__).resolve().parent.parent.parent / "monitor" / "index.html"


def _dial_host(host: str) -> str:
    return "127.0.0.1" if host in ("0.0.0.0", "::", "") else host


def _agent_base_urls(supervisors) -> list[str]:
    """Base URL of every alive supervised Agent (host + its data-plane PORT)."""
    host = _dial_host(config.HOST)
    urls: list[str] = []
    for sup in supervisors:
        if not getattr(sup, "is_alive", False):
            continue
        env = getattr(sup, "env", None) or {}
        port = env.get("PORT")
        if str(port).isdigit():
            urls.append(f"http://{host}:{int(port)}")
    return urls


def create_monitor_proxy_router(supervisors) -> APIRouter:
    router = APIRouter(prefix="/_monitor")

    async def _scrape(client: httpx.AsyncClient, base: str, path: str, method: str = "GET"):
        try:
            r = await client.request(method, base + path)
            if r.status_code == 200:
                return r.json() if method == "GET" else True
        except Exception as exc:  # noqa: BLE001 - a down/slow Agent must not fail the console
            log.warning("monitor_scrape_failed", agent=base, error=str(exc))
        return None

    @router.get("")
    @router.get("/")
    async def index() -> HTMLResponse:
        return HTMLResponse(_HTML_PATH.read_text(encoding="utf-8"),
                            headers={"Cache-Control": "no-store, max-age=0"})

    @router.get("/api/summary")
    async def summary() -> JSONResponse:
        bases = _agent_base_urls(supervisors)
        summaries = []
        if bases:
            async with httpx.AsyncClient(timeout=5.0) as client:
                results = await asyncio.gather(
                    *(_scrape(client, b, "/_monitor/api/summary") for b in bases)
                )
            summaries = [r for r in results if isinstance(r, dict)]
        merged = merge_summaries(summaries)
        merged["agents_total"] = len(bases)
        return JSONResponse(merged)

    @router.post("/api/reset")
    async def reset() -> JSONResponse:
        bases = _agent_base_urls(supervisors)
        n = 0
        if bases:
            async with httpx.AsyncClient(timeout=5.0) as client:
                results = await asyncio.gather(
                    *(_scrape(client, b, "/_monitor/api/reset", method="POST") for b in bases),
                    return_exceptions=True,
                )
            n = sum(1 for r in results if r is True)
        return JSONResponse({"ok": True, "reset_agents": n})

    return router

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
from urllib.parse import quote

import httpx
from fastapi import APIRouter
from fastapi.responses import HTMLResponse, JSONResponse

import config
from enterprise.control.monitor_agg import merge_summaries
from observability.logbuffer import get_buffer
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


def create_monitor_proxy_router(supervisors, registry=None, monitor_token: str = "") -> APIRouter:
    router = APIRouter(prefix="/_monitor")

    async def _scrape(client: httpx.AsyncClient, base: str, path: str, method: str = "GET"):
        try:
            headers = {"X-FSP-Internal-Monitor": monitor_token} if monitor_token else {}
            r = await client.request(method, base + path, headers=headers)
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

    @router.get("/api/health")
    async def health() -> JSONResponse:
        from enterprise.control.cluster_health import aggregate_health
        return JSONResponse(aggregate_health(registry, supervisors))

    @router.get("/api/health/history")
    async def health_history(limit: int = 60) -> JSONResponse:
        from enterprise.control.cluster_health import health_history
        return JSONResponse({"history": health_history(limit)})

    @router.get("/api/open-mirror")
    async def open_mirror(include_landing_zone_count: bool = False) -> JSONResponse:
        """Return Manager-owned Open Mirror status and publishing statistics.

        The Open Mirror scheduler runs in the Manager process, so its configured
        targets and state are authoritative there. Agent data is retained as a
        fallback for deployments where publishing is delegated to Agents.
        """
        from monitor.router import open_mirror_summary

        local = await open_mirror_summary(
            include_landing_zone_count=include_landing_zone_count
        )
        if local.get("targets"):
            local["agents_total"] = len(_agent_base_urls(supervisors))
            return JSONResponse(local)

        bases = _agent_base_urls(supervisors)
        payloads = []
        if bases:
            async with httpx.AsyncClient(timeout=5.0) as client:
                results = await asyncio.gather(
                    *(_scrape(
                        client, b,
                        "/_monitor/api/open-mirror"
                        + ("?include_landing_zone_count=true" if include_landing_zone_count else ""),
                    ) for b in bases)
                )
            payloads = [r for r in results if isinstance(r, dict)]

        by_id: dict[str, dict] = {}
        for payload in payloads:
            for target in payload.get("targets", []) or []:
                target_id = str(target.get("id") or "")
                if not target_id:
                    continue
                current = by_id.get(target_id)
                if current is None:
                    current = dict(target)
                    current["agent_count"] = 0
                    current["published_rows"] = 0
                    current["published_tables"] = 0
                    current["last_published_at"] = None
                    by_id[target_id] = current
                current["agent_count"] += 1
                current["published_rows"] += int(target.get("published_rows", 0) or 0)
                current["published_tables"] = max(
                    current["published_tables"], int(target.get("published_tables", 0) or 0)
                )
                candidate_time = target.get("last_published_at")
                if candidate_time and (
                    current["last_published_at"] is None
                    or candidate_time > current["last_published_at"]
                ):
                    current["last_published_at"] = candidate_time

        targets = list(by_id.values())
        tables = sum(len(target.get("tables", []) or []) for target in targets)
        initialized = sum(
            1 for target in targets for table in target.get("tables", []) or []
            if table.get("initialized")
        )
        pending = sum(
            1 for target in targets for table in target.get("tables", []) or []
            if table.get("pending")
        )
        return JSONResponse({
            "generated_at": payloads[0].get("generated_at") if payloads else None,
            "agents_total": len(bases),
            "targets": targets,
            "totals": {
                "targets": len(targets),
                "enabled_targets": sum(1 for target in targets if target.get("enabled")),
                "tables": tables,
                "initialized_tables": initialized,
                "pending_tables": pending,
                "published_rows": sum(target["published_rows"] for target in targets),
                "last_batch_rows": sum(
                    sum(table.get("last_batch_rows", 0) or 0
                        for table in target.get("tables", []) or [])
                    for target in targets
                ),
                "last_published_at": max(
                    (target["last_published_at"] for target in targets
                     if target["last_published_at"]),
                    default=None,
                ),
            },
        })

    @router.post("/api/open-mirror/cleanup")
    async def open_mirror_cleanup(payload: dict) -> JSONResponse:
        from monitor.router import open_mirror_cleanup as run_cleanup

        target_id = str(payload.get("target_id") or "").strip()
        if not target_id:
            return JSONResponse({"error": "target_id is required"}, status_code=400)
        try:
            result = await run_cleanup(
                target_id,
                table_name=(str(payload["table"]) if payload.get("table") else None),
                execute=bool(payload.get("execute", False)),
            )
        except (OSError, RuntimeError, ValueError) as exc:
            log.warning("open_mirror_cleanup_failed", target=target_id, error=str(exc))
            return JSONResponse({"error": str(exc)}, status_code=400)
        log.info("open_mirror_cleanup", target=target_id, execute=result["execute"],
                 deleted=len(result["deleted"]))
        return JSONResponse(result)

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

    @router.get("/api/logs")
    async def logs(limit: int = 1000, q: str | None = None) -> JSONResponse:
        """Fleet log tail: the Manager's own lines plus each live Agent's buffer.

        Every line is tagged with its source (``[manager]`` / ``[agent <port>]``)
        so the operator can tell which process emitted it. Read-only.
        """
        limit = max(1, min(limit, 1000))
        query = (q or "").strip() or None
        tagged: list[str] = [f"[manager] {ln}" for ln in get_buffer().tail(query=query)]

        bases = _agent_base_urls(supervisors)
        if bases:
            path = "/_monitor/api/logs?limit=1000"
            if query:
                path += "&q=" + quote(query)
            async with httpx.AsyncClient(timeout=5.0) as client:
                results = await asyncio.gather(*(_scrape(client, b, path) for b in bases))
            for base, res in zip(bases, results):
                if isinstance(res, dict):
                    port = base.rsplit(":", 1)[-1]
                    tagged.extend(f"[agent {port}] {ln}" for ln in res.get("lines", []))

        tagged = tagged[-limit:]
        return JSONResponse({
            "lines": tagged,
            "returned": len(tagged),
            "total": len(tagged),
            "capacity": 1000,
            "query": query or "",
            "agents_total": len(bases),
        })

    return router

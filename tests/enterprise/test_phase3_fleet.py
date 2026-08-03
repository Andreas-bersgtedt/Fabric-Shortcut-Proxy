"""Phase 3: fleet gateway (round-robin S3 LB) + multi-Agent supervisor wiring."""
from __future__ import annotations

import os

os.environ.setdefault("DB_URL", "sqlite+aiosqlite:///:memory:")
os.environ.setdefault("S3_BUCKET", "test-bucket")

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse

import config
from enterprise.control.registry import Registry
from enterprise.control.contract import RegisterRequest
from enterprise.control.gateway import Gateway, create_gateway_router, _dial_host


def _register(reg: Registry, agent_id: str, port: int, host: str = "0.0.0.0") -> None:
    reg.register(RegisterRequest(agent_id=agent_id, host=host, port=port, os="linux", version="v"))


# ---------------------------------------------------------------------------
# Target selection (pure)
# ---------------------------------------------------------------------------

def test_dial_host_maps_wildcard_to_loopback():
    assert _dial_host("0.0.0.0") == "127.0.0.1"
    assert _dial_host("::") == "127.0.0.1"
    assert _dial_host("") == "127.0.0.1"
    assert _dial_host("10.0.0.5") == "10.0.0.5"


def test_pick_round_robins_and_excludes_dead():
    reg = Registry(heartbeat_ms=1000, miss_limit=3)
    _register(reg, "a1", 9000)
    _register(reg, "a2", 9001)
    gw = Gateway(reg)
    picks = [gw.pick() for _ in range(4)]
    assert set(picks) == {("127.0.0.1", 9000), ("127.0.0.1", 9001)}
    assert picks[0] != picks[1]                         # alternates
    # a2 misses heartbeats -> excluded from rotation
    reg.get("a2").last_seen -= 100
    assert {gw.pick() for _ in range(4)} == {("127.0.0.1", 9000)}


def test_pick_none_when_empty():
    assert Gateway(Registry()).pick() is None


# ---------------------------------------------------------------------------
# Reverse proxy (in-process ASGI upstream)
# ---------------------------------------------------------------------------

def _fake_agent() -> FastAPI:
    app = FastAPI()

    @app.api_route("/{bucket}/{key:path}", methods=["GET", "HEAD"])
    async def obj(bucket: str, key: str, request: Request):
        return PlainTextResponse(
            f"AGENT:{key}",
            headers={"x-agent": "1", "accept-ranges": "bytes",
                     "range-seen": request.headers.get("range", "none")},
        )

    return app


async def test_gateway_proxies_get_and_forwards_headers():
    reg = Registry()
    _register(reg, "a1", 9000)
    gw = Gateway(reg, transport=httpx.ASGITransport(app=_fake_agent()))
    mgr = FastAPI()
    mgr.include_router(create_gateway_router(gw))
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=mgr),
                                     base_url="http://mgr") as c:
            r = await c.get("/fabric-iceberg-poc/warehouse/db/sales/data/x.parquet",
                            headers={"range": "bytes=0-3"})
            assert r.status_code == 200
            assert r.text == "AGENT:warehouse/db/sales/data/x.parquet"
            assert r.headers.get("x-agent") == "1"
            assert r.headers.get("range-seen") == "bytes=0-3"   # range forwarded
    finally:
        await gw.aclose()


async def test_gateway_503_when_no_ready_agent():
    gw = Gateway(Registry(), transport=httpx.ASGITransport(app=FastAPI()))
    mgr = FastAPI()
    mgr.include_router(create_gateway_router(gw))
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=mgr),
                                     base_url="http://mgr") as c:
            r = await c.get("/bucket/key")
            assert r.status_code == 503
    finally:
        await gw.aclose()


async def test_gateway_reserves_manager_namespace():
    # The gateway must not proxy the Manager's own paths to an Agent (which would
    # reject them with a confusing SigV4 403). It returns a clean 404 + hint.
    reg = Registry()
    _register(reg, "a1", 9000)
    gw = Gateway(reg, transport=httpx.ASGITransport(app=_fake_agent()))
    mgr = FastAPI()
    mgr.include_router(create_gateway_router(gw))
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=mgr),
                                     base_url="http://mgr") as c:
            r = await c.get("/_manager")
            assert r.status_code == 404
            assert "ENABLE_ADMIN_UI" in r.json()["detail"]
            assert (await c.get("/favicon.ico")).status_code == 404
            # a real bucket path still proxies through
            assert (await c.get("/fabric/data/x.parquet")).status_code == 200
    finally:
        await gw.aclose()


# ---------------------------------------------------------------------------
# Multi-Agent supervisor wiring (no processes spawned)
# ---------------------------------------------------------------------------

def test_build_supervisors_assigns_ports_and_shards(monkeypatch):
    from enterprise.control import manager_app
    monkeypatch.setattr(config, "AGENT_COUNT", 3, raising=False)
    monkeypatch.setattr(config, "PORT", 9000, raising=False)
    sups = manager_app._build_supervisors()
    assert [s.name for s in sups] == ["agent-1", "agent-2", "agent-3"]
    envs = [s.env for s in sups]
    assert [e["PORT"] for e in envs] == ["9000", "9001", "9002"]
    assert [e["AGENT_SHARD_INDEX"] for e in envs] == ["0", "1", "2"]
    assert all(e["AGENT_SHARD_COUNT"] == "3" for e in envs)
    assert all("MANAGER_URL" in e for e in envs)

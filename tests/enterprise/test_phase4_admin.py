"""Phase 4: /_manager operator console — fleet snapshot + start/stop/restart/drain API."""
from __future__ import annotations

import os

os.environ.setdefault("DB_URL", "sqlite+aiosqlite:///:memory:")
os.environ.setdefault("S3_BUCKET", "test-bucket")

import httpx
from fastapi import FastAPI

from enterprise.control.admin import create_admin_router, fleet_snapshot
from enterprise.control.contract import RegisterRequest
from enterprise.control.registry import Registry


class FakeSupervisor:
    """A supervisor stand-in that records async calls without spawning processes."""

    def __init__(self, name, port, shard_index, shard_count, *,
                 alive=True, crash_looped=False, restart_count=0):
        self.name = name
        self.env = {
            "PORT": str(port),
            "AGENT_SHARD_INDEX": str(shard_index),
            "AGENT_SHARD_COUNT": str(shard_count),
        }
        self._alive = alive
        self.crash_looped = crash_looped
        self.restart_count = restart_count
        self.pid = 1000 if alive else None
        self.calls: list[str] = []
        # Memory monitoring fields the admin fleet snapshot reads.
        self.rss_mb = 0.0
        self.avg_rss_mb = 0.0
        self.peak_rss_mb = 0.0
        self.memory_alert_threshold_mb = 0
        self.memory_restart_threshold_mb = 0

    @property
    def is_alive(self):
        return self._alive

    @property
    def is_running(self):
        return self._alive

    async def start(self):
        self.calls.append("start")
        self._alive = True
        self.pid = 1000

    async def stop(self):
        self.calls.append("stop")
        self._alive = False
        self.pid = None


def _registry_with(*agent_ports, host="0.0.0.0") -> Registry:
    reg = Registry(heartbeat_ms=1000, miss_limit=3)
    for agent_id, port in agent_ports:
        reg.register(RegisterRequest(agent_id=agent_id, host=host, port=port, os="linux", version="v"))
    return reg


def _client(app: FastAPI) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://mgr")


def _app(reg, sups, *, gateway=None, token="") -> FastAPI:
    app = FastAPI()
    app.include_router(create_admin_router(reg, sups, gateway=gateway, token=token))
    return app


# ---------------------------------------------------------------------------
# Fleet snapshot (pure)
# ---------------------------------------------------------------------------

def test_fleet_snapshot_combines_supervisor_and_registry():
    reg = _registry_with(("agent-1", 9100), ("agent-2", 9101))
    sups = [
        FakeSupervisor("agent-1", 9100, 0, 2),
        FakeSupervisor("agent-2", 9101, 1, 2, alive=False),
    ]
    snap = fleet_snapshot(reg, sups, gateway=object(), token_required=True)

    assert snap["agents_total"] == 2
    assert snap["agents_alive"] == 1                 # agent-2 process down
    assert snap["agents_registered"] == 2
    assert snap["gateway_enabled"] is True
    assert snap["gateway_targets"] == 2              # both still registered/alive by heartbeat
    assert snap["admin_token_required"] is True
    a1 = next(a for a in snap["agents"] if a["name"] == "agent-1")
    assert a1["port"] == 9100 and a1["shard_index"] == 0 and a1["shard_count"] == 2
    assert a1["registered"] is True and a1["process_alive"] is True
    assert a1["runtime"] == "python" and a1["edition"] == "enterprise" and a1["version"] == "v"
    a2 = next(a for a in snap["agents"] if a["name"] == "agent-2")
    assert a2["process_alive"] is False


def test_fleet_snapshot_gateway_off():
    reg = _registry_with(("agent-1", 9100))
    snap = fleet_snapshot(reg, [FakeSupervisor("agent-1", 9100, 0, 1)])
    assert snap["gateway_enabled"] is False
    assert snap["gateway_targets"] == 0
    assert snap["ready"] is True


def test_fleet_snapshot_includes_external_registered_agent():
    reg = _registry_with(("fsp-materializer-smoke", 9000), host="192.0.2.10")
    rec = reg.get("fsp-materializer-smoke")
    rec.serving_tables = ["SO_Header"]
    rec.epochs = {"SO_Header": 1}
    rec.health.mem_bytes = 650313728

    snap = fleet_snapshot(reg, [])

    assert snap["ready"] is True
    assert snap["agents_total"] == 1
    assert snap["agents_alive"] == 1
    assert snap["agents_registered_alive"] == 1
    agent = snap["agents"][0]
    assert agent["name"] == "fsp-materializer-smoke"
    assert agent["supervised"] is False
    assert agent["process_alive"] is True
    assert agent["serving_tables"] == ["SO_Header"]
    assert agent["runtime"] == "python"
    assert agent["edition"] == "enterprise"
    assert agent["os"] == "linux"
    assert agent["version"] == "v"


# ---------------------------------------------------------------------------
# Console page + JSON API
# ---------------------------------------------------------------------------

async def test_manager_page_and_fleet_api_served():
    reg = _registry_with(("agent-1", 9100))
    app = _app(reg, [FakeSupervisor("agent-1", 9100, 0, 1)])
    async with _client(app) as c:
        page = await c.get("/_manager")
        assert page.status_code == 200
        assert "text/html" in page.headers["content-type"]
        assert "Manager" in page.text
        api = await c.get("/_manager/api/fleet")
        assert api.status_code == 200
        assert api.json()["agents"][0]["name"] == "agent-1"


async def test_stop_start_restart_call_supervisor():
    reg = _registry_with(("agent-1", 9100))
    sup = FakeSupervisor("agent-1", 9100, 0, 1)
    app = _app(reg, [sup])
    async with _client(app) as c:
        r = await c.post("/_manager/api/agents/agent-1/stop")
        assert r.status_code == 200 and r.json()["ok"] is True
        assert sup.calls == ["stop"] and sup.is_alive is False

        r = await c.post("/_manager/api/agents/agent-1/start")   # revive stopped agent
        assert r.status_code == 200
        assert sup.calls == ["stop", "stop", "start"] and sup.is_alive is True

        sup.calls.clear()
        r = await c.post("/_manager/api/agents/agent-1/restart")
        assert r.status_code == 200
        assert sup.calls == ["stop", "start"]


async def test_start_noop_when_already_alive():
    reg = _registry_with(("agent-1", 9100))
    sup = FakeSupervisor("agent-1", 9100, 0, 1, alive=True)
    app = _app(reg, [sup])
    async with _client(app) as c:
        r = await c.post("/_manager/api/agents/agent-1/start")
        assert r.status_code == 200
        assert sup.calls == []                         # already alive -> no respawn


async def test_drain_queues_command():
    reg = _registry_with(("agent-1", 9100))
    sups = [FakeSupervisor("agent-1", 9100, 0, 1), FakeSupervisor("agent-2", 9101, 0, 1)]
    app = _app(reg, sups)
    async with _client(app) as c:
        r = await c.post("/_manager/api/agents/agent-1/drain")
        assert r.status_code == 200 and r.json()["ok"] is True
        rec = reg.get("agent-1")
        assert len(rec.commands) == 1 and rec.commands[0].kind == "drain"

        # agent-2 is supervised but not registered -> nothing to queue
        r = await c.post("/_manager/api/agents/agent-2/drain")
        assert r.status_code == 200 and r.json()["ok"] is False


async def test_drain_external_registered_agent_without_supervisor():
    reg = _registry_with(("fsp-materializer-smoke", 9000), host="192.0.2.10")
    app = _app(reg, [])
    async with _client(app) as c:
        r = await c.post("/_manager/api/agents/fsp-materializer-smoke/drain")
        assert r.status_code == 200 and r.json()["ok"] is True
        rec = reg.get("fsp-materializer-smoke")
        assert len(rec.commands) == 1 and rec.commands[0].kind == "drain"


async def test_forget_removes_only_dead_registered_agent(monkeypatch):
    reg = _registry_with(("dead-agent", 9000), ("live-agent", 9001), host="192.0.2.10")
    monkeypatch.setattr(reg, "is_alive", lambda agent_id: agent_id == "live-agent")
    app = _app(reg, [])
    async with _client(app) as c:
        live = await c.delete("/_manager/api/agents/live-agent")
        assert live.status_code == 409
        missing = await c.delete("/_manager/api/agents/missing-agent")
        assert missing.status_code == 404
        dead = await c.delete("/_manager/api/agents/dead-agent")
        assert dead.status_code == 200 and dead.json()["ok"] is True
        assert reg.get("dead-agent") is None
        assert reg.get("live-agent") is not None


async def test_unknown_agent_and_action():
    reg = _registry_with(("agent-1", 9100))
    app = _app(reg, [FakeSupervisor("agent-1", 9100, 0, 1)])
    async with _client(app) as c:
        assert (await c.post("/_manager/api/agents/nope/start")).status_code == 404
        assert (await c.post("/_manager/api/agents/agent-1/frobnicate")).status_code == 400


# ---------------------------------------------------------------------------
# Admin token guard
# ---------------------------------------------------------------------------

async def test_token_guards_mutations_but_not_reads():
    reg = _registry_with(("agent-1", 9100))
    sup = FakeSupervisor("agent-1", 9100, 0, 1)
    app = _app(reg, [sup], token="s3cret")
    async with _client(app) as c:
        # reads stay open
        assert (await c.get("/_manager/api/fleet")).status_code == 200
        assert "/_monitor/api/open-mirror" in (await c.get("/_manager")).text
        # mutation without token -> 401
        assert (await c.post("/_manager/api/agents/agent-1/stop")).status_code == 401
        assert sup.calls == []
        # header token accepted
        r = await c.post("/_manager/api/agents/agent-1/stop", headers={"X-Admin-Token": "s3cret"})
        assert r.status_code == 200 and sup.calls == ["stop"]
        # query token accepted
        r = await c.post("/_manager/api/agents/agent-1/start?token=s3cret")
        assert r.status_code == 200


async def test_fleet_reports_token_required_flag():
    reg = _registry_with(("agent-1", 9100))
    app = _app(reg, [FakeSupervisor("agent-1", 9100, 0, 1)], token="x")
    async with _client(app) as c:
        assert (await c.get("/_manager/api/fleet")).json()["admin_token_required"] is True

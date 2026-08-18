"""Fabric workspace / mirrored-database browse — helper + endpoint tests.

The Fabric REST calls are stubbed (no network, no Azure SDK): we patch the token
and the paged GET, then verify mapping, the mirrored-database fallback, the
landing-zone URL, and that the router surfaces results and errors.
"""
from __future__ import annotations

import os

os.environ.setdefault("DB_URL", "sqlite+aiosqlite:///:memory:")

import httpx
import pytest
from fastapi import FastAPI

import open_mirror.fabric_api as fab
from configbuilder.router import router as cb_router
from open_mirror import scheduler, target_from_dict

# --- helper --------------------------------------------------------------

def test_landing_zone_url():
    assert fab.landing_zone_url("ws", "db") == \
        "https://onelake.dfs.fabric.microsoft.com/ws/db/Files/LandingZone"


def test_request_uses_bearer_token(monkeypatch):
    seen = {}

    class Response:
        status_code = 202
        content = b""
        headers = {}

    class Client:
        def __init__(self, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def request(self, method, url, headers):
            seen.update(headers)
            return Response()

    monkeypatch.setattr(httpx, "Client", Client)

    assert fab._request("POST", "status", "unit-token") == {}
    assert seen["Authorization"] == "Bearer" + " " + "unit-token"


def test_list_workspaces_maps_and_skips_idless(monkeypatch):
    monkeypatch.setattr(fab, "_token", lambda credential=None: "t")
    monkeypatch.setattr(fab, "_get_all", lambda path, token, params=None: [
        {"id": "w1", "displayName": "Sales"},
        {"id": "w2", "name": "Ops"},
        {"displayName": "no id"},
    ])
    assert fab.list_workspaces() == [{"id": "w1", "name": "Sales"}, {"id": "w2", "name": "Ops"}]


def test_list_mirrored_databases_maps_landing(monkeypatch):
    monkeypatch.setattr(fab, "_token", lambda credential=None: "t")
    monkeypatch.setattr(fab, "_get_all", lambda path, token, params=None: [
        {"id": "db1", "displayName": "FSPOpenMirror"},
    ])
    dbs = fab.list_mirrored_databases("ws1")
    assert dbs[0]["name"] == "FSPOpenMirror"
    assert dbs[0]["workspace_id"] == "ws1"
    assert dbs[0]["landing_zone_root"].endswith("/ws1/db1/Files/LandingZone")


def test_list_mirrored_databases_falls_back_to_items(monkeypatch):
    monkeypatch.setattr(fab, "_token", lambda credential=None: "t")
    seen = []

    def fake_get_all(path, token, params=None):
        seen.append(path)
        if path.endswith("/mirroredDatabases"):
            raise fab.FabricApiError("not available")
        return [{"id": "db2", "displayName": "X"}]

    monkeypatch.setattr(fab, "_get_all", fake_get_all)
    dbs = fab.list_mirrored_databases("ws1")
    assert dbs[0]["id"] == "db2"
    assert any(p.endswith("/items") for p in seen)


# --- endpoints -----------------------------------------------------------

@pytest.fixture
def app():
    a = FastAPI()
    a.include_router(cb_router)
    return a


def _client(app):
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t")


async def test_workspaces_endpoint(app, monkeypatch):
    monkeypatch.setattr(fab, "list_workspaces", lambda credential=None: [{"id": "w1", "name": "Sales"}])
    async with _client(app) as c:
        r = await c.get("/_config/api/open-mirror/fabric/workspaces")
    assert r.status_code == 200
    assert r.json()["workspaces"][0]["id"] == "w1"


async def test_mirrored_dbs_endpoint(app, monkeypatch):
    monkeypatch.setattr(
        fab, "list_mirrored_databases",
        lambda wsid, credential=None: [{
            "id": "db1", "name": "M", "workspace_id": wsid,
            "landing_zone_root": fab.landing_zone_url(wsid, "db1"),
        }],
    )
    async with _client(app) as c:
        r = await c.get("/_config/api/open-mirror/fabric/workspaces/ws1/mirrored-databases")
    assert r.status_code == 200
    body = r.json()
    assert body["mirrored_databases"][0]["landing_zone_root"].endswith("/ws1/db1/Files/LandingZone")


async def test_workspaces_endpoint_surfaces_error(app, monkeypatch):
    def boom(credential=None):
        raise fab.FabricApiError("Fabric API returned 401 Unauthorized")
    monkeypatch.setattr(fab, "list_workspaces", boom)
    async with _client(app) as c:
        r = await c.get("/_config/api/open-mirror/fabric/workspaces")
    assert r.status_code == 400
    assert "401" in r.json()["error"]


def _healing_target(tmp_path):
    return target_from_dict({
        "id": "fabric-db",
        "connection": "default",
        "landing_zone_root": str(tmp_path),
        "workspace_id": "ws",
        "mirrored_database_id": "db",
        "self_healing": True,
        "tables": [],
    })


async def test_running_preflight_does_not_start(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(
        scheduler, "get_mirroring_status",
        lambda *args: {"status": "Running"},
    )
    monkeypatch.setattr(
        scheduler, "start_mirroring",
        lambda *args: calls.append(args),
    )

    result = await scheduler.ensure_replication_running(_healing_target(tmp_path))

    assert result.ready is True
    assert result.action == "already_running"
    assert calls == []


async def test_stopped_preflight_starts_once_then_runs(tmp_path, monkeypatch):
    statuses = iter(["Stopped", "Running"])
    starts = []
    scheduler._LAST_START_ATTEMPT.clear()
    monkeypatch.setattr(
        scheduler, "get_mirroring_status",
        lambda *args: {"status": next(statuses)},
    )
    monkeypatch.setattr(
        scheduler, "start_mirroring",
        lambda *args: starts.append(args) or {},
    )
    monkeypatch.setattr(scheduler.asyncio, "sleep", lambda delay: _completed())

    result = await scheduler.ensure_replication_running(_healing_target(tmp_path))

    assert result.ready is True
    assert result.action == "started"
    assert len(starts) == 1


async def _completed():
    return None


async def test_stopping_preflight_defers_without_start(tmp_path, monkeypatch):
    starts = []
    monkeypatch.setattr(
        scheduler, "get_mirroring_status",
        lambda *args: {"status": "Stopping"},
    )
    monkeypatch.setattr(
        scheduler, "start_mirroring",
        lambda *args: starts.append(args),
    )

    result = await scheduler.ensure_replication_running(_healing_target(tmp_path))

    assert result.ready is False
    assert result.action == "deferred"
    assert starts == []


async def test_authorization_failure_is_permission_specific(tmp_path, monkeypatch):
    def denied(*args):
        raise fab.FabricApiError(
            "Read and Write permission required", status_code=403
        )

    monkeypatch.setattr(scheduler, "get_mirroring_status", denied)

    result = await scheduler.ensure_replication_running(_healing_target(tmp_path))

    assert result.ready is False
    assert result.action == "permission_error"

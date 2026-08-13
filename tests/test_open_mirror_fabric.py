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


# --- helper --------------------------------------------------------------

def test_landing_zone_url():
    assert fab.landing_zone_url("ws", "db") == \
        "https://onelake.dfs.fabric.microsoft.com/ws/db/Files/LandingZone"


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

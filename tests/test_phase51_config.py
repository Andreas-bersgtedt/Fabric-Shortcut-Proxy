"""Phase 5.1: live config — read effective config, save changes, scale the fleet."""
from __future__ import annotations

import json
import os

os.environ.setdefault("DB_URL", "sqlite+aiosqlite:///:memory:")
os.environ.setdefault("S3_BUCKET", "test-bucket")

import httpx
import pytest
from fastapi import FastAPI

import config
from configbuilder.router import router as cb_router
from control.admin import create_admin_router
from control.registry import Registry


# ---------------------------------------------------------------------------
# config.py helpers
# ---------------------------------------------------------------------------

def test_effective_settings_reports_value_and_source(monkeypatch):
    monkeypatch.delenv("NUM_SPLITS", raising=False)
    by_key = {s["key"]: s for s in config.effective_settings()}
    assert by_key["num_splits"]["type"] == "int"
    assert by_key["num_splits"]["source"] in ("env", "file", "default")

    monkeypatch.setenv("NUM_SPLITS", "16")
    by_key = {s["key"]: s for s in config.effective_settings()}
    assert by_key["num_splits"]["value"] == 16
    assert by_key["num_splits"]["source"] == "env"


def test_effective_settings_redacts_secrets():
    by_key = {s["key"]: s for s in config.effective_settings()}
    assert by_key["db_url"]["secret"] is True
    assert by_key["db_url"]["value"] in ("", "***set***")   # never the real URL


def test_validate_updates_coerces_and_rejects():
    clean, errors = config.validate_setting_updates(
        {"num_splits": "12", "enable_gateway": "true", "agent_count": 3})
    assert not errors
    assert clean == {"num_splits": 12, "enable_gateway": True, "agent_count": 3}

    clean2, errors2 = config.validate_setting_updates({"nope": 1, "num_splits": "abc"})
    assert "num_splits" not in clean2 and "nope" not in clean2
    assert any("unknown" in e for e in errors2)
    assert any("num_splits" in e for e in errors2)


def test_write_config_updates_merges_and_preserves(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.system.json").write_text(json.dumps({"system": {"host": "1.2.3.4"}}))

    result = config.write_config_updates({"agent_count": 4, "num_splits": 16})
    assert result["changed"] == ["agent_count", "num_splits"]
    # Settings route to their section files (nested under the section name); existing keys preserved.
    system = json.loads((tmp_path / "config.system.json").read_text())["system"]
    perf = json.loads((tmp_path / "config.performance.json").read_text())["performance"]
    assert system["agent_count"] == 4 and system["host"] == "1.2.3.4"
    assert perf["num_splits"] == 16


def test_write_config_updates_rejects_bad(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ValueError):
        config.write_config_updates({"num_splits": "not-an-int"})


# ---------------------------------------------------------------------------
# config builder /_config API
# ---------------------------------------------------------------------------

def _cb_app() -> FastAPI:
    app = FastAPI()
    app.include_router(cb_router)
    return app


def _client(app: FastAPI) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t")


async def test_current_endpoint_lists_effective_config():
    async with _client(_cb_app()) as c:
        r = await c.get("/_config/api/current")
        assert r.status_code == 200
        d = r.json()
        keys = {s["key"] for s in d["settings"]}
        assert "agent_count" in keys and "config_file" in d


async def test_save_endpoint_persists_and_validates(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    async with _client(_cb_app()) as c:
        r = await c.post("/_config/api/save", json={"settings": {"agent_count": 5}})
        assert r.status_code == 200
        body = r.json()
        assert body["ok"] and body["changed"] == ["agent_count"] and body["restart_required"]

        bad = await c.post("/_config/api/save", json={"settings": {"bogus": 1}})
        assert bad.status_code == 400
        empty = await c.post("/_config/api/save", json={"settings": {}})
        assert empty.status_code == 400


async def test_save_endpoint_allows_secret_replace_and_clear(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    async with _client(_cb_app()) as c:
        rep = await c.post("/_config/api/save", json={"settings": {"secret_access_key": "new-secret"}})
        assert rep.status_code == 200 and rep.json()["ok"]
        after_replace = json.loads((tmp_path / "config.system.json").read_text())["system"]
        assert after_replace["secret_access_key"] == "new-secret"

        clr = await c.post("/_config/api/save", json={"settings": {"secret_access_key": ""}})
        assert clr.status_code == 200 and clr.json()["ok"]
        after_clear = json.loads((tmp_path / "config.system.json").read_text())["system"]
        assert after_clear["secret_access_key"] == ""


# ---------------------------------------------------------------------------
# Manager live-scale endpoint
# ---------------------------------------------------------------------------

def _mgr_app(scale, token=""):
    app = FastAPI()
    app.include_router(create_admin_router(Registry(), [], gateway=None, token=token, scale=scale))
    return app


async def test_scale_endpoint_invokes_callback():
    calls: list[int] = []

    async def fake_scale(count):
        calls.append(count)
        return {"ok": True, "count": count, "agents": [f"agent-{i+1}" for i in range(count)]}

    async with _client(_mgr_app(fake_scale)) as c:
        r = await c.post("/_manager/api/scale", json={"count": 3})
        assert r.status_code == 200 and r.json()["count"] == 3
        assert calls == [3]
        assert (await c.post("/_manager/api/scale", json={"count": 0})).status_code == 400
        assert (await c.post("/_manager/api/scale", json={})).status_code == 400


async def test_scale_endpoint_token_guarded():
    async def fake_scale(count):
        return {"ok": True, "count": count}

    async with _client(_mgr_app(fake_scale, token="s3cret")) as c:
        assert (await c.post("/_manager/api/scale", json={"count": 2})).status_code == 401
        ok = await c.post("/_manager/api/scale", json={"count": 2},
                          headers={"X-Admin-Token": "s3cret"})
        assert ok.status_code == 200


def test_scale_endpoint_absent_without_callback():
    # No scale callback -> the route isn't registered (single-process safety).
    router = create_admin_router(Registry(), [], gateway=None, token="")
    paths = {getattr(r, "path", None) for r in router.routes}
    assert "/_manager/api/scale" not in paths
    # ...but it IS registered when a scale callback is provided.
    async def fake_scale(count):
        return {"ok": True}
    router2 = create_admin_router(Registry(), [], gateway=None, token="", scale=fake_scale)
    paths2 = {getattr(r, "path", None) for r in router2.routes}
    assert "/_manager/api/scale" in paths2


async def test_shutdown_endpoint_invokes_callback():
    called = {"n": 0}

    async def fake_shutdown():
        called["n"] += 1
        return {"ok": True, "action": "shutdown", "note": "stopping"}

    app = FastAPI()
    app.include_router(create_admin_router(Registry(), [], gateway=None,
                                           token="tok", shutdown=fake_shutdown))
    async with _client(app) as c:
        # token-guarded
        assert (await c.post("/_manager/api/shutdown")).status_code == 401
        ok = await c.post("/_manager/api/shutdown", headers={"X-Admin-Token": "tok"})
        assert ok.status_code == 200 and ok.json()["action"] == "shutdown"
        assert called["n"] == 1


def test_shutdown_endpoint_absent_without_callback():
    router = create_admin_router(Registry(), [], gateway=None, token="")
    paths = {getattr(r, "path", None) for r in router.routes}
    assert "/_manager/api/shutdown" not in paths

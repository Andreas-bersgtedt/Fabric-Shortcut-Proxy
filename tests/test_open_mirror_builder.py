"""Phase 4 — config-builder Open Mirroring endpoints.

Exercises the router's ``/api/open-mirror/preview`` and ``/api/open-mirror/save``
endpoints in isolation (their own FastAPI app), plus the bootstrap payload, using
a temp working directory so config.open_mirror.json is written there.
"""
from __future__ import annotations

import asyncio
import json
import os
from types import SimpleNamespace

os.environ.setdefault("DB_URL", "sqlite+aiosqlite:///:memory:")

import httpx
import pytest
from fastapi import FastAPI

import config
from configbuilder.router import router as cb_router


@pytest.fixture
def app():
    a = FastAPI()
    a.include_router(cb_router)
    return a


def _client(app):
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t")


def _target(root="https://onelake.dfs.fabric.microsoft.com/ws/db/Files/LandingZone"):
    return {
        "id": "fabric-sales",
        "connection": "default",
        "landing_zone_root": root,
        "source_type": "SQL",
        "tables": [{
            "name": "sales", "source_table": "dbo.sales", "target_table": "sales",
            "key_column": "id", "schema": "dbo",
        }],
    }


async def test_preview_returns_landing_zone_layout(app):
    async with _client(app) as c:
        r = await c.post("/_config/api/open-mirror/preview", json={"target": _target()})
    assert r.status_code == 200
    d = r.json()
    assert d["ok"] is True
    assert d["is_onelake"] is True
    assert d["partner_events_file"].endswith("/_partnerEvents.json")
    layout = d["layout"][0]
    assert layout["folder"].endswith("/dbo.schema/sales")
    assert layout["metadata_file"].endswith("/dbo.schema/sales/_metadata.json")
    assert layout["first_data_file"].endswith("/dbo.schema/sales/00000000000000000001.parquet")
    assert layout["key_columns"] == ["id"]


async def test_preview_rejects_target_on_unknown_connection(app):
    bad = _target()
    bad["connection"] = "missing-conn"
    async with _client(app) as c:
        r = await c.post("/_config/api/open-mirror/preview", json={"target": bad})
    assert r.status_code == 400
    assert any("missing-conn" in e for e in r.json()["errors"])


async def test_save_writes_open_mirror_config(app, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    async with _client(app) as c:
        r = await c.post("/_config/api/open-mirror/save", json={"open_mirror_targets": [_target()]})
    assert r.status_code == 200
    assert r.json()["ok"] is True

    data = json.loads((tmp_path / "config.open_mirror.json").read_text(encoding="utf-8"))
    saved = data["open_mirror"]["open_mirror_targets"][0]
    assert saved["id"] == "fabric-sales"
    assert saved["connection"] == "default"
    assert saved["tables"][0]["target_table"] == "sales"


async def test_save_rejects_invalid_payload(app, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    async with _client(app) as c:
        r = await c.post("/_config/api/open-mirror/save", json={"open_mirror_targets": "nope"})
    assert r.status_code == 400


async def test_bootstrap_includes_open_mirror_targets(app):
    async with _client(app) as c:
        r = await c.get("/_config/api/bootstrap")
    assert r.status_code == 200
    assert "open_mirror_targets" in r.json()["builder"]


def test_index_has_open_mirror_tab():
    from pathlib import Path
    html = (Path(__file__).parent.parent / "configbuilder" / "index.html").read_text(encoding="utf-8")
    assert 'data-tab="openmirror"' in html
    assert 'id="openmirror-tab"' in html
    assert "api/open-mirror/save" in html
    assert "api/open-mirror/preview" in html
    assert "api/open-mirror/publish" in html
    assert "api/open-mirror/list-tables" in html
    assert "api/open-mirror/inspect-table" in html
    assert "api/open-mirror/fabric/workspaces" in html
    assert 'id="omMirroredDb"' in html
    assert "Column policies" in html
    assert "deterministic_hash" in html
    assert "omUpdatePolicy" in html


async def test_list_and_inspect_tables_for_connection(app, tmp_path, monkeypatch):
    from sqlalchemy import create_engine, text
    db = tmp_path / "picker.db"
    eng = create_engine(f"sqlite:///{db.as_posix()}")
    with eng.begin() as c:
        c.execute(text("CREATE TABLE customer (id INTEGER PRIMARY KEY, name TEXT)"))
        c.execute(text("CREATE TABLE orders (order_id INTEGER PRIMARY KEY, total REAL)"))
    eng.dispose()
    monkeypatch.setattr(config, "DB_URL", f"sqlite+aiosqlite:///{db.as_posix()}", raising=False)

    async with _client(app) as c:
        r = await c.post("/_config/api/open-mirror/list-tables", json={"connection": "default"})
        assert r.status_code == 200
        d = r.json()
        assert d["ok"] is True
        names = {t["name"] for t in d["tables"]}
        assert {"customer", "orders"} <= names

        r2 = await c.post("/_config/api/open-mirror/inspect-table",
                          json={"connection": "default", "name": "customer"})
        assert r2.status_code == 200
        d2 = r2.json()
        assert d2["ok"] is True
        assert d2["detected_key"] == "id"



async def test_publish_unknown_target_returns_404(app, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    async with _client(app) as c:
        r = await c.post("/_config/api/open-mirror/publish", json={"target_id": "nope"})
    assert r.status_code == 404


async def test_publish_no_targets_is_ok(app, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)  # no config.open_mirror.json here => zero targets
    async with _client(app) as c:
        r = await c.post("/_config/api/open-mirror/publish", json={"dry_run": True})
        await asyncio.sleep(0)
        status = await c.get("/_config/api/open-mirror/publish/jobs/latest")
    assert r.status_code == 202
    d = r.json()
    assert d["ok"] is True and d["job"]["targets"] == {}
    assert status.json()["job"]["status"] == "completed"


async def test_publish_job_reports_per_target_progress(app, monkeypatch):
    import configbuilder.router as router_module
    import open_mirror.config as mirror_config
    import open_mirror.scheduler as scheduler
    from open_mirror.source import PublishResult, TargetResult

    started = asyncio.Event()
    release = asyncio.Event()
    target = SimpleNamespace(id="fabric-sales")

    async def fake_publish(targets, *, dry_run=False, mode=None):
        started.set()
        await release.wait()
        return [TargetResult("fabric-sales", results=[
            PublishResult("fabric-sales", "sales", action="incremental", inserts=3)
        ])]

    monkeypatch.setattr(mirror_config, "load_targets", lambda: [target])
    monkeypatch.setattr(scheduler, "publish_targets_with_preflight", fake_publish)
    router_module._OPEN_MIRROR_JOBS.clear()
    router_module._LATEST_OPEN_MIRROR_JOB_ID = None

    async with _client(app) as client:
        response = await client.post("/_config/api/open-mirror/publish", json={})
        assert response.status_code == 202
        job_id = response.json()["job"]["id"]
        await started.wait()

        running = await client.get(f"/_config/api/open-mirror/publish/jobs/{job_id}")
        assert running.json()["job"]["targets"]["fabric-sales"]["status"] == "running"
        duplicate = await client.post("/_config/api/open-mirror/publish", json={})
        assert duplicate.status_code == 409
        assert duplicate.json()["job"]["id"] == job_id

        release.set()
        for _ in range(10):
            await asyncio.sleep(0)
            completed = await client.get(f"/_config/api/open-mirror/publish/jobs/{job_id}")
            if completed.json()["job"]["status"] == "completed":
                break

    job = completed.json()["job"]
    assert job["status"] == "completed"
    mirror = job["targets"]["fabric-sales"]
    assert mirror["status"] == "completed"
    assert mirror["tables"][0]["inserts"] == 3


async def test_save_rejects_unknown_tracking_mode(app, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    target = _target()
    target["tables"][0]["mode"] = "magic"

    async with _client(app) as c:
        r = await c.post(
            "/_config/api/open-mirror/save",
            json={"open_mirror_targets": [target]},
        )

    assert r.status_code == 400
    assert "unsupported mode" in " ".join(r.json()["errors"])


async def test_save_rejects_transformed_key_column(app, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    target = _target()
    target["tables"][0]["columns"] = [{
        "field_id": 1,
        "name": "id_token",
        "source": "id",
        "type": "string",
        "transform": {
            "kind": "deterministic_hash",
            "key_ref": "customer-pii-v1",
        },
    }]
    async with _client(app) as c:
        r = await c.post(
            "/_config/api/open-mirror/save",
            json={"open_mirror_targets": [target]},
        )

    assert r.status_code == 400
    assert "control column 'id' must be pass-through" in " ".join(r.json()["errors"])


async def test_save_rejects_missing_token_key(app, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(config, "DB_URL", "mssql+aioodbc://user:pass@host/db", raising=False)
    target = _target()
    target["tables"][0]["columns"] = [
        {"field_id": 1, "name": "id", "type": "long", "nullable": False},
        {
            "field_id": 2,
            "name": "email_token",
            "source": "email",
            "type": "string",
            "transform": {
                "kind": "deterministic_hash",
                "key_ref": "missing-key",
            },
        },
    ]
    async with _client(app) as c:
        r = await c.post(
            "/_config/api/open-mirror/save",
            json={"open_mirror_targets": [target]},
        )

    assert r.status_code == 400
    assert "missing-key" in " ".join(r.json()["errors"])


async def test_reset_requires_confirmation_and_is_table_scoped(
    app, tmp_path, monkeypatch
):
    import open_mirror.config as om_config
    from open_mirror.config import target_from_dict

    target = target_from_dict(_target(str(tmp_path / "lz")))
    monkeypatch.setattr(om_config, "load_targets", lambda: [target])
    monkeypatch.setattr(
        config, "OPEN_MIRROR_STATE_DIR", str(tmp_path / "state"), raising=False
    )

    async with _client(app) as c:
        denied = await c.post(
            "/_config/api/open-mirror/reset",
            json={"target_id": target.id, "table": "sales"},
        )
        reset = await c.post(
            "/_config/api/open-mirror/reset",
            json={"target_id": target.id, "table": "sales", "confirm": True},
        )

    assert denied.status_code == 400
    assert reset.status_code == 200
    assert reset.json()["reason"] == "table_reset"
    assert reset.json()["previous_status"] == "missing"

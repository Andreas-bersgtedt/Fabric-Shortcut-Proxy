"""External Manager supervision is backed solely by Pod heartbeats."""
from __future__ import annotations

import os

os.environ.setdefault("DB_URL", "sqlite+aiosqlite:///:memory:")
os.environ.setdefault("S3_BUCKET", "test-bucket")

import httpx

import config
from enterprise.control.contract import RegisterRequest
from enterprise.control.manager_app import create_manager_app


async def test_external_supervision_has_no_children_and_becomes_ready_on_heartbeat(monkeypatch):
    monkeypatch.setattr(config, "AGENT_SUPERVISION_MODE", "external")
    app = create_manager_app()
    assert app.state.supervisors == []
    registry = app.state.registry
    registry.register(RegisterRequest(
        agent_id="python-materializer-0", host="127.0.0.1", advertise_host="127.0.0.1",
        port=9400, os="linux", version="test",
    ))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                 base_url="http://manager") as client:
        response = await client.get("/readyz")
    assert response.status_code == 200
    assert response.json()["agents_registered_alive"] == 1

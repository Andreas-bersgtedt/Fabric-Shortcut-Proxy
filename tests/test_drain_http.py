"""End-to-end drain validation over HTTP: a drained agent reports /readyz 503 (so
an external LB deregisters it) while /healthz stays 200 (liveness must not flap)."""
from __future__ import annotations

import os

os.environ.setdefault("DB_URL", "sqlite+aiosqlite:///:memory:")
os.environ.setdefault("S3_BUCKET", "test-bucket")

import httpx
import pytest

from main import app
from runtime import drain


@pytest.fixture
async def client():
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://lb"
    ) as c:
        yield c


@pytest.mark.asyncio
async def test_drain_flips_readyz_but_keeps_healthz(client):
    assert (await client.get("/healthz")).status_code == 200
    drain.set_draining(True)
    try:
        r = await client.get("/readyz")
        assert r.status_code == 503
        assert r.json()["status"] == "draining"
        # Liveness stays green through the grace window so the LB does not treat the
        # agent as dead while in-flight requests drain.
        assert (await client.get("/healthz")).status_code == 200
    finally:
        drain.set_draining(False)


@pytest.mark.asyncio
async def test_readyz_not_reported_draining_after_clear(client):
    drain.set_draining(True)
    drain.set_draining(False)
    # No longer draining: whatever readiness says, the reason is not "draining".
    assert (await client.get("/readyz")).json().get("status") != "draining"

"""Readiness-aware draining: /readyz flips to 503 on drain, /healthz stays 200."""
from __future__ import annotations

import os

os.environ.setdefault("DB_URL", "sqlite+aiosqlite:///:memory:")
os.environ.setdefault("S3_BUCKET", "test-bucket")

import pytest

from observability import endpoints
from runtime import drain


def test_drain_flag_roundtrip():
    assert drain.is_draining() is False
    drain.set_draining(True)
    assert drain.is_draining() is True
    drain.set_draining(False)
    assert drain.is_draining() is False


@pytest.mark.asyncio
async def test_readyz_503_when_draining():
    drain.set_draining(True)
    try:
        resp = await endpoints.readyz()
        assert resp.status_code == 503
        assert b"draining" in resp.body
    finally:
        drain.set_draining(False)


@pytest.mark.asyncio
async def test_healthz_ok_while_draining():
    # Liveness must stay green during a drain so the LB does not flap the process.
    drain.set_draining(True)
    try:
        assert (await endpoints.healthz()) == {"status": "ok"}
    finally:
        drain.set_draining(False)

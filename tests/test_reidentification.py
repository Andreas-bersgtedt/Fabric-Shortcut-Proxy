from __future__ import annotations

import pytest
from fastapi import FastAPI
import httpx

import config
import module_registry
from observability import audit
from reidentification.gate import enabled
from reidentification.router import router
from security.authorization import User
from security.authorization_middleware import AuthorizationMiddleware
from security.identity import IdentityProvider, identity_provider


def test_reidentification_module_profile_is_optional(monkeypatch, tmp_path):
    monkeypatch.setenv("FSP_CONFIG_DIR", str(tmp_path))
    module = next(item for item in module_registry.CATALOG if item.id == "reidentification")

    assert module.extra == "reidentification"
    assert module_registry.module_plan([])["blocked"] == []
    assert module_registry.module_plan(["reidentification"])["desired"] == ["reidentification"]


def test_reidentification_requires_system_and_profile_gates(monkeypatch):
    monkeypatch.setattr(config, "ENABLE_REIDENTIFICATION", False, raising=False)
    monkeypatch.setattr(module_registry, "desired_profile", lambda: ["reidentification"])
    assert enabled() is False

    monkeypatch.setattr(config, "ENABLE_REIDENTIFICATION", True, raising=False)
    monkeypatch.setattr(module_registry, "desired_profile", lambda: [])
    assert enabled() is False

    monkeypatch.setattr(module_registry, "desired_profile", lambda: ["reidentification"])
    assert enabled() is True


@pytest.mark.asyncio
async def test_reidentification_route_is_auditor_only_and_redacted(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "ENABLE_AUDIT_LOG", True, raising=False)
    monkeypatch.setenv("ADMIN_TOKEN", "admin-test-token")
    identity_path = tmp_path / "identities.json"
    monkeypatch.setenv("FSP_IDENTITY_FILE", str(identity_path))
    provider = IdentityProvider(str(identity_path))
    provider.create_or_replace(User("auditor", roles=("auditor",)), "correct horse battery staple")
    auditor = provider.authenticate("auditor", "correct horse battery staple")
    session = identity_provider().create_session(auditor)
    audit._buf.clear()

    app = FastAPI()
    app.add_middleware(AuthorizationMiddleware)
    app.include_router(router)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test",
        cookies={"fsp_session": session},
    ) as client:
        allowed = await client.post("/_reidentify/api/v1/lookup")
        denied = await client.post(
            "/_reidentify/api/v1/lookup", headers={"X-Admin-Token": "admin-test-token"}
        )

    assert allowed.status_code == 501
    assert allowed.json()["error"] == "re-identification lookup is not implemented"
    assert denied.status_code == 403
    events = audit.recent()
    assert events[-2]["action"] == "reidentification_placeholder"
    assert events[-2]["identity"] == "auditor"
    assert events[-1]["action"] == "reidentification_request"
    assert events[-1]["identity"] == "admin-token"
    assert events[-1]["status"] == 403
    assert all(isinstance(event["ts"], float) for event in events[-2:])
    assert all("value" not in event and "token" not in event for event in events[-2:])
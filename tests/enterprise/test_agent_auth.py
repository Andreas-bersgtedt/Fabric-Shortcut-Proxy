"""Manager Agent-authentication boundary and Python client coverage."""
from __future__ import annotations

import asyncio
import base64
import json
import time

import httpx
import pytest
from fastapi import FastAPI, Request, Response

from fabric_shortcut_proxy import config
from enterprise.control.contract import HeartbeatRequest, RegisterRequest, TaskResult
from enterprise.control.registry import Registry
from enterprise.control.server import ControlService
from enterprise.control.transport import RestControlClient, create_control_router
from fabric_shortcut_proxy.security.agent_auth import (
    AGENT_ID_HEADER,
    AGENT_TOKEN_HEADER,
    AgentAuthMiddleware,
    AgentTokenProvider,
)
from fabric_shortcut_proxy.security.operator_auth import ManagerAuthMiddleware

ACTIVE = "a" * 64
PREVIOUS = "b" * 64


def _basic(user: str = "operator", password: str = "manager-secret") -> str:
    encoded = base64.b64encode(f"{user}:{password}".encode()).decode()
    return f"Basic {encoded}"


def _request(agent_id: str = "agent-1") -> RegisterRequest:
    return RegisterRequest(
        agent_id=agent_id,
        host="127.0.0.1",
        port=9000,
        os="linux",
        version="test",
    )


def _app(registry: Registry) -> FastAPI:
    app = FastAPI()
    app.add_middleware(ManagerAuthMiddleware)
    app.add_middleware(AgentAuthMiddleware)
    app.include_router(create_control_router(ControlService(registry)))

    @app.get("/control/work-queue")
    async def work_queue():
        return {"ok": True}

    return app


def _headers(token: str = ACTIVE, agent_id: str = "agent-1") -> dict[str, str]:
    return {AGENT_TOKEN_HEADER: token, AGENT_ID_HEADER: agent_id}


@pytest.fixture(autouse=True)
def _auth_config(monkeypatch):
    monkeypatch.setenv("AGENT_AUTH_MODE", "required")
    monkeypatch.setenv("AGENT_TOKEN", ACTIVE)
    monkeypatch.delenv("AGENT_TOKEN_PREVIOUS", raising=False)
    monkeypatch.delenv("AGENT_TOKEN_PREVIOUS_VALID_UNTIL", raising=False)
    monkeypatch.setattr(config, "MANAGER_AUTH_ENABLED", True)
    monkeypatch.setattr(config, "MANAGER_AUTH_USERNAME", "operator")
    monkeypatch.setattr(config, "MANAGER_AUTH_PASSWORD", "manager-secret")


@pytest.mark.parametrize(
    "headers",
    [
        {AGENT_ID_HEADER: "agent-1"},
        _headers("short"),
        _headers("x" * 64),
        {AGENT_TOKEN_HEADER: ACTIVE},
    ],
)
async def test_required_mode_rejects_missing_malformed_and_incorrect_credentials(headers):
    registry = Registry()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(registry)), base_url="http://manager"
    ) as client:
        response = await client.post(
            "/control/register", json=_request().to_dict(), headers=headers
        )

    assert response.status_code == 401
    assert response.json() == {"detail": "agent authentication required"}
    assert registry.count() == 0


async def test_active_and_unexpired_previous_tokens_are_accepted(monkeypatch):
    registry = Registry()
    monkeypatch.setenv("AGENT_TOKEN_PREVIOUS", PREVIOUS)
    monkeypatch.setenv("AGENT_TOKEN_PREVIOUS_VALID_UNTIL", str(int(time.time()) + 60))

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(registry)), base_url="http://manager"
    ) as client:
        active = await client.post(
            "/control/register", json=_request("active").to_dict(),
            headers=_headers(ACTIVE, "active"),
        )
        previous = await client.post(
            "/control/register", json=_request("previous").to_dict(),
            headers=_headers(PREVIOUS, "previous"),
        )

    assert active.status_code == 200
    assert previous.status_code == 200
    assert registry.count() == 2


async def test_previous_token_is_rejected_at_deadline(monkeypatch):
    registry = Registry()
    monkeypatch.setenv("AGENT_TOKEN_PREVIOUS", PREVIOUS)
    monkeypatch.setenv("AGENT_TOKEN_PREVIOUS_VALID_UNTIL", str(int(time.time())))

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(registry)), base_url="http://manager"
    ) as client:
        response = await client.post(
            "/control/register", json=_request().to_dict(), headers=_headers(PREVIOUS)
        )

    assert response.status_code == 401
    assert registry.count() == 0


async def test_required_mode_without_active_token_returns_503(monkeypatch):
    monkeypatch.delenv("AGENT_TOKEN")
    registry = Registry()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(registry)), base_url="http://manager"
    ) as client:
        response = await client.post(
            "/control/register", json=_request().to_dict(),
            headers={AGENT_ID_HEADER: "agent-1"},
        )

    assert response.status_code == 503
    assert response.json() == {"detail": "agent authentication unavailable"}
    assert registry.count() == 0


async def test_equal_active_and_previous_tokens_are_unavailable(monkeypatch):
    monkeypatch.setenv("AGENT_TOKEN_PREVIOUS", ACTIVE)
    monkeypatch.setenv("AGENT_TOKEN_PREVIOUS_VALID_UNTIL", str(int(time.time()) + 60))
    registry = Registry()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(registry)), base_url="http://manager"
    ) as client:
        response = await client.post(
            "/control/register", json=_request().to_dict(), headers=_headers()
        )

    assert response.status_code == 503
    assert registry.count() == 0


async def test_identity_mismatch_is_rejected_before_registration():
    registry = Registry()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(registry)), base_url="http://manager"
    ) as client:
        response = await client.post(
            "/control/register",
            json=_request("payload-agent").to_dict(),
            headers=_headers(ACTIVE, "header-agent"),
        )

    assert response.status_code == 401
    assert registry.count() == 0


async def test_compatibility_basic_is_explicit_and_required_mode_rejects_it(monkeypatch):
    registry = Registry()
    basic_headers = {
        "Authorization": _basic(),
        AGENT_ID_HEADER: "agent-1",
    }
    app = _app(registry)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://manager"
    ) as client:
        required = await client.post(
            "/control/register", json=_request().to_dict(), headers=basic_headers
        )
        monkeypatch.setenv("AGENT_AUTH_MODE", "compatibility")
        compatibility = await client.post(
            "/control/register", json=_request().to_dict(), headers=basic_headers
        )

    assert required.status_code == 401
    assert compatibility.status_code == 200


async def test_compatibility_preserves_legacy_basic_clients_without_identity(monkeypatch):
    monkeypatch.setenv("AGENT_AUTH_MODE", "compatibility")
    registry = Registry()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(registry)), base_url="http://manager"
    ) as client:
        response = await client.post(
            "/control/register",
            json=_request().to_dict(),
            headers={"Authorization": _basic()},
        )

    assert response.status_code == 200
    assert registry.count() == 1


async def test_bearer_is_not_agent_authentication(monkeypatch):
    monkeypatch.setenv("AGENT_AUTH_MODE", "compatibility")
    registry = Registry()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(registry)), base_url="http://manager"
    ) as client:
        response = await client.post(
            "/control/register",
            json=_request().to_dict(),
            headers={"Authorization": "******", AGENT_ID_HEADER: "agent-1"},
        )

    assert response.status_code == 401
    assert registry.count() == 0


async def test_operator_session_is_not_agent_authentication(monkeypatch):
    monkeypatch.setenv("AGENT_AUTH_MODE", "compatibility")
    registry = Registry()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(registry)), base_url="http://manager"
    ) as client:
        client.cookies.set("fsp_session", "operator-session")
        response = await client.post(
            "/control/register",
            json=_request().to_dict(),
            headers={AGENT_ID_HEADER: "agent-1"},
        )

    assert response.status_code == 401
    assert registry.count() == 0


async def test_agent_token_cannot_access_operator_route():
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(Registry())), base_url="http://manager"
    ) as client:
        response = await client.get("/control/work-queue", headers=_headers())

    assert response.status_code == 401


async def test_concurrent_valid_and_invalid_requests_are_isolated():
    registry = Registry()
    app = _app(registry)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://manager"
    ) as client:
        requests = []
        for index in range(10):
            agent_id = f"valid-{index}"
            requests.append(client.post(
                "/control/register",
                json=_request(agent_id).to_dict(),
                headers=_headers(ACTIVE, agent_id),
            ))
            requests.append(client.post(
                "/control/register",
                json=_request(f"invalid-{index}").to_dict(),
                headers=_headers("x" * 64, f"invalid-{index}"),
            ))
        responses = await asyncio.gather(*requests)

    assert [response.status_code for response in responses].count(200) == 10
    assert [response.status_code for response in responses].count(401) == 10
    assert registry.count() == 10


def test_provider_compares_active_and_previous_candidates(monkeypatch):
    from fabric_shortcut_proxy.security import agent_auth

    monkeypatch.setenv("AGENT_TOKEN_PREVIOUS", PREVIOUS)
    monkeypatch.setenv("AGENT_TOKEN_PREVIOUS_VALID_UNTIL", str(int(time.time()) + 60))
    calls: list[tuple[str, str]] = []
    original = agent_auth.hmac.compare_digest

    def recording_compare(left, right):
        calls.append((left, right))
        return original(left, right)

    monkeypatch.setattr(agent_auth.hmac, "compare_digest", recording_compare)
    result = AgentTokenProvider.verify("x" * 64)

    assert result.reason == "invalid"
    assert len(calls) == 3
    assert [left for left, _ in calls[-2:]] == ["x" * 64, "x" * 64]


async def test_python_client_reads_rotated_token_for_each_request(monkeypatch):
    seen: list[dict[str, str]] = []
    app = FastAPI()

    @app.post("/control/register")
    async def register(request: Request):
        seen.append(dict(request.headers))
        return {"lease_id": "lease", "heartbeat_ms": 1000}

    client = RestControlClient(
        "http://manager",
        agent_id="agent-1",
        transport=httpx.ASGITransport(app=app),
    )
    try:
        await client.register(_request())
        monkeypatch.setenv("AGENT_TOKEN", PREVIOUS)
        await client.register(_request())
    finally:
        await client.aclose()

    assert seen[0]["x-fsp-agent-token"] == ACTIVE
    assert seen[1]["x-fsp-agent-token"] == PREVIOUS
    assert all(item["x-fsp-agent-id"] == "agent-1" for item in seen)
    assert all("authorization" not in item for item in seen)


async def test_python_client_authenticates_every_control_call():
    seen: list[tuple[str, str, dict[str, str]]] = []
    app = FastAPI()

    def capture(request: Request) -> None:
        seen.append((request.method, request.url.path, dict(request.headers)))

    @app.post("/control/register")
    async def register(request: Request):
        capture(request)
        return {"lease_id": "lease", "heartbeat_ms": 1000}

    @app.post("/control/heartbeat")
    async def heartbeat(request: Request):
        capture(request)
        return {"commands": []}

    @app.get("/control/assignment/{agent_id}")
    async def assignment(agent_id: str, request: Request):
        capture(request)
        return {"agent_id": agent_id, "tables": []}

    @app.get("/control/snapshot/{table}")
    async def snapshot(table: str, request: Request):
        capture(request)
        return Response(status_code=404)

    @app.post("/control/task-result")
    async def task_result(request: Request):
        capture(request)
        return {"ok": True}

    client = RestControlClient(
        "http://manager",
        agent_id="agent-1",
        transport=httpx.ASGITransport(app=app),
    )
    try:
        await client.register(_request())
        await client.heartbeat(HeartbeatRequest(agent_id="agent-1", lease_id="lease"))
        await client.get_assignment("agent-1")
        await client.get_snapshot("orders")
        await client.report_task_result(
            TaskResult(
                agent_id="agent-1",
                table="orders",
                epoch=1,
                split_index=0,
                ok=False,
            )
        )
    finally:
        await client.aclose()

    assert [item[:2] for item in seen] == [
        ("POST", "/control/register"),
        ("POST", "/control/heartbeat"),
        ("GET", "/control/assignment/agent-1"),
        ("GET", "/control/snapshot/orders"),
        ("POST", "/control/task-result"),
    ]
    for _, _, headers in seen:
        assert headers["x-fsp-agent-token"] == ACTIVE
        assert headers["x-fsp-agent-id"] == "agent-1"
        assert "authorization" not in headers


async def test_failed_auth_audit_contains_reason_not_token(tmp_path, monkeypatch):
    from fabric_shortcut_proxy.observability import audit

    audit_path = tmp_path / "audit.jsonl"
    monkeypatch.setattr(config, "ENABLE_AUDIT_LOG", True)
    monkeypatch.setattr(config, "AUDIT_LOG_FILE", str(audit_path))
    rejected = "z" * 64
    request_id = "agent-auth-denied"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(Registry())), base_url="http://manager"
    ) as client:
        response = await client.post(
            "/control/register",
            json=_request().to_dict(),
            headers={
                **_headers(rejected),
                "X-Request-ID": request_id,
            },
        )

    assert response.status_code == 401
    record = next(item for item in audit.recent() if item.get("request_id") == request_id)
    assert record["action"] == "agent_auth"
    assert record["reason"] == "invalid"
    assert rejected not in json.dumps(record)
    assert rejected not in audit_path.read_text(encoding="utf-8")

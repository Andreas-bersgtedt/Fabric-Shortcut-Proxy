from __future__ import annotations

import base64
import copy
import os
import subprocess
import sys

import httpx
import pytest
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from structlog.testing import capture_logs

from fabric_shortcut_proxy import config
from fabric_shortcut_proxy.runtime.artifact_store import MemoryStore


def _basic_auth() -> dict[str, str]:
    token = base64.b64encode(b"operator:test-password").decode()
    return {"Authorization": f"Basic {token}"}


@pytest.fixture
def operator_apps(tmp_path, monkeypatch):
    from enterprise.control.placement import PlacementConfig
    from fabric_shortcut_proxy.runtime import artifact_store

    monkeypatch.setenv("FSP_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("CONFIG_FILE", str(tmp_path / "config.json"))
    monkeypatch.setenv("FSP_AUTHZ_ENFORCE", "0")
    monkeypatch.setenv("AGENT_TOKEN", "a" * 64)
    monkeypatch.delenv("AGENT_IDENTITY_TOKENS", raising=False)
    monkeypatch.delenv("FSP_FEDERATION_ENABLED", raising=False)
    for name, value in {
        "MANAGER_AUTH_ENABLED": True,
        "MANAGER_AUTH_USERNAME": "operator",
        "MANAGER_AUTH_PASSWORD": "test-password",
        "ENTRA_ENABLED": False,
        "MANAGER_HA": False,
        "MANAGER_SUPERVISION_MODE": "external",
        "MATERIALIZATION_WORK_QUEUE": False,
        "ENABLE_GATEWAY": False,
        "ENABLE_ADMIN_UI": True,
        "ENABLE_CONFIG_BUILDER": True,
        "ADMIN_TOKEN": "",
        "CREDENTIAL_STORE_PATH": str(tmp_path / "credentials.json"),
    }.items():
        monkeypatch.setattr(config, name, value)
    monkeypatch.setattr(artifact_store, "get_default_store", lambda: MemoryStore())

    from enterprise.control.catalog_app import create_catalog_app
    from enterprise.control.manager_app import create_manager_app
    from tests.enterprise.test_residency_policy import _config

    manager = create_manager_app()
    catalog = create_catalog_app(
        store=MemoryStore(), placement=PlacementConfig.from_dict(_config()),
    )
    return manager, catalog


def _client(app):
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    )


def _assert_problem(response, status, code, correlation_id="contract-test"):
    assert response.status_code == status
    assert response.headers["content-type"] == "application/problem+json"
    problem = response.json()
    assert {
        "type", "title", "status", "code", "correlationId",
    } <= problem.keys()
    assert problem["status"] == status
    assert problem["code"] == code
    assert problem["correlationId"] == response.headers["x-correlation-id"] == correlation_id
    return problem


async def test_manager_auth_and_validation_errors_use_the_shared_contract(
    operator_apps, monkeypatch,
):
    from fabric_shortcut_proxy.security import authorization_middleware
    from fabric_shortcut_proxy.security.authorization import AuthorizationError

    manager, _ = operator_apps
    correlation = {"X-Correlation-ID": "contract-test"}
    async with _client(manager) as client:
        unauthorized = await client.get("/_manager/api/fleet", headers=correlation)
        bad_request = await client.post(
            "/_config/api/save",
            headers={**_basic_auth(), **correlation},
            json={"settings": {}},
        )

        def deny(*_args, **_kwargs):
            raise AuthorizationError("permission denied")

        monkeypatch.setattr(authorization_middleware, "require", deny)
        forbidden = await client.post(
            "/_config/api/save",
            headers={**_basic_auth(), **correlation},
            json={"settings": {}},
        )

    _assert_problem(unauthorized, 401, "authentication_required")
    assert unauthorized.headers["www-authenticate"].startswith("Basic")
    bad_problem = _assert_problem(bad_request, 400, "bad_request")
    assert "settings" in bad_problem["error"]
    _assert_problem(forbidden, 403, "permission_denied")


async def test_catalog_conflict_and_validation_hide_input_values(operator_apps):
    _, catalog = operator_apps
    headers = {**_basic_auth(), "X-Correlation-ID": "contract-test"}
    payload = {
        "dataset_id": "erp::sales",
        "owner_id": "manager-swc",
        "owner_location": "azure:swedencentral",
        "lease_ms": 60_000,
        "freshness_target_ms": 5_000,
    }
    sentinel = "catalog-validation-secret-sentinel"
    async with _client(catalog) as client:
        created = await client.post("/catalog/datasets", headers=headers, json=payload)
        conflict = await client.post("/catalog/datasets", headers=headers, json=payload)
        invalid = await client.post(
            "/catalog/datasets", headers=headers, json={**payload, "lease_ms": sentinel},
        )
    assert created.status_code == 201
    _assert_problem(conflict, 409, "conflict")
    _assert_problem(invalid, 422, "validation_error")
    assert sentinel not in invalid.text


async def test_configuration_problem_codes_revision_and_safe_logging(operator_apps, monkeypatch):
    from fabric_shortcut_proxy.configbuilder import router as builder

    manager, _ = operator_apps
    headers = {**_basic_auth(), "X-Correlation-ID": "contract-test"}
    typed = {**headers, "X-FSP-Configuration-Contract": "3.1.0"}
    async with _client(manager) as client:
        initial = await client.get("/_config/api/current", headers=headers)
        saved = await client.post(
            "/_config/api/save", headers={**headers, "If-Match": initial.headers["etag"]},
            json={"settings": {"num_splits": 7}},
        )
        with capture_logs() as logs:
            stale = await client.post(
                "/_config/api/apply",
                headers={**headers, "If-Match": initial.headers["etag"]},
                json={"settings": {"num_splits": 8}},
            )
        invalid = await client.post(
            "/_config/api/apply",
            headers={**typed, "If-Match": saved.headers["etag"]},
            json={"settings": {"unsupported": "typed-secret-sentinel"}},
        )
        missing = await client.post(
            "/_config/api/apply", headers=typed, json={"settings": {"tables": []}},
        )
        with capture_logs() as invalid_logs:
            invalid_id = await client.post(
                "/_config/api/apply",
                headers={**_basic_auth(), "X-Correlation-ID": "x" * 129,
                         "If-Match": initial.headers["etag"]},
                json={"settings": {"num_splits": 8}},
            )

        def persist_failure(_body):
            raise OSError("persist-secret-sentinel")

        monkeypatch.setattr(builder, "apply_typed_settings", persist_failure)
        persist_error = await client.post(
            "/_config/api/apply",
            headers={**typed, "If-Match": saved.headers["etag"]},
            json={"settings": {"tables": []}},
        )

    assert saved.status_code == 200
    _assert_problem(stale, 409, "stale_revision")
    assert stale.headers["etag"] == saved.headers["etag"]
    _assert_problem(invalid, 422, "configuration_not_representable")
    _assert_problem(missing, 428, "precondition_required")
    _assert_problem(persist_error, 500, "configuration_persist_failed")
    assert "persist-secret-sentinel" not in persist_error.text
    generated_id = invalid_id.headers["x-correlation-id"]
    _assert_problem(invalid_id, 409, "stale_revision", generated_id)
    assert generated_id != "x" * 129
    assert {
        entry["correlation_id"] for entry in invalid_logs
        if entry.get("code") == "stale_revision"
    } == {generated_id}
    assert "typed-secret-sentinel" not in invalid.text + str(logs)
    assert any(
        entry.get("correlation_id") == "contract-test"
        and entry.get("code") == "stale_revision"
        for entry in logs
    )


async def test_unexpected_error_is_safe_and_uses_a_generated_correlation_id(
    operator_apps, monkeypatch,
):
    from fabric_shortcut_proxy.observability import operator_errors

    manager, _ = operator_apps
    sentinel = "unexpected-secret-sentinel"
    logs = []

    class RecordingLogger:
        def error(self, event, **fields):
            logs.append({"event": event, **fields})

    def fail():
        raise RuntimeError(sentinel)

    monkeypatch.setattr(operator_errors, "log", RecordingLogger())
    monkeypatch.setattr(config, "effective_settings", fail)
    async with _client(manager) as client:
        response = await client.get(
            "/_config/api/current",
            headers={**_basic_auth(), "X-Correlation-ID": "x" * 129},
        )

    correlation_id = response.headers["x-correlation-id"]
    _assert_problem(response, 500, "internal_error", correlation_id)
    assert correlation_id != "x" * 129
    assert sentinel not in response.text + str(logs)
    assert any(
        entry.get("correlation_id") == correlation_id
        and entry.get("code") == "internal_error"
        for entry in logs
    )


async def test_legacy_client_errors_keep_fields_but_scrub_nested_secrets():
    from fastapi.responses import JSONResponse
    from fabric_shortcut_proxy.observability.operator_errors import (
        OPERATOR_API_PREFIXES,
        install_operator_errors,
    )

    app = FastAPI()

    @app.get("/_config/api/example")
    def error():
        return JSONResponse({
            "detail": "postgresql://user:db-secret@host/db?password=query-secret",
            "issues": [
                {"credential": "nested-secret", "reason": "password=message-secret"},
                {"target": "postgresql://user:bad-url-secret@host:bad/db?sig=signed-url-secret"},
                {"target": "postgresql://alice:malformed-secret?sig=query-sentinel@host/db"},
            ],
        }, status_code=400)

    install_operator_errors(app, prefixes=OPERATOR_API_PREFIXES)
    async with _client(app) as client:
        response = await client.get(
            "/_config/api/example", headers={"X-Correlation-ID": "contract-test"},
        )
    problem = _assert_problem(response, 400, "bad_request")
    assert problem["issues"][0]["credential"] == "[REDACTED]"
    assert problem["issues"][2]["target"] == "[REDACTED URL]"
    assert all(secret not in response.text for secret in (
        "db-secret", "query-secret", "nested-secret", "message-secret",
        "bad-url-secret", "signed-url-secret", "malformed-secret", "query-sentinel",
    ))


async def test_agent_auth_contract_is_not_changed(operator_apps):
    manager, _ = operator_apps
    async with _client(manager) as client:
        response = await client.post("/control/register", json={})
    assert response.status_code == 401
    assert response.headers["www-authenticate"] == "FSP-Agent"
    assert response.json() == {"detail": "agent authentication required"}
    assert response.headers["content-type"] == "application/json"


def test_openapi_publishes_shared_errors_and_revision_headers(operator_apps):
    manager, catalog = operator_apps
    for app, path, method in [
        (manager, "/_manager/api/fleet", "get"),
        (manager, "/_config/api/current", "get"),
        (manager, "/_config/api/apply", "post"),
        (catalog, "/catalog/datasets", "post"),
    ]:
        schema = app.openapi()
        operation = schema["paths"][path][method]
        assert "OperatorProblem" in schema["components"]["schemas"]
        for status in ("400", "401", "403", "409", "422", "500"):
            error = operation["responses"][status]
            assert "application/problem+json" in error["content"]
            assert "X-Correlation-ID" in error["headers"]

    paths = manager.openapi()["paths"]
    assert "ETag" in paths["/_config/api/current"]["get"]["responses"]["200"]["headers"]
    write = paths["/_config/api/apply"]["post"]
    assert any(parameter["name"] == "If-Match" for parameter in write["parameters"])
    assert "ETag" in write["responses"]["409"]["headers"]
    first = copy.deepcopy(manager.openapi())
    assert manager.openapi() == first


async def test_streamed_errors_are_bounded_and_success_streams_are_unchanged():
    from fabric_shortcut_proxy.observability.operator_errors import (
        OPERATOR_API_PREFIXES,
        install_operator_errors,
    )

    app = FastAPI()

    @app.get("/_config/api/large-error")
    async def large_error():
        return StreamingResponse(iter([b"x" * 70_000, b"tail"]), status_code=400)

    @app.get("/_config/api/stream")
    async def stream():
        return StreamingResponse(iter([b"first", b"second"]))

    install_operator_errors(app, prefixes=OPERATOR_API_PREFIXES)
    async with _client(app) as client:
        rejected = await client.get(
            "/_config/api/large-error", headers={"X-Correlation-ID": "contract-test"},
        )
        success = await client.get("/_config/api/stream")
    _assert_problem(rejected, 400, "bad_request")
    assert len(rejected.content) < 1024
    assert success.content == b"firstsecond"


def test_standalone_app_formats_operator_errors_without_changing_s3(tmp_path):
    script = """
from fastapi.testclient import TestClient
from fabric_shortcut_proxy import config
from fabric_shortcut_proxy.main import app

client = TestClient(app)
response = client.get("/_config/api/current", headers={"X-Correlation-ID": "lite-test"})
assert response.status_code == 401
assert response.headers["content-type"] == "application/problem+json"
assert response.json()["correlationId"] == response.headers["x-correlation-id"] == "lite-test"
assert "application/problem+json" in app.openapi()["paths"]["/_config/api/current"]["get"]["responses"]["401"]["content"]
s3 = client.get("/not-a-bucket/not-an-object")
assert s3.headers["content-type"].startswith("application/xml")
config.MANAGER_AUTH_ENABLED = False
for path in ("/agents/not-an-object", "/control/not-an-object"):
    s3 = client.get(path)
    assert s3.status_code == 404
    assert s3.headers["content-type"].startswith("application/xml")
"""
    env = {
        **os.environ,
        "DB_URL": "sqlite+aiosqlite:///:memory:",
        "ENABLE_CONFIG_BUILDER": "1",
        "MANAGER_AUTH_ENABLED": "1",
        "MANAGER_AUTH_USERNAME": "operator",
        "MANAGER_AUTH_PASSWORD": "test-password",
        "FSP_AUTHZ_ENFORCE": "0",
        "REQUIRE_SIGV4": "0",
        "FSP_CONFIG_DIR": str(tmp_path),
        "CONFIG_FILE": str(tmp_path / "config.json"),
    }
    result = subprocess.run(
        [sys.executable, "-c", script], env=env, capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, result.stderr

import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path

import httpx
import pytest
from cryptography.fernet import Fernet
from fastapi import FastAPI

from fabric_shortcut_proxy import config, system_config
from fabric_shortcut_proxy.configbuilder import router as router_module
from fabric_shortcut_proxy.security.configuration_references import credential_record_id
from fabric_shortcut_proxy.security.credential_store import CredentialStore

HEADER = {"X-FSP-Configuration-Contract": "3.1.0"}
URL = "postgresql+asyncpg://operator:typed-secret-sentinel@localhost/db"


def reference(key):
    return {"kind": "fsp_credential_store", "reference": f"fsp-credential://{key}"}


def connection(cid="default"):
    return {
        "id": cid, "database_url_reference": reference(cid),
        "query_timeout_seconds": 300, "query_max_rows": 500_000,
        "max_retries": 3, "retry_backoff_seconds": 1, "validate_source_schema": True,
    }


def table():
    return {
        "name": "orders", "source_table": "public.orders", "key_column": "id",
        "connection": "default",
        "schema": [{"field_id": 1, "name": "id", "type": "long", "nullable": False}],
    }


@pytest.fixture
def typed_app(tmp_path, monkeypatch):
    monkeypatch.setenv("FSP_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("CONFIG_FILE", str(tmp_path / "config.json"))
    monkeypatch.setenv("FSP_AUTHZ_ENFORCE", "0")
    monkeypatch.setenv("FSP_CRED_KEY", Fernet.generate_key().decode())
    store_path = str(tmp_path / "credentials.json")
    monkeypatch.setenv("CREDENTIAL_STORE_PATH", store_path)
    monkeypatch.setattr(config, "CREDENTIAL_STORE_PATH", store_path)
    monkeypatch.setattr(system_config, "CREDENTIAL_STORE_PATH", store_path)
    for name in list(os.environ):
        if name == "DB_URL" or name.startswith(("DB_URL_", "FSP_TOKENIZATION_KEY_")):
            monkeypatch.delenv(name)
    store = CredentialStore(store_path)
    store.set_url("default", URL)
    store.set_url("analytics", URL + "2")
    (tmp_path / "config.connection.json").write_text(json.dumps({
        "connection": {
            "db_url_reference": reference("default"), "query_timeout": 300,
            "query_max_rows": 500_000, "db_max_retries": 3,
            "db_retry_backoff": 1, "validate_source_schema": True,
            "source_table": "legacy_default_preserved",
        },
        "connections": [], "future_native_section": {"retain": True},
    }))
    (tmp_path / "config.tables.json").write_text(json.dumps({
        "tables": [table()], "future_native_section": {"retain": True},
    }))
    app = FastAPI()
    app.include_router(router_module.router)
    return app


def client(app):
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


@pytest.mark.asyncio
async def test_opt_in_fixes_settings_array_mismatch_with_exact_nonsecret_shape(typed_app):
    async with client(typed_app) as api:
        native = await api.get("/_config/api/current")
        typed = await api.get("/_config/api/current", headers=HEADER)
    assert native.status_code == typed.status_code == 200
    assert isinstance(native.json()["settings"], list)
    assert typed.json() == {"settings": {"connections": [connection()], "tables": [table()]}}
    assert typed.headers["etag"] == native.headers["etag"]
    assert typed.headers["cache-control"] == "no-store"
    assert typed.headers["vary"] == "X-FSP-Configuration-Contract"
    assert typed.headers["x-fsp-configuration-contract"] == "3.1.0"
    assert URL not in typed.text
    assert "typed-secret-sentinel" not in typed.text
    assert "db_url" not in typed.text


@pytest.mark.asyncio
async def test_persisted_connections_then_tables_readback_and_stale_rejection(
    typed_app, tmp_path, monkeypatch,
):
    monkeypatch.setattr(config, "apply_live_settings", lambda _: pytest.fail("must not apply live"))
    sources = [connection(), connection("analytics")]
    changed_table = {**table(), "connection": "analytics", "num_splits": 12,
                     "split_strategy": "range", "split_balance": "count",
                     "split_target_rows": 25000}
    credentials = (tmp_path / "credentials.json").read_bytes()
    async with client(typed_app) as api:
        initial = await api.get("/_config/api/current", headers=HEADER)
        first = await api.post("/_config/api/apply", headers={
            **HEADER, "If-Match": initial.headers["etag"],
        }, json={"settings": {"connections": sources}})
        assert first.status_code == 200, first.text
        assert first.json()["settings"]["connections"] == sources
        assert first.json()["restart_required"] == ["connections"]
        second = await api.post("/_config/api/apply", headers={
            **HEADER, "If-Match": first.headers["etag"],
        }, json={"settings": {"tables": [changed_table]}})
        assert second.status_code == 200, second.text
        before = {path.name: path.read_bytes() for path in tmp_path.glob("*.json")}
        rejected = await api.post("/_config/api/apply", headers={
            **HEADER, "If-Match": first.headers["etag"], "X-Correlation-ID": "typed-stale",
        }, json={"settings": {"tables": []}})
        current = await api.get("/_config/api/current", headers=HEADER)
    assert rejected.status_code == 409
    assert rejected.json()["code"] == "stale_revision"
    assert rejected.json()["correlationId"] == "typed-stale"
    assert current.headers["etag"] == second.headers["etag"] == rejected.headers["etag"]
    assert current.json()["settings"] == second.json()["settings"] == {
        "connections": sources, "tables": [changed_table],
    }
    assert before == {path.name: path.read_bytes() for path in tmp_path.glob("*.json")}
    assert (tmp_path / "credentials.json").read_bytes() == credentials
    saved = json.loads((tmp_path / "config.connection.json").read_text())
    assert saved["future_native_section"] == {"retain": True}
    assert saved["connection"]["source_table"] == "legacy_default_preserved"
    assert saved["connection"]["db_url_reference"] == reference("default")
    assert saved["connections"][0]["db_max_retries"] == 3
    assert saved["connections"][0]["db_url_reference"] == reference("analytics")
    assert URL not in json.dumps(saved)
    saved_tables = json.loads((tmp_path / "config.tables.json").read_text())
    assert saved_tables["future_native_section"] == {"retain": True}
    # A fresh interpreter must resolve the same credentials/limits, not an API cache.
    script = """
from fabric_shortcut_proxy import config
assert config.CONNECTIONS["default"].db_url.endswith("/db")
source = config.CONNECTIONS["analytics"]
assert source.db_url.endswith("/db2")
assert source.query_timeout_seconds == 300 and source.db_max_retries == 3
assert source.db_retry_backoff_seconds == 1
assert config.TABLES[0].connection_id == "analytics"
assert config.TABLES[0].num_splits == 12
"""
    result = await asyncio.to_thread(
        subprocess.run, [sys.executable, "-c", script], capture_output=True, text=True,
        check=False, env={**os.environ, "PYTHONPATH": str(Path(config.__file__).parent.parent)},
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.asyncio
@pytest.mark.parametrize("route", ["save", "apply"])
async def test_precondition_required_and_authorization_unchanged(typed_app, monkeypatch, route):
    async with client(typed_app) as api:
        missing = await api.post(f"/_config/api/{route}", headers=HEADER,
                                 json={"settings": {"tables": []}})
        assert missing.status_code == 428
        monkeypatch.setenv("FSP_AUTHZ_ENFORCE", "1")
        monkeypatch.setattr(router_module, "_request_user", lambda _: None)
        denied = await api.post(f"/_config/api/{route}", headers=HEADER,
                                json={"settings": {"tables": []}})
        assert denied.status_code == 401


@pytest.mark.asyncio
@pytest.mark.parametrize("settings", [
    {"connections": [{**connection(), "database_url_reference": {
        "kind": "azure_key_vault", "reference": "https://vault/secrets/db",
    }}]},
    {"connections": [{**connection(), "database_url_reference": {
        "kind": "kubernetes_secret", "reference": "ns/secret",
    }}]},
    {"connections": [{**connection(), "database_url_reference": reference("missing")}]},
    {"connections": [{**connection(), "db_url": URL}]},
    {"connections": [{**connection(), "retry_backoff_seconds": 0.5}]},
    {"connections": [{**connection(), "query_timeout_seconds": True}]},
    {"connections": [connection("analytics")]},
    {"connections": [connection(), connection()]},
    {"tables": [{**table(), "connection": "unknown"}]},
    {"tables": [{**table(), "enabled": False}]},
    {"tables": [{**table(), "schema": [{**table()["schema"][0], "password": URL}]}]},
    {"tables": [{**table(), "split_strategy": "invented"}]},
    {"tables": [{**table(), "schema": [{
        "field_id": 1, "name": "id", "type": "invented",
    }]}]},
    {"tables": [{**table(), "schema": [
        table()["schema"][0], {
            "field_id": 2, "name": "private", "type": "string", "transform": {
                "kind": "deterministic_hash", "key_reference": reference("absent-key"),
            },
        },
    ]}]},
    {"tables": [table(), table()]},
    {"tables": [], "connections": [connection()]},
    {"db_url": URL},
])
async def test_unsupported_input_is_explicit_and_leaves_all_files_unchanged(
    typed_app, tmp_path, settings, caplog,
):
    async with client(typed_app) as api:
        current = await api.get("/_config/api/current", headers=HEADER)
        before = {p.name: p.read_bytes() for p in tmp_path.glob("*.json")}
        response = await api.post("/_config/api/apply", headers={
            **HEADER, "If-Match": current.headers["etag"],
        }, json={"settings": settings})
    assert response.status_code == 422, response.text
    assert response.json()["code"] == "configuration_not_representable"
    assert before == {p.name: p.read_bytes() for p in tmp_path.glob("*.json")}
    assert "typed-secret-sentinel" not in response.text + caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", [
    lambda d: d["connection"].update(db_url=URL),
    lambda d: d["connection"].pop("db_url_reference"),
    lambda d: d["connection"].update(db_retry_backoff=0.5),
    lambda d: d.update(connections=[{
        "id": "analytics", "db_url_reference": reference("analytics"), "future_setting": 123,
    }]),
])
async def test_legacy_configuration_rejected_not_projected_or_overwritten(
    typed_app, tmp_path, mutation,
):
    path = tmp_path / "config.connection.json"
    document = json.loads(path.read_text())
    mutation(document)
    path.write_text(json.dumps(document))
    before = path.read_bytes()
    async with client(typed_app) as api:
        native = await api.get("/_config/api/current")
        current = await api.get("/_config/api/current", headers=HEADER)
        result = await api.post("/_config/api/apply", headers={
            **HEADER, "If-Match": native.headers["etag"],
        }, json={"settings": {"connections": [connection()]}})
    assert native.status_code == 200
    assert current.status_code == result.status_code == 422
    assert path.read_bytes() == before
    assert "typed-secret-sentinel" not in current.text + result.text


@pytest.mark.asyncio
@pytest.mark.parametrize("canonical", [True, False])
async def test_transform_references_round_trip_without_secret_values(typed_app, tmp_path, canonical):
    store = CredentialStore(str(tmp_path / "credentials.json"))
    store.set_tokenization_key("customer-key", "tokenization-secret-sentinel-long-enough")
    key_reference = reference("customer-key") if canonical else {
        "kind": "fsp_credential_store", "reference": "customer-key",
    }
    transformed = table()
    transformed["schema"].append({
        "field_id": 2, "name": "customer", "source": "customer_email",
        "type": "string", "nullable": True,
        "transform": {
            "kind": "deterministic_hash", "key_reference": key_reference,
            "domain": "customer", "normalization": "trim_lower",
        },
    })
    async with client(typed_app) as api:
        current = await api.get("/_config/api/current", headers=HEADER)
        response = await api.post("/_config/api/apply", headers={
            **HEADER, "If-Match": current.headers["etag"],
        }, json={"settings": {"tables": [transformed]}})
        assert response.status_code == 200, response.text
        after = await api.get("/_config/api/current", headers=HEADER)
    assert after.json()["settings"]["tables"] == [transformed]
    assert response.json()["settings"] == after.json()["settings"]
    assert "tokenization-secret-sentinel" not in response.text + after.text
    saved = json.loads((tmp_path / "config.tables.json").read_text())
    assert saved["tables"][0]["schema"][1]["transform"]["key_ref"] == key_reference["reference"]
    credentials = (tmp_path / "credentials.json").read_bytes()
    result = await asyncio.to_thread(
        subprocess.run, [sys.executable, "-c", """
from fabric_shortcut_proxy import config
transform = config.TABLES[0].schema[1].transform
assert config.resolve_tokenization_key(transform.key_ref) == "tokenization-secret-sentinel-long-enough"
"""],
        capture_output=True, text=True, check=False,
        env={**os.environ, "ENABLE_CREDENTIAL_STORE": "1",
             "PYTHONPATH": str(Path(config.__file__).parent.parent)},
    )
    assert result.returncode == 0, result.stderr
    assert (tmp_path / "credentials.json").read_bytes() == credentials


@pytest.mark.asyncio
async def test_unknown_contract_never_falls_back_to_native_write(typed_app, tmp_path):
    async with client(typed_app) as api:
        before = {p.name: p.read_bytes() for p in tmp_path.glob("*.json")}
        for method, route, kwargs in [
            ("get", "current", {}),
            ("post", "apply", {"json": {"settings": {"num_splits": 99}}}),
        ]:
            response = await getattr(api, method)(
                f"/_config/api/{route}", headers={"X-FSP-Configuration-Contract": "99"}, **kwargs,
            )
            assert response.status_code == 406
    assert before == {p.name: p.read_bytes() for p in tmp_path.glob("*.json")}


@pytest.mark.asyncio
async def test_credential_rotation_changes_revision_and_rejects_old_write(typed_app, tmp_path):
    async with client(typed_app) as api:
        initial = await api.get("/_config/api/current", headers=HEADER)
        CredentialStore(str(tmp_path / "credentials.json")).set_url("default", URL + "rotated")
        after = await api.get("/_config/api/current", headers=HEADER)
        rejected = await api.post("/_config/api/apply", headers={
            **HEADER, "If-Match": initial.headers["etag"],
        }, json={"settings": {"tables": []}})
    assert initial.headers["etag"] != after.headers["etag"]
    assert rejected.status_code == 409


@pytest.mark.asyncio
async def test_typed_save_defaults_and_empty_tables_survive_restart(typed_app):
    async with client(typed_app) as api:
        initial = await api.get("/_config/api/current", headers=HEADER)
        saved = await api.post("/_config/api/save", headers={
            **HEADER, "If-Match": initial.headers["etag"],
        }, json={"settings": {"connections": [{
            "id": "default", "database_url_reference": reference("default"),
        }]}})
        assert saved.status_code == 200, saved.text
        assert saved.json()["settings"]["connections"] == [connection()]
        emptied = await api.post("/_config/api/save", headers={
            **HEADER, "If-Match": saved.headers["etag"],
        }, json={"settings": {"tables": []}})
        after = await api.get("/_config/api/current", headers=HEADER)
    assert emptied.status_code == after.status_code == 200
    assert after.json()["settings"]["tables"] == []
    result = await asyncio.to_thread(
        subprocess.run, [sys.executable, "-c",
                         "from fabric_shortcut_proxy import config; assert config.TABLES == []; config.validate_config()"],
        capture_output=True, text=True, check=False,
        env={**os.environ, "REQUIRE_SIGV4": "0", "AGENT_TOKEN_PREVIOUS": "",
             "PYTHONPATH": str(Path(config.__file__).parent.parent)},
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.asyncio
async def test_native_save_switches_url_and_reference_without_breaking_restart(typed_app, tmp_path):
    path = tmp_path / "config.connection.json"
    document = json.loads(path.read_text())
    document["connection"].pop("db_url_reference")
    document["connection"]["db_url"] = URL
    path.write_text(json.dumps(document))
    async with client(typed_app) as api:
        for updates, removed in [
            ({"db_url_reference": reference("default")}, "db_url"),
            ({"db_url": URL}, "db_url_reference"),
        ]:
            response = await api.post("/_config/api/save", json={"settings": updates})
            assert response.status_code == 200, response.text
            saved = json.loads(path.read_text())["connection"]
            assert removed not in saved
            assert all(saved[key] == value for key, value in updates.items())
            result = await asyncio.to_thread(
                subprocess.run, [sys.executable, "-c", """
from fabric_shortcut_proxy import connection_config
assert connection_config.DB_URL.endswith("/db")
"""],
                capture_output=True, text=True, check=False,
                env={**os.environ, "PYTHONPATH": str(Path(config.__file__).parent.parent)},
            )
            assert result.returncode == 0, result.stderr


@pytest.mark.asyncio
async def test_native_save_rejects_simultaneous_url_and_reference(typed_app, tmp_path):
    path = tmp_path / "config.connection.json"
    before = path.read_bytes()
    async with client(typed_app) as api:
        response = await api.post("/_config/api/save", json={"settings": {
            "db_url": URL, "db_url_reference": reference("default"),
        }})
    assert response.status_code == 400
    assert "mutually exclusive" in response.text
    assert path.read_bytes() == before


@pytest.mark.asyncio
@pytest.mark.parametrize("extra", [
    {"enabled": False},
    {"split_target_bytes": 1024},
    {"future_native_field": "preserve-me"},
    {"schema": [{"field_id": 1, "name": "id", "type": "long", "tokenization": {
        "action": "keep",
    }}]},
])
async def test_unrepresentable_native_table_fields_reject_read_and_write(
    typed_app, tmp_path, extra,
):
    path = tmp_path / "config.tables.json"
    path.write_text(json.dumps({"tables": [{**table(), **extra}]}))
    before = path.read_bytes()
    async with client(typed_app) as api:
        native = await api.get("/_config/api/current")
        typed = await api.get("/_config/api/current", headers=HEADER)
        result = await api.post("/_config/api/apply", headers={
            **HEADER, "If-Match": native.headers["etag"],
        }, json={"settings": {"tables": []}})
    assert typed.status_code == result.status_code == 422
    assert path.read_bytes() == before


@pytest.mark.asyncio
async def test_conflicting_native_env_is_not_reported_as_a_saved_reference(typed_app, monkeypatch):
    monkeypatch.setenv("DB_URL", URL + "unexpected")
    async with client(typed_app) as api:
        response = await api.get("/_config/api/current", headers=HEADER)
    assert response.status_code == 422
    assert "typed-secret-sentinel" not in response.text


@pytest.mark.asyncio
async def test_failure_to_persist_does_not_apply_live_or_claim_success(typed_app, monkeypatch, tmp_path):
    def fail_replace(*_):
        raise OSError("test write failure")

    monkeypatch.setattr(config, "apply_live_settings", lambda _: pytest.fail("must not apply live"))
    before = (tmp_path / "config.tables.json").read_bytes()
    async with client(typed_app) as api:
        initial = await api.get("/_config/api/current", headers=HEADER)
        monkeypatch.setattr(os, "replace", fail_replace)
        result = await api.post("/_config/api/apply", headers={
            **HEADER, "If-Match": initial.headers["etag"],
        }, json={"settings": {"tables": []}})
    assert result.status_code == 500
    assert result.json()["code"] == "configuration_persist_failed"
    assert (tmp_path / "config.tables.json").read_bytes() == before


@pytest.mark.asyncio
async def test_tables_validation_uses_saved_sources_not_import_time_registry(
    typed_app, monkeypatch,
):
    from dataclasses import replace

    stale = replace(config.CONNECTIONS["default"], db_url="databricks://host/db")
    monkeypatch.setattr(config, "CONNECTIONS", {"default": stale})
    changed = {**table(), "schema": []}
    async with client(typed_app) as api:
        initial = await api.get("/_config/api/current", headers=HEADER)
        assert initial.status_code == 200, initial.text
        result = await api.post("/_config/api/apply", headers={
            **HEADER, "If-Match": initial.headers["etag"],
        }, json={"settings": {"tables": [changed]}})
    assert result.status_code == 200, result.text


def test_typed_negotiation_is_documented_in_openapi(typed_app):
    paths = typed_app.openapi()["paths"]
    for route, method in [("current", "get"), ("apply", "post"), ("save", "post")]:
        parameters = paths[f"/_config/api/{route}"][method]["parameters"]
        assert any(item["name"] == "X-FSP-Configuration-Contract" for item in parameters)


@pytest.mark.asyncio
async def test_header_does_not_bypass_manager_auth_or_credential_read_boundary(
    typed_app, monkeypatch,
):
    from enterprise.control.auth import ManagerAuthMiddleware

    monkeypatch.setattr(config, "MANAGER_AUTH_ENABLED", True)
    monkeypatch.setattr(config, "MANAGER_AUTH_USERNAME", "test-operator")
    monkeypatch.setattr(config, "MANAGER_AUTH_PASSWORD", "test-password")
    monkeypatch.setattr(CredentialStore, "get_url", lambda *_: pytest.fail("unauthorized secret access"))
    typed_app.add_middleware(ManagerAuthMiddleware)
    async with client(typed_app) as api:
        response = await api.get("/_config/api/current", headers=HEADER)
        mutation = await api.post("/_config/api/apply", headers={
            **HEADER, "If-Match": '"fake"',
        }, json={"settings": {"tables": []}})
    assert response.status_code == mutation.status_code == 401


@pytest.mark.asyncio
async def test_snapshot_validation_uses_persisted_references_not_old_runtime_default(
    typed_app, monkeypatch,
):
    monkeypatch.setattr(config, "GENERATION_SOURCE_CONSISTENCY", "snapshot")
    monkeypatch.setattr(config, "MATERIALIZE_MODE", "eager")
    monkeypatch.setattr(config, "AUTO_REFRESH", False)
    monkeypatch.setattr(config, "AGENT_SHARD_COUNT", 1)
    monkeypatch.setattr(config, "DB_URL", "sqlite+aiosqlite:///:memory:")
    async with client(typed_app) as api:
        initial = await api.get("/_config/api/current", headers=HEADER)
        assert initial.status_code == 200, initial.text
        result = await api.post("/_config/api/apply", headers={
            **HEADER, "If-Match": initial.headers["etag"],
        }, json={"settings": {"connections": [connection(), connection("analytics")]}})
    assert result.status_code == 200, result.text


@pytest.mark.asyncio
async def test_bare_database_reference_compatibility_preserves_exact_readback(typed_app, tmp_path):
    path = tmp_path / "config.connection.json"
    native = json.loads(path.read_text())
    bare_reference = {"kind": "fsp_credential_store", "reference": "default"}
    native["connection"]["db_url_reference"] = bare_reference
    path.write_text(json.dumps(native))
    expected = [{**connection(), "database_url_reference": bare_reference}]
    async with client(typed_app) as api:
        initial = await api.get("/_config/api/current", headers=HEADER)
        assert initial.status_code == 200, initial.text
        assert initial.json()["settings"]["connections"] == expected
        response = await api.post("/_config/api/apply", headers={
            **HEADER, "If-Match": initial.headers["etag"],
        }, json={"settings": {"connections": expected}})
        assert response.status_code == 200, response.text
        final = await api.get("/_config/api/current", headers=HEADER)
    assert final.json()["settings"] == initial.json()["settings"] == response.json()["settings"]
    assert json.loads(path.read_text())["connection"]["db_url_reference"] == bare_reference


INVALID_REFERENCES = [
    "", "fsp-credential://", "fsp-credential:///default",
    "fsp-credential://default/", "fsp-credential://default/path",
    "fsp-credential://default?key=value", "fsp-credential://default#fragment",
    "fsp-credential://default?", "fsp-credential://default#",
    "fsp-credential://user:typed-secret-sentinel@default",
    "fsp-credential://user@default", "fsp-credential://default:443",
    "fsp-credential://%64efault", "fsp-credential://default%2Fpath",
    "fsp-credential://.", "fsp-credential://..", "fsp-credential://default.json",
    "FSP-CREDENTIAL://default", "https://default",
    " fsp-credential://default", "fsp-credential://default\n",
    "fsp-credential://" + "a" * 129,
]


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_reference", INVALID_REFERENCES)
@pytest.mark.parametrize("target", ["database", "transform"])
async def test_unsafe_credential_references_rejected_without_changes(
    typed_app, tmp_path, invalid_reference, target, caplog,
):
    supplied = {"kind": "fsp_credential_store", "reference": invalid_reference}
    if target == "database":
        updates = {"connections": [{**connection(), "database_url_reference": supplied}]}
    else:
        transformed = table()
        transformed["schema"].append({
            "field_id": 2, "name": "private", "type": "string", "transform": {
                "kind": "deterministic_hash", "key_reference": supplied,
            },
        })
        updates = {"tables": [transformed]}
    before = {p.name: p.read_bytes() for p in tmp_path.glob("*.json")}
    async with client(typed_app) as api:
        current = await api.get("/_config/api/current", headers=HEADER)
        assert current.status_code == 200, current.text
        response = await api.post("/_config/api/apply", headers={
            **HEADER, "If-Match": current.headers["etag"],
        }, json={"settings": updates})
    assert response.status_code == 422, response.text
    assert response.json()["code"] == "configuration_not_representable"
    assert before == {p.name: p.read_bytes() for p in tmp_path.glob("*.json")}
    assert "typed-secret-sentinel" not in response.text + caplog.text


@pytest.mark.parametrize("record_id", ["default", "Record_ID-2", "a" * 128])
def test_credential_reference_parser_extracts_only_record_id(record_id):
    assert credential_record_id(f"fsp-credential://{record_id}") == record_id
    assert credential_record_id(record_id) == record_id


@pytest.mark.parametrize("invalid_reference", INVALID_REFERENCES)
def test_native_reference_parser_also_rejects_unsafe_syntax(invalid_reference):
    with pytest.raises(ValueError, match="credential reference must be"):
        credential_record_id(invalid_reference)

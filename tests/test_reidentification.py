from __future__ import annotations

import json

import pytest
from fastapi import FastAPI
import httpx

import config
import module_registry
from config import ColumnDef, ColumnTransform, TableDef
from observability import audit
from reidentification.gate import enabled
from reidentification.mappings import (
    LookupMappings,
    ReidentificationMappingError,
    default_mappings_path,
)
from reidentification.source_lookup import build_lookup_query
from reidentification.limits import RequestLimiter
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


def test_lookup_mapping_requires_matching_enabled_durable_policy(tmp_path, monkeypatch):
    policy_file = tmp_path / "policies.json"
    policy_file.write_text(
        '{"policies":[{"policy_id":"customer-pii-v1","kind":"durable_token",'
        '"algorithm":"sha256","key_ref":"customer-pii-v1","domain":"customer-email",'
        '"normalization":"trim_lower","digest_size":32,"framing_version":1,"enabled":true}]}',
        encoding="utf-8",
    )
    table = TableDef(
        name="customers_safe", source_table="dbo.customers", key_column="customer_id",
        schema=[
            ColumnDef(field_id=1, name="customer_id", iceberg_type="long", nullable=False),
            ColumnDef(
                field_id=2, name="email_token", source="email", iceberg_type="string",
                transform=ColumnTransform(
                    kind="deterministic_hash", key_ref="customer-pii-v1",
                    domain="customer-email", normalization="trim_lower",
                ), policy_id="customer-pii-v1",
            ),
        ],
    )
    monkeypatch.setenv("TOKENIZATION_POLICY_FILE", str(policy_file))
    monkeypatch.setattr(config, "TABLES", [table])
    mappings = LookupMappings.from_dict({"mappings": [{
        "policy_id": "customer-pii-v1", "table_id": "customers_safe",
        "column_id": "email_token", "lookup_column": "email_token_lookup",
        "clear_text_column": "email", "primary_key_column": "customer_id",
    }]})

    mappings.validate()
    assert mappings.list_public()[0]["lookup_column"] == "email_token_lookup"
    with pytest.raises(ReidentificationMappingError, match="not assigned"):
        LookupMappings.from_dict({"mappings": [{
            "policy_id": "customer-pii-v1", "table_id": "customers_safe",
            "column_id": "missing", "lookup_column": "email_token_lookup",
            "clear_text_column": "email", "primary_key_column": "customer_id",
        }]}).validate()


def test_lookup_mapping_uses_config_directory_unless_explicit(monkeypatch, tmp_path):
    monkeypatch.setenv("FSP_CONFIG_DIR", str(tmp_path))
    assert default_mappings_path() == str(tmp_path / "config.reidentification.json")
    monkeypatch.setenv("REIDENTIFICATION_MAPPING_FILE", "D:/secure/reidentification.json")
    assert default_mappings_path() == "D:/secure/reidentification.json"


@pytest.mark.parametrize(
    ("db_url", "expected", "unexpected"),
    [
        ("mssql+aioodbc://h/db", "SELECT TOP (:__reidentification_limit)", "LIMIT"),
        ("postgresql+asyncpg://h/db", "LIMIT :__reidentification_limit", "TOP"),
        ("oracle+oracledb://h/db", "FETCH FIRST :__reidentification_limit ROWS ONLY", "TOP"),
        ("databricks://token:x@h?http_path=/sql/1.0/warehouses/x", "LIMIT :__reidentification_limit", "TOP"),
    ],
)
def test_lookup_query_is_bounded_and_parameterized(monkeypatch, db_url, expected, unexpected):
    table = TableDef(
        name="customers_safe", source_table="dbo.customers", key_column="customer_id",
        schema=[],
    )
    mapping = LookupMappings.from_dict({"mappings": [{
        "policy_id": "customer-pii-v1", "table_id": "customers_safe",
        "column_id": "email_token", "lookup_column": "email_token_lookup",
        "clear_text_column": "email", "primary_key_column": "customer_id",
    }]}).list_public()[0]
    from reidentification.mappings import LookupMapping

    monkeypatch.setattr(config, "TABLES", [table])
    monkeypatch.setattr(config, "effective_db_url", lambda connection: db_url)
    sql, params, connection = build_lookup_query(
        LookupMapping.from_dict(mapping), "A" * 64
    )

    assert expected in sql and unexpected not in sql
    assert "A" * 64 not in sql
    assert params == {"__token_reidentification": "A" * 64, "__reidentification_limit": 2}
    assert connection == "default"


def test_lookup_query_rejects_invalid_token_and_unsupported_dialect(monkeypatch):
    table = TableDef(name="customers_safe", source_table="customers", schema=[])
    mapping = LookupMappings.from_dict({"mappings": [{
        "policy_id": "customer-pii-v1", "table_id": "customers_safe",
        "column_id": "email_token", "lookup_column": "email_token_lookup",
        "clear_text_column": "email", "primary_key_column": "customer_id",
    }]}).list_public()[0]
    from reidentification.mappings import LookupMapping

    monkeypatch.setattr(config, "TABLES", [table])
    monkeypatch.setattr(config, "effective_db_url", lambda connection: "sqlite+aiosqlite:///x.db")
    with pytest.raises(ReidentificationMappingError, match="64-character"):
        build_lookup_query(LookupMapping.from_dict(mapping), "not-a-token")
    with pytest.raises(ReidentificationMappingError, match="not supported"):
        build_lookup_query(LookupMapping.from_dict(mapping), "A" * 64)


def test_reidentification_request_limits_per_minute_and_day():
    limiter = RequestLimiter()
    assert limiter.allow("auditor", per_minute=2, per_day=3, now=0).allowed
    assert limiter.allow("auditor", per_minute=2, per_day=3, now=1).allowed
    assert limiter.allow("auditor", per_minute=2, per_day=3, now=2).reason == "re-identification minute quota exceeded"
    assert limiter.allow("auditor", per_minute=2, per_day=3, now=61).allowed
    assert limiter.allow("auditor", per_minute=2, per_day=3, now=62).reason == "re-identification daily quota exceeded"


@pytest.mark.asyncio
async def test_reidentification_route_is_auditor_only_and_redacted(tmp_path, monkeypatch):
    import reidentification.router as reidentification_router

    monkeypatch.setattr(config, "ENABLE_AUDIT_LOG", True, raising=False)
    monkeypatch.setattr(config, "AUDIT_LOG_FILE", str(tmp_path / "audit.jsonl"), raising=False)
    monkeypatch.setenv("ADMIN_TOKEN", "admin-test-token")
    identity_path = tmp_path / "identities.json"
    monkeypatch.setenv("FSP_IDENTITY_FILE", str(identity_path))
    provider = IdentityProvider(str(identity_path))
    provider.create_or_replace(User("auditor", roles=("auditor",)), "correct horse battery staple")
    auditor = provider.authenticate("auditor", "correct horse battery staple")
    session = identity_provider().create_session(auditor)
    audit._buf.clear()
    mapping = LookupMappings.from_dict({"mappings": [{
        "policy_id": "customer-pii-v1", "table_id": "customers_safe",
        "column_id": "email_token", "lookup_column": "email_token_lookup",
        "clear_text_column": "email", "primary_key_column": "customer_id",
    }]})
    async def fake_lookup_rows(mapping, token):
        assert token == "A" * 64
        return [{"__reidentify_primary_key": 123, "__reidentify_value": "alice@example.com"}]
    monkeypatch.setattr(reidentification_router, "_mappings", mapping)
    monkeypatch.setattr(reidentification_router, "lookup_rows", fake_lookup_rows)

    app = FastAPI()
    app.add_middleware(AuthorizationMiddleware)
    app.include_router(router)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test",
        cookies={"fsp_session": session},
    ) as client:
        allowed = await client.post(
            "/_reidentify/api/v1/lookup/customer-pii-v1/customers_safe/email_token",
            json={"token": "A" * 64, "reason_code": "audit", "case_reference": "CASE-123"},
        )
        denied = await client.post(
            "/_reidentify/api/v1/lookup/customer-pii-v1/customers_safe/email_token",
            headers={"X-Admin-Token": "admin-test-token"},
        )

    assert allowed.status_code == 200
    assert allowed.json()["primary_key"] == 123
    assert allowed.json()["value"] == "alice@example.com"
    assert denied.status_code == 403
    events = audit.recent()
    assert events[-2]["action"] == "reidentification_request"
    assert events[-2]["identity"] == "auditor"
    assert events[-2]["outcome"] == "success"
    assert events[-2]["token_fingerprint"] != "A" * 16
    assert events[-2]["case_reference"] == "CASE-123"
    assert events[-1]["request_id"] != "-"
    assert events[-1]["action"] == "reidentification_request"
    assert events[-1]["identity"] == "admin-token"
    assert events[-1]["status"] == 403
    assert all(isinstance(event["ts"], float) for event in events[-2:])
    assert all("value" not in event and "token" not in event for event in events[-2:])
    persisted = [
        json.loads(line)
        for line in (tmp_path / "audit.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert persisted[-2]["identity"] == "auditor"
    forbidden = {"value", "clear_text", "token", "sql", "params", "key", "credential"}
    assert all(not forbidden.intersection(event) for event in persisted)


@pytest.mark.asyncio
async def test_reidentification_fails_closed_when_durable_audit_is_unavailable(tmp_path, monkeypatch):
    import reidentification.router as reidentification_router

    monkeypatch.setattr(config, "ENABLE_AUDIT_LOG", True, raising=False)
    monkeypatch.setattr(config, "AUDIT_LOG_FILE", "", raising=False)
    identity_path = tmp_path / "identities.json"
    monkeypatch.setenv("FSP_IDENTITY_FILE", str(identity_path))
    provider = IdentityProvider(str(identity_path))
    provider.create_or_replace(User("auditor", roles=("auditor",)), "correct horse battery staple")
    session = identity_provider().create_session(provider.authenticate("auditor", "correct horse battery staple"))
    monkeypatch.setattr(reidentification_router, "_mappings", LookupMappings())
    app = FastAPI()
    app.add_middleware(AuthorizationMiddleware)
    app.include_router(router)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"fsp_session": session}
    ) as client:
        response = await client.post("/_reidentify/api/v1/lookup/missing/table/column", json={})
    assert response.status_code == 503
    assert response.json()["error"] == "audit service unavailable"
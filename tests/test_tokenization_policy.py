from __future__ import annotations

import config
import pytest
from tokenization.policy import (
    algorithm_specs,
    default_registry_path,
    TokenizationPolicy,
    TokenizationPolicyError,
    TokenizationPolicyRegistry,
    TokenizationSelection,
    legacy_policy,
    load_registry,
    load_default_registry,
    policy_fingerprint,
    save_registry,
    selection_from_transform,
)


def test_legacy_sha256_transform_normalizes_to_durable_policy():
    transform = config.ColumnTransform(
        kind="deterministic_hash",
        key_ref="customer-pii-v1",
        domain="customer-email",
        normalization="trim_lower",
    )
    policy = legacy_policy(transform)

    assert policy.kind == "durable_token"
    assert policy.transform_kind == "deterministic_hash"
    assert policy.algorithm == "sha256"
    assert policy.to_public()["key_ref"] == "customer-pii-v1"
    assert "secret" not in policy.to_public()


def test_policy_builds_compatible_sha256_token(monkeypatch):
    monkeypatch.setenv("FSP_TOKENIZATION_KEY_CUSTOMER_PII_V1", "uat-secret")
    transform = config.ColumnTransform(
        kind="deterministic_hash",
        key_ref="customer-pii-v1",
        domain="customer-email",
        normalization="trim_lower",
    )
    policy = legacy_policy(transform)
    assert policy.deterministic_token(" Alice@Example.com ") == (
        "15A720CD53AD7C0ADBB4010CE8EF57EBBA3AD5CFCF63F315007B314E59FAE31D"
    )


def test_random_legacy_transform_has_no_key():
    policy = legacy_policy(config.ColumnTransform(kind="random_token"))
    assert policy.kind == "random_token"
    assert policy.transform_kind == "random_token"
    assert policy.key_ref is None


def test_table_selector_contains_only_action_and_policy_id():
    transform = config.ColumnTransform(
        kind="deterministic_hash", key_ref="customer-pii-v1"
    )
    selection = selection_from_transform(transform)
    assert selection.to_dict() == {
        "action": "durable_token", "policy_id": "legacy-inline"
    }
    assert TokenizationSelection("keep").to_dict() == {"action": "keep"}
    assert TokenizationSelection("remove").to_dict() == {"action": "remove"}


def test_table_selector_rejects_policy_details_on_keep_or_remove():
    with pytest.raises(TokenizationPolicyError, match="must not reference"):
        TokenizationSelection("keep", policy_id="secret-policy")
    with pytest.raises(TokenizationPolicyError, match="requires a policy_id"):
        TokenizationSelection("random_token")


def test_registry_rejects_unknown_and_disabled_policies():
    registry = TokenizationPolicyRegistry([
        TokenizationPolicy(
            policy_id="active",
            kind="durable_token",
            key_ref="key-v1",
        ),
        TokenizationPolicy(
            policy_id="disabled",
            kind="durable_token",
            key_ref="key-v1",
            enabled=False,
        ),
    ])
    assert registry.get("active").policy_id == "active"
    with pytest.raises(TokenizationPolicyError, match="unknown"):
        registry.get("missing")
    with pytest.raises(TokenizationPolicyError, match="disabled"):
        registry.get("disabled")


def test_registry_enforces_selector_policy_kind():
    registry = TokenizationPolicyRegistry([
        TokenizationPolicy(policy_id="durable", kind="durable_token", key_ref="k"),
        TokenizationPolicy(policy_id="random", kind="random_token"),
    ])
    assert registry.resolve_selection(
        TokenizationSelection("durable_token", "durable")
    ).policy_id == "durable"
    assert registry.selection("random").to_dict() == {
        "action": "random_token", "policy_id": "random"
    }
    with pytest.raises(TokenizationPolicyError, match="not 'random_token'"):
        registry.resolve_selection(TokenizationSelection("random_token", "durable"))
    with pytest.raises(TokenizationPolicyError, match="does not resolve"):
        registry.resolve_selection(TokenizationSelection("keep"))


def test_registry_round_trips_secret_free_json_shape():
    registry = TokenizationPolicyRegistry([
        TokenizationPolicy(
            policy_id="customer-pii-v1", kind="durable_token",
            key_ref="customer-pii-v1", domain="customer-email",
            normalization="trim_lower",
        ),
        TokenizationPolicy(policy_id="support-v1", kind="random_token"),
    ])
    loaded = TokenizationPolicyRegistry.from_dict(registry.to_dict())
    assert loaded.list_public() == registry.list_public()
    assert "secret" not in str(registry.to_dict()).lower()


def test_policy_json_rejects_secret_material():
    with pytest.raises(TokenizationPolicyError, match="key material"):
        TokenizationPolicy.from_dict({
            "policy_id": "bad", "kind": "durable_token",
            "key_ref": "customer-pii-v1", "secret": "do-not-store",
        })


def test_registry_file_round_trip_and_missing_file(tmp_path):
    path = tmp_path / "tokenization.json"
    empty = load_registry(str(path))
    assert empty.list_public() == []
    registry = TokenizationPolicyRegistry([
        TokenizationPolicy(policy_id="support-v1", kind="random_token")
    ])
    save_registry(str(path), registry)
    assert load_registry(str(path)).list_public() == registry.list_public()


def test_default_registry_uses_operator_selected_path(tmp_path, monkeypatch):
    path = tmp_path / "central-policies.json"
    monkeypatch.setenv("TOKENIZATION_POLICY_FILE", str(path))
    registry = TokenizationPolicyRegistry([
        TokenizationPolicy(policy_id="support-v1", kind="random_token")
    ])
    save_registry(str(path), registry)
    assert default_registry_path() == str(path)
    assert load_default_registry().list_public() == registry.list_public()


async def test_config_builder_policy_catalog_is_secret_free(tmp_path, monkeypatch):
    import httpx
    from fastapi import FastAPI

    path = tmp_path / "central-policies.json"
    monkeypatch.setenv("TOKENIZATION_POLICY_FILE", str(path))
    save_registry(str(path), TokenizationPolicyRegistry([
        TokenizationPolicy(
            policy_id="customer-pii-v1", kind="durable_token",
            key_ref="customer-pii-v1", domain="customer-email",
        ),
    ]))
    from configbuilder.router import router

    app = FastAPI()
    app.include_router(router)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/_config/api/tokenization/policies")
    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True
    assert body["algorithms"] == ["blake2b", "sha256"]
    assert body["policies"][0]["key_ref"] == "customer-pii-v1"
    assert "uat-secret" not in response.text


async def test_config_builder_policy_catalog_reports_malformed_file(tmp_path, monkeypatch):
    import httpx
    from fastapi import FastAPI

    path = tmp_path / "central-policies.json"
    path.write_text("{broken", encoding="utf-8")
    monkeypatch.setenv("TOKENIZATION_POLICY_FILE", str(path))
    from configbuilder.router import router

    app = FastAPI()
    app.include_router(router)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/_config/api/tokenization/policies")
    assert response.status_code == 503
    assert response.json()["ok"] is False


async def test_config_builder_policy_mutation_requires_admin_and_never_stores_secret(tmp_path, monkeypatch):
    import httpx
    from fastapi import FastAPI

    path = tmp_path / "central-policies.json"
    monkeypatch.setenv("TOKENIZATION_POLICY_FILE", str(path))
    monkeypatch.setenv("ADMIN_TOKEN", "admin-test-token")
    from configbuilder.router import router

    app = FastAPI()
    app.include_router(router)
    payload = {
        "policy_id": "customer-pii-v1", "kind": "durable_token",
        "key_ref": "customer-pii-v1", "domain": "customer-email",
    }
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        denied = await client.post("/_config/api/tokenization/policies", json=payload)
        allowed = await client.post(
            "/_config/api/tokenization/policies", json=payload,
            headers={"X-Admin-Token": "admin-test-token"},
        )
    assert denied.status_code == 401
    assert allowed.status_code == 200
    assert "admin-test-token" not in path.read_text(encoding="utf-8")
    assert allowed.json()["policy"]["key_ref"] == "customer-pii-v1"


async def test_config_builder_policy_mutation_rejects_secret_field(tmp_path, monkeypatch):
    import httpx
    from fastapi import FastAPI

    path = tmp_path / "central-policies.json"
    monkeypatch.setenv("TOKENIZATION_POLICY_FILE", str(path))
    monkeypatch.setenv("ADMIN_TOKEN", "admin-test-token")
    from configbuilder.router import router

    app = FastAPI()
    app.include_router(router)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/_config/api/tokenization/policies",
            json={"policy_id": "bad", "kind": "durable_token", "key_ref": "k", "secret": "x"},
            headers={"X-Admin-Token": "admin-test-token"},
        )
    assert response.status_code == 400
    assert "key material" in response.json()["error"]


async def test_config_builder_supports_multiple_named_policies(tmp_path, monkeypatch):
    import httpx
    from fastapi import FastAPI

    path = tmp_path / "central-policies.json"
    monkeypatch.setenv("TOKENIZATION_POLICY_FILE", str(path))
    monkeypatch.setenv("ADMIN_TOKEN", "admin-test-token")
    from configbuilder.router import router

    app = FastAPI()
    app.include_router(router)
    durable = {
        "policy_id": "customer-email-v1", "kind": "durable_token",
        "key_ref": "customer-pii-v1", "domain": "customer-email",
        "normalization": "trim_lower",
    }
    random = {
        "policy_id": "support-note-v1", "kind": "random_token",
        "algorithm": "sha256", "digest_size": 32,
    }
    headers = {"X-Admin-Token": "admin-test-token"}
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        first = await client.post("/_config/api/tokenization/policies", json=durable, headers=headers)
        second = await client.post("/_config/api/tokenization/policies", json=random, headers=headers)
        catalog = await client.get("/_config/api/tokenization/policies")
    assert first.status_code == second.status_code == 200
    policies = catalog.json()["policies"]
    assert [policy["policy_id"] for policy in policies] == [
        "customer-email-v1", "support-note-v1"
    ]
    assert policies[0]["kind"] == "durable_token"
    assert policies[1]["kind"] == "random_token"


def test_policy_fingerprint_is_stable_and_secret_free():
    policy = TokenizationPolicy(
        policy_id="customer-pii-v1",
        kind="durable_token",
        key_ref="customer-pii-v1",
        domain="customer-email",
        normalization="trim_lower",
    )
    fingerprint = policy_fingerprint(policy)
    assert fingerprint == policy_fingerprint(policy)
    assert len(fingerprint) == 24
    assert "customer-secret" not in fingerprint


def test_policy_rejects_unsupported_algorithm_and_invalid_kind():
    with pytest.raises(TokenizationPolicyError, match="algorithm"):
        TokenizationPolicy(policy_id="bad", kind="durable_token", algorithm="sha512", key_ref="k")
    with pytest.raises(TokenizationPolicyError, match="kind"):
        TokenizationPolicy(policy_id="bad", kind="deterministic_hash", key_ref="k")


def test_blake2b_is_arrow_ready_without_native_claim(monkeypatch):
    monkeypatch.setenv("FSP_TOKENIZATION_KEY_CUSTOMER_PII_V1", "uat-secret")
    policy = TokenizationPolicy(
        policy_id="customer-pii-b2-v1",
        kind="durable_token",
        algorithm="blake2b",
        key_ref="customer-pii-v1",
        digest_size=32,
    )
    assert len(policy.deterministic_token("alice@example.com")) == 64
    assert "blake2b" in {spec.name for spec in algorithm_specs()}

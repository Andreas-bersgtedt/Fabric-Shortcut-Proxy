from __future__ import annotations

import config
import pytest
from tokenization.policy import (
    algorithm_specs,
    TokenizationPolicy,
    TokenizationPolicyError,
    TokenizationPolicyRegistry,
    TokenizationSelection,
    legacy_policy,
    policy_fingerprint,
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

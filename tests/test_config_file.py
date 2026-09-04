"""
External JSON config tests:
  - precedence: env var > config.json > default
  - _tabledef_from_json parsing (minimal + explicit schema)
  - file loading, BOM tolerance, and missing-file behavior
"""
from __future__ import annotations

import json

import config
import pytest


# ---------------------------------------------------------------------------
# Precedence (use fake env names to avoid clashing with other modules)
# ---------------------------------------------------------------------------

def test_precedence_env_over_json_over_default(monkeypatch):
    monkeypatch.setattr(config, "_FILE_CFG", {"jk": 5})
    assert config._get_int("FAKE_ENV_XYZ", "jk", 8) == 5      # json when env absent
    monkeypatch.setenv("FAKE_ENV_XYZ", "9")
    assert config._get_int("FAKE_ENV_XYZ", "jk", 8) == 9      # env overrides json
    assert config._get_int("OTHER_FAKE", "nokey", 3) == 3     # default when neither


def test_get_str_and_bool_from_json(monkeypatch):
    monkeypatch.setattr(config, "_FILE_CFG", {"b": "jsonval", "flag": True, "flag2": "yes"})
    assert config._get_str("FAKE_STR_ENV", "b", "def") == "jsonval"
    assert config._get_bool("FAKE_BOOL_ENV", "flag", False) is True
    assert config._get_bool("FAKE_BOOL_ENV2", "flag2", False) is True
    assert config._get_bool("FAKE_BOOL_ENV3", "missing", True) is True


# ---------------------------------------------------------------------------
# Table parsing
# ---------------------------------------------------------------------------

def test_tabledef_from_json_minimal():
    t = config._tabledef_from_json({"source_table": "public.orders", "key_column": "order_id"})
    assert t.name == "orders"                 # derived from source last segment
    assert t.source_table == "public.orders"
    assert t.key_column == "order_id"
    assert t.schema is None                    # reflected at startup
    assert t.num_splits == config.NUM_SPLITS


def test_tabledef_from_json_with_schema():
    t = config._tabledef_from_json({
        "name": "ev",
        "source_table": "analytics.events",
        "num_splits": 3,
        "schema": [
            {"field_id": 1, "name": "event_id", "type": "long", "nullable": False},
            {"field_id": 2, "name": "payload", "type": "string"},
        ],
    })
    assert t.name == "ev"
    assert t.num_splits == 3
    assert [c.name for c in t.schema] == ["event_id", "payload"]
    assert t.schema[0].iceberg_type == "long"
    assert t.schema[0].nullable is False
    assert t.schema[1].nullable is True        # default


def test_tabledef_from_json_with_tokenization(monkeypatch):
    t = config._tabledef_from_json({
        "name": "customers_safe",
        "source_table": "dbo.customers",
        "key_column": "customer_id",
        "schema": [
            {"field_id": 1, "name": "customer_id", "type": "long"},
            {
                "field_id": 2,
                "name": "email_token",
                "source": "email",
                "type": "string",
                "transform": {
                    "kind": "deterministic_hash",
                    "key_ref": "customer-pii-v1",
                    "domain": "customer-email",
                    "normalization": "trim_lower",
                },
            },
        ],
    })
    token = t.schema[1]
    assert token.source_name == "email"
    assert token.transform.kind == "deterministic_hash"
    assert token.transform.domain == "customer-email"
    assert config.tokenization_key_env_var("customer-pii-v1") == (
        "FSP_TOKENIZATION_KEY_CUSTOMER_PII_V1"
    )
    monkeypatch.setenv("FSP_TOKENIZATION_KEY_CUSTOMER_PII_V1", "uat-secret")
    assert config.resolve_tokenization_key("customer-pii-v1") == "uat-secret"


def test_tabledef_from_json_with_central_policy_selectors(tmp_path, monkeypatch):
    policy_file = tmp_path / "tokenization.json"
    policy_file.write_text(json.dumps({"policies": [{
        "policy_id": "customer-pii-v1", "kind": "durable_token",
        "key_ref": "customer-pii-v1", "domain": "customer-email",
        "normalization": "trim_lower",
    }]}), encoding="utf-8")
    monkeypatch.setenv("TOKENIZATION_POLICY_FILE", str(policy_file))
    table = config._tabledef_from_json({
        "name": "customers_safe", "source_table": "dbo.customers",
        "key_column": "customer_id", "schema": [
            {"field_id": 1, "name": "customer_id", "type": "long"},
            {"field_id": 2, "name": "email_token", "source": "email", "type": "string",
             "tokenization": {"action": "durable_token", "policy_id": "customer-pii-v1"}},
            {"field_id": 3, "name": "ssn", "type": "string",
             "tokenization": {"action": "remove"}},
        ],
    })
    assert [column.name for column in table.schema] == ["customer_id", "email_token"]
    token = table.schema[1]
    assert token.policy_id == "customer-pii-v1"
    assert token.transform.kind == "deterministic_hash"
    assert token.transform.normalization == "trim_lower"


def test_tabledef_central_policy_selector_fails_closed(tmp_path, monkeypatch):
    policy_file = tmp_path / "tokenization.json"
    policy_file.write_text(json.dumps({"policies": [{
        "policy_id": "random-v1", "kind": "random_token",
    }]}), encoding="utf-8")
    monkeypatch.setenv("TOKENIZATION_POLICY_FILE", str(policy_file))
    with pytest.raises(ValueError, match="not 'durable_token'"):
        config._tabledef_from_json({
            "source_table": "dbo.customers", "key_column": "id", "schema": [
                {"field_id": 1, "name": "email", "type": "string",
                 "tokenization": {"action": "durable_token", "policy_id": "random-v1"}},
            ],
        })


def test_column_transform_validation():
    with pytest.raises(ValueError, match="requires a non-empty key_ref"):
        config.ColumnTransform(kind="deterministic_hash")
    with pytest.raises(ValueError, match="must use Iceberg type 'string'"):
        config.ColumnDef(
            field_id=1,
            name="bad_token",
            iceberg_type="long",
            transform=config.ColumnTransform(
                kind="deterministic_hash", key_ref="key-v1"
            ),
        )


def test_missing_tokenization_key_fails_closed(monkeypatch):
    monkeypatch.delenv("FSP_TOKENIZATION_KEY_CUSTOMER_PII_V1", raising=False)
    with pytest.raises(ValueError, match="FSP_TOKENIZATION_KEY_CUSTOMER_PII_V1"):
        config.resolve_tokenization_key("customer-pii-v1")


# ---------------------------------------------------------------------------
# File loading
# ---------------------------------------------------------------------------

def test_load_config_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.performance.json").write_text(json.dumps({"num_splits": 4}), encoding="utf-8")
    assert config._load_config_file()["performance"]["num_splits"] == 4


def test_load_config_file_tolerates_bom(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.freshness.json").write_text(json.dumps({"refresh_strategy": "ttl"}), encoding="utf-8-sig")  # UTF-8 BOM
    assert config._load_config_file()["freshness"]["refresh_strategy"] == "ttl"


def test_load_config_file_missing_is_empty(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert config._load_config_file() == {}


def test_write_config_updates_persists_keyvault_settings(tmp_path, monkeypatch):
    # Regression: the Key Vault keys must route to config.system.json (were dropped
    # by write_config_updates because they were missing from _SETTINGS_TO_FILE_MAP).
    monkeypatch.chdir(tmp_path)
    result = config.write_config_updates({
        "keyvault_uri": "https://fabproxy01.vault.azure.net/",
        "auth_mode": "managed_identity",
        "require_keyvault": True,
        "keyvault_refresh_seconds": 120,
    })
    assert "keyvault_uri" in result["changed"]
    saved = json.loads((tmp_path / "config.system.json").read_text(encoding="utf-8"))["system"]
    assert saved["keyvault_uri"] == "https://fabproxy01.vault.azure.net/"
    assert saved["auth_mode"] == "managed_identity"
    assert saved["require_keyvault"] is True
    assert saved["keyvault_refresh_seconds"] == 120


def test_write_config_updates_persists_open_mirror_limits(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    result = config.write_config_updates({
        "open_mirror_max_rows": 10_000,
        "open_mirror_max_pages_per_cycle": 1,
        "open_mirror_max_rows_per_cycle": 10_000,
        "open_mirror_preflight_timeout_seconds": 30,
        "open_mirror_fabric_retry_attempts": 2,
    })
    assert "open_mirror_max_pages_per_cycle" in result["changed"]
    saved = json.loads((tmp_path / "config.system.json").read_text(encoding="utf-8"))["system"]
    assert saved["open_mirror_max_rows"] == 10_000
    assert saved["open_mirror_max_pages_per_cycle"] == 1
    assert saved["open_mirror_max_rows_per_cycle"] == 10_000
    assert saved["open_mirror_preflight_timeout_seconds"] == 30
    assert saved["open_mirror_fabric_retry_attempts"] == 2


def test_effective_settings_reads_system_file(tmp_path, monkeypatch):
    # Regression: system settings (e.g. keyvault_uri) must resolve from
    # config.system.json in the live editor, not silently fall back to default
    # (which made a just-saved value appear to vanish on reload).
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.system.json").write_text(
        json.dumps({"system": {"keyvault_uri": "https://v.vault.azure.net/", "enable_gateway": True}}),
        encoding="utf-8")
    by_key = {s["key"]: s for s in config.effective_settings()}
    assert by_key["keyvault_uri"]["value"] == "https://v.vault.azure.net/"
    assert by_key["keyvault_uri"]["source"] == "file"
    assert by_key["enable_gateway"]["value"] is True
    assert by_key["enable_gateway"]["source"] == "file"

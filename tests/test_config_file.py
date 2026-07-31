"""
External JSON config tests:
  - precedence: env var > config.json > default
  - _tabledef_from_json parsing (minimal + explicit schema)
  - file loading, BOM tolerance, and missing-file behavior
"""
from __future__ import annotations

import json

import config


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

"""
Hardening regression tests (Plan item H9).

Locks in the correctness that was hard-won during Fabric bring-up so it can't
silently regress:
  - HTTP Range handling incl. the Parquet-footer suffix form ``bytes=-N``
  - deterministic, restart-stable snapshot identifiers
  - Iceberg v2 metadata carries all reader-required fields
  - startup config validation + DB-URL secret redaction (H7)

Pure unit tests (no HTTP server) so they stay fast and deterministic in CI.
"""
from __future__ import annotations

import json
import os

os.environ.setdefault("DB_URL", "sqlite+aiosqlite:///:memory:")
os.environ.setdefault("NUM_SPLITS", "4")
os.environ.setdefault("S3_BUCKET", "test-bucket")

import pytest

import config
from s3.router import _apply_range
from iceberg.state_store import build_snapshot
from iceberg.metadata import build_metadata_json


# ---------------------------------------------------------------------------
# HTTP Range handling (Parquet footer reads depend on suffix ranges)
# ---------------------------------------------------------------------------

_DATA = bytes(range(256)) * 4  # 1024 deterministic bytes


def test_range_none_returns_full_not_partial():
    sliced, start, end, partial = _apply_range(_DATA, None)
    assert sliced == _DATA
    assert (start, end, partial) == (0, len(_DATA) - 1, False)


def test_range_explicit_start_end():
    sliced, start, end, partial = _apply_range(_DATA, "bytes=0-3")
    assert sliced == _DATA[0:4]
    assert (start, end, partial) == (0, 3, True)


def test_range_open_ended():
    sliced, start, end, partial = _apply_range(_DATA, "bytes=1000-")
    assert sliced == _DATA[1000:]
    assert (start, end, partial) == (1000, len(_DATA) - 1, True)


def test_range_suffix_last_n_bytes():
    # bytes=-8 must return the LAST 8 bytes (Parquet footer read), NOT the first 8.
    sliced, start, end, partial = _apply_range(_DATA, "bytes=-8")
    assert sliced == _DATA[-8:]
    assert (start, end, partial) == (len(_DATA) - 8, len(_DATA) - 1, True)


def test_range_suffix_larger_than_object_clamped():
    sliced, start, end, partial = _apply_range(_DATA, "bytes=-99999")
    assert sliced == _DATA
    assert (start, end, partial) == (0, len(_DATA) - 1, True)


def test_range_unparseable_returns_full():
    sliced, start, end, partial = _apply_range(_DATA, "bytes=abc")
    assert sliced == _DATA
    assert partial is False


# ---------------------------------------------------------------------------
# Deterministic, restart-stable snapshot identifiers
# ---------------------------------------------------------------------------

def _snap():
    return build_snapshot(
        table_name="sales",
        num_splits=4,
        bucket="test-bucket",
        warehouse_prefix="warehouse/db",
    )


def test_snapshot_ids_are_deterministic_across_rebuilds():
    a = _snap()
    b = _snap()
    assert a.snapshot_id == b.snapshot_id
    assert a.watermark_ms == b.watermark_ms
    assert a.manifest_list_key == b.manifest_list_key
    assert a.manifest_file_key == b.manifest_file_key
    assert [s.object_key for s in a.splits] == [s.object_key for s in b.splits]


def test_snapshot_id_is_positive_long():
    snap = _snap()
    assert 0 < snap.snapshot_id < 2**63


# ---------------------------------------------------------------------------
# Iceberg v2 metadata must carry all reader-required fields
# ---------------------------------------------------------------------------

def test_metadata_has_required_v2_fields():
    snap = _snap()
    meta = json.loads(build_metadata_json(snap))

    required = [
        "format-version",
        "table-uuid",
        "location",
        "last-sequence-number",
        "last-updated-ms",
        "last-column-id",
        "current-schema-id",
        "schemas",
        "partition-specs",
        "default-spec-id",
        "last-partition-id",
        "snapshots",
        "current-snapshot-id",
        "refs",
    ]
    for field in required:
        assert field in meta, f"metadata.json missing required field: {field}"

    assert meta["format-version"] == 2
    # last-sequence-number must match the (single) snapshot's sequence-number.
    assert meta["last-sequence-number"] == snap.sequence_number
    snapshot = meta["snapshots"][0]
    assert snapshot["sequence-number"] == snap.sequence_number
    assert snapshot["summary"]["operation"] == "append"


# ---------------------------------------------------------------------------
# H7 — config validation + secret redaction
# ---------------------------------------------------------------------------

def test_validate_config_passes_with_defaults():
    config.validate_config()  # should not raise


def test_validate_config_rejects_bad_num_splits(monkeypatch):
    monkeypatch.setattr(config, "NUM_SPLITS", 0)
    with pytest.raises(ValueError, match="NUM_SPLITS"):
        config.validate_config()


def test_validate_config_accepts_best_effort_generation_consistency(monkeypatch):
    monkeypatch.setattr(config, "GENERATION_SOURCE_CONSISTENCY", "best_effort")
    config.validate_config()


def test_validate_config_rejects_unsupported_snapshot_consistency(monkeypatch):
    monkeypatch.setattr(config, "GENERATION_SOURCE_CONSISTENCY", "snapshot")
    with pytest.raises(ValueError, match="do not share a source snapshot token"):
        config.validate_config()


def test_validate_config_rejects_duplicate_field_ids(monkeypatch):
    dup = list(config.TABLE_SCHEMA) + [config.TABLE_SCHEMA[0]]
    monkeypatch.setattr(config, "TABLE_SCHEMA", dup)
    with pytest.raises(ValueError, match="field_ids must be unique"):
        config.validate_config()


def _transformed_table(transform):
    return config.TableDef(
        name="customers_safe",
        source_table="dbo.customers",
        key_column="customer_id",
        schema=[
            config.ColumnDef(1, "customer_id", "long", nullable=False),
            config.ColumnDef(
                2,
                "email_token",
                "string",
                source="email",
                transform=transform,
            ),
        ],
    )


def test_validate_config_rejects_missing_token_key(monkeypatch):
    table = _transformed_table(config.ColumnTransform(
        kind="deterministic_hash", key_ref="missing-key-v1"
    ))
    monkeypatch.setattr(config, "DB_URL", "mssql+aioodbc://h/db")
    monkeypatch.setattr(config, "TABLES", [table])
    monkeypatch.delenv("FSP_TOKENIZATION_KEY_MISSING_KEY_V1", raising=False)
    with pytest.raises(ValueError, match="FSP_TOKENIZATION_KEY_MISSING_KEY_V1"):
        config.validate_config()


def test_validate_config_rejects_transform_on_unsupported_dialect(monkeypatch):
    table = _transformed_table(config.ColumnTransform(
        kind="deterministic_hash", key_ref="customer-pii-v1"
    ))
    monkeypatch.setattr(config, "DB_URL", "sqlite+aiosqlite:///x.db")
    monkeypatch.setattr(config, "TABLES", [table])
    monkeypatch.setenv("FSP_TOKENIZATION_KEY_CUSTOMER_PII_V1", "test-secret")
    with pytest.raises(ValueError, match="deterministic_hash is not supported"):
        config.validate_config()


@pytest.mark.parametrize("db_url", [
    "postgresql+asyncpg://h/db",
    "oracle+oracledb://h/db",
    "databricks://token:pat@dbc.cloud",
])
def test_validate_config_accepts_supported_token_dialects(monkeypatch, db_url):
    table = _transformed_table(config.ColumnTransform(
        kind="deterministic_hash", key_ref="customer-pii-v1"
    ))
    monkeypatch.setattr(config, "DB_URL", db_url)
    monkeypatch.setattr(config, "TABLES", [table])
    monkeypatch.setenv("FSP_TOKENIZATION_KEY_CUSTOMER_PII_V1", "test-secret")
    config.validate_config()


def test_validate_config_rejects_random_token_content_refresh(monkeypatch):
    table = _transformed_table(config.ColumnTransform(kind="random_token"))
    monkeypatch.setattr(config, "DB_URL", "mssql+aioodbc://h/db")
    monkeypatch.setattr(config, "TABLES", [table])
    monkeypatch.setattr(config, "AUTO_REFRESH", True)
    monkeypatch.setattr(config, "REFRESH_STRATEGY", "content_hash")
    with pytest.raises(ValueError, match="incompatible with content-based"):
        config.validate_config()


def test_redact_db_url_masks_password():
    assert (
        config.redact_db_url("mssql+aioodbc://user:s3cr3t@host/db")
        == "mssql+aioodbc://user:***@host/db"
    )
    assert (
        config.redact_db_url("postgresql+asyncpg://admin:p%40ss@10.0.0.1/sales")
        == "postgresql+asyncpg://admin:***@10.0.0.1/sales"
    )


def test_redact_db_url_leaves_sqlite_untouched():
    url = "sqlite+aiosqlite:///./poc_source.db"
    assert config.redact_db_url(url) == url

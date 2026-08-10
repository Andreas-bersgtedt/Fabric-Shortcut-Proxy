"""
Object-store transforming-mount tests (issue #12).

Covers mount config parsing (format/key_column/columns), the tokenizing cache
store's policy hashing + key-rotation invalidation, and an end-to-end Delta
materialization (guarded by the ``objectstore`` extra).
"""
from __future__ import annotations

import importlib.util
import os

import pyarrow as pa
import pytest

from config import ColumnDef, ColumnTransform
from storage import tokenizing_store
from storage.mounts import Mount, _mount_from_json
from storage.objectstore_reader import ObjectStoreReaderUnavailable

_KEY_ENV = "FSP_TOKENIZATION_KEY_CUSTOMER_PII_V1"
_HAS_DELTALAKE = importlib.util.find_spec("deltalake") is not None


def _delta_mount(root: str) -> Mount:
    return Mount(
        bucket="customers-safe", backend="local", root=root,
        format="delta", key_column="customer_id",
        columns=(
            ColumnDef(field_id=1, name="customer_id", iceberg_type="long", nullable=False),
            ColumnDef(
                field_id=2, name="email_token", source="email", iceberg_type="string",
                transform=ColumnTransform(
                    kind="deterministic_hash", key_ref="customer-pii-v1",
                    domain="customer-email", normalization="trim_lower",
                ),
            ),
        ),
    )


# --- mount config parsing ----------------------------------------------------

def test_mount_from_json_parses_format_and_columns():
    mount = _mount_from_json({
        "bucket": "customers-safe", "backend": "local", "root": "/data/customers",
        "format": "Delta", "key_column": "customer_id",
        "columns": [
            {"field_id": 1, "name": "customer_id", "type": "long", "nullable": False},
            {"field_id": 2, "name": "email_token", "source": "email", "type": "string",
             "transform": {"kind": "deterministic_hash", "key_ref": "customer-pii-v1",
                           "domain": "customer-email", "normalization": "trim_lower"}},
        ],
    })
    assert mount.format == "delta"                      # normalized lowercase
    assert mount.key_column == "customer_id"
    assert len(mount.columns) == 2
    assert mount.columns[1].name == "email_token"
    assert mount.columns[1].source_name == "email"
    assert mount.columns[1].transform.kind == "deterministic_hash"


def test_mount_from_json_rejects_transformed_non_string_column():
    with pytest.raises(ValueError):
        _mount_from_json({
            "bucket": "b", "backend": "local", "root": "/x", "format": "delta",
            "columns": [
                {"field_id": 1, "name": "x", "source": "y", "type": "long",
                 "transform": {"kind": "deterministic_hash", "key_ref": "k"}},
            ],
        })


def test_plain_mount_has_no_format():
    mount = _mount_from_json({"bucket": "b", "backend": "local", "root": "/x"})
    assert mount.format == "" and mount.columns == ()


# --- tokenizing store policy hashing ----------------------------------------

def test_policy_hash_is_stable_and_scoped(monkeypatch):
    monkeypatch.setenv(_KEY_ENV, "uat-secret")
    mount = _delta_mount("/src")
    assert tokenizing_store._policy_hash(mount) == tokenizing_store._policy_hash(mount)
    cache_dir = tokenizing_store.cache_dir_for(mount)
    assert mount.bucket in cache_dir.replace("\\", "/")


def test_policy_hash_changes_on_key_rotation(monkeypatch):
    mount = _delta_mount("/src")
    monkeypatch.setenv(_KEY_ENV, "key-one")
    first = tokenizing_store._policy_hash(mount)
    monkeypatch.setenv(_KEY_ENV, "key-two")
    second = tokenizing_store._policy_hash(mount)
    assert first != second               # key fingerprint invalidates the cache


def test_policy_hash_changes_on_policy_change(monkeypatch):
    monkeypatch.setenv(_KEY_ENV, "uat-secret")
    base = tokenizing_store._policy_hash(_delta_mount("/src"))
    changed = Mount(
        bucket="customers-safe", backend="local", root="/src",
        format="delta", key_column="customer_id",
        columns=(
            ColumnDef(field_id=1, name="customer_id", iceberg_type="long", nullable=False),
            ColumnDef(field_id=2, name="email_token", source="email", iceberg_type="string",
                      transform=ColumnTransform(kind="deterministic_hash", key_ref="customer-pii-v1",
                                                domain="other-domain", normalization="trim_lower")),
        ),
    )
    assert tokenizing_store._policy_hash(changed) != base


@pytest.mark.skipif(_HAS_DELTALAKE, reason="exercises the missing-extra error path")
def test_materialize_without_extra_fails_cleanly(tmp_path, monkeypatch):
    monkeypatch.setenv(_KEY_ENV, "uat-secret")
    monkeypatch.setenv("FSP_TOKENIZING_CACHE_DIR", str(tmp_path / "cache"))
    mount = _delta_mount(str(tmp_path / "src"))
    with pytest.raises(ObjectStoreReaderUnavailable):
        tokenizing_store.ensure_materialized(mount)


# --- end-to-end materialization (needs the objectstore extra) ----------------

@pytest.mark.skipif(not _HAS_DELTALAKE, reason="needs the objectstore extra (deltalake)")
def test_tokenizing_store_materializes_tokenized_delta(tmp_path, monkeypatch):
    import deltalake

    src = tmp_path / "src"
    deltalake.write_deltalake(str(src), pa.table({
        "customer_id": pa.array([1, 2], type=pa.int64()),
        "email": pa.array(["Alice@Example.com", None]),
    }))

    monkeypatch.setenv(_KEY_ENV, "uat-secret")
    monkeypatch.setenv("FSP_TOKENIZING_CACHE_DIR", str(tmp_path / "cache"))
    mount = _delta_mount(str(src))

    cache_dir = tokenizing_store.ensure_materialized(mount)

    assert os.path.isdir(os.path.join(cache_dir, "_delta_log"))   # a valid Delta table
    served = deltalake.DeltaTable(cache_dir).to_pyarrow_table().sort_by("customer_id")
    assert served.column_names == ["customer_id", "email_token"]  # ssn-style omission holds
    tokens = served.column("email_token").to_pylist()
    assert len(tokens[0]) == 64 and tokens[1] is None
    assert "Alice@Example.com" not in tokens                      # no plaintext served

    # Second call is a cache hit (marker present) — no re-materialization needed.
    assert tokenizing_store.ensure_materialized(mount) == cache_dir

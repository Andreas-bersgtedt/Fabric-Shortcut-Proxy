"""
Config Builder object-store tokenizer tests (issue #12, Phase 3).

Covers mount-payload validation/persistence of the tokenizer policy, the
object-store capability block, the schema-inspect endpoint, and the extensibility
of the tokenize pipeline (any reader that yields Arrow batches is tokenizable).
"""
from __future__ import annotations

import importlib.util

import pyarrow as pa
import pytest

from configbuilder.router import (
    _object_store_capabilities,
    _validate_mounts_payload,
)

_HAS_DELTALAKE = importlib.util.find_spec("deltalake") is not None

_DELTA_COLUMNS = [
    {"field_id": 1, "name": "customer_id", "type": "long", "nullable": False},
    {"field_id": 2, "name": "email_token", "source": "email", "type": "string",
     "transform": {"kind": "deterministic_hash", "key_ref": "customer-pii-v1",
                   "domain": "customer-email", "normalization": "trim_lower"}},
]


def _delta_mount_payload(**overrides) -> dict:
    payload = {
        "bucket": "customers-safe", "backend": "local", "root": "/data/customers",
        "format": "delta", "key_column": "customer_id", "columns": _DELTA_COLUMNS,
    }
    payload.update(overrides)
    return payload


# --- mount payload validation / persistence ---------------------------------

def test_validate_persists_tokenizer_policy():
    clean, errors = _validate_mounts_payload([_delta_mount_payload()])
    assert errors == []
    entry = clean[0]
    assert entry["format"] == "delta"
    assert entry["key_column"] == "customer_id"
    names = [c["name"] for c in entry["columns"]]
    assert names == ["customer_id", "email_token"]
    token = next(c for c in entry["columns"] if c["name"] == "email_token")
    assert token["source"] == "email"
    assert token["transform"]["kind"] == "deterministic_hash"
    assert token["transform"]["key_ref"] == "customer-pii-v1"


def test_validate_rejects_unknown_format():
    _, errors = _validate_mounts_payload([_delta_mount_payload(format="parquet")])
    assert any("unsupported table format" in e for e in errors)


def test_validate_rejects_transform_on_key_column():
    bad = _delta_mount_payload(
        key_column="email",
        columns=[{"field_id": 1, "name": "email", "source": "email", "type": "string",
                  "transform": {"kind": "random_token"}}],
    )
    _, errors = _validate_mounts_payload([bad])
    assert any("email" in e for e in errors)


def test_validate_requires_columns_for_format_mount():
    _, errors = _validate_mounts_payload([_delta_mount_payload(columns=[])])
    assert any("needs a 'columns' policy" in e for e in errors)


def test_plain_mount_still_validates_without_format():
    clean, errors = _validate_mounts_payload(
        [{"bucket": "share", "backend": "local", "root": "/mnt/share"}])
    assert errors == [] and "format" not in clean[0]


# --- capability block --------------------------------------------------------

def test_object_store_capabilities_shape():
    caps = _object_store_capabilities()
    assert set(caps["formats"]) == {"delta", "iceberg"}
    assert caps["reader_backends"]["delta"] == ["local", "s3", "azure"]
    assert caps["reader_backends"]["iceberg"] == ["local"]
    assert set(caps["reader_available"]) == {"delta", "iceberg"}


# --- schema inspect endpoint -------------------------------------------------

@pytest.mark.skipif(not _HAS_DELTALAKE, reason="needs the objectstore extra (deltalake)")
async def test_inspect_mount_reflects_delta_columns(tmp_path):
    import deltalake
    import httpx
    from fastapi import FastAPI
    from configbuilder.router import router as cfg_router

    src = tmp_path / "src"
    deltalake.write_deltalake(str(src), pa.table({
        "customer_id": pa.array([1, 2], type=pa.int64()),
        "email": pa.array(["a@example.com", "b@example.com"]),
    }))

    app = FastAPI()
    app.include_router(cfg_router)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        r = await client.post("/_config/api/mounts/inspect",
                              json={"backend": "local", "root": str(src), "format": "delta"})
    body = r.json()
    assert body["ok"] is True
    cols = {c["name"]: c["type"] for c in body["columns"]}
    assert cols == {"customer_id": "long", "email": "string"}


async def test_inspect_mount_requires_format():
    import httpx
    from fastapi import FastAPI
    from configbuilder.router import router as cfg_router

    app = FastAPI()
    app.include_router(cfg_router)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        r = await client.post("/_config/api/mounts/inspect",
                              json={"backend": "local", "root": "/x"})
    assert r.json()["ok"] is False


# --- extensibility: any Arrow-batch reader is tokenizable --------------------

def test_custom_reader_is_tokenizable(monkeypatch):
    """A new table-format reader only needs to yield Arrow batches; the tokenize
    pipeline is reader-agnostic (criterion 5)."""
    from config import ColumnDef, ColumnTransform
    from storage.tokenizer import tokenize_batch

    monkeypatch.setenv("FSP_TOKENIZATION_KEY_CUSTOMER_PII_V1", "uat-secret")

    class StubReader:  # imagine a future Hudi/Lance reader
        def schema(self):
            return pa.schema([("customer_id", pa.int64()), ("email", pa.string())])

        def read_batches(self, *, batch_rows):
            yield pa.record_batch({
                "customer_id": pa.array([1], type=pa.int64()),
                "email": pa.array(["a@example.com"]),
            })

    columns = [
        ColumnDef(field_id=1, name="customer_id", iceberg_type="long", nullable=False),
        ColumnDef(field_id=2, name="email_token", source="email", iceberg_type="string",
                  transform=ColumnTransform(kind="deterministic_hash", key_ref="customer-pii-v1",
                                            domain="customer-email", normalization="trim_lower")),
    ]
    rows = []
    for batch in StubReader().read_batches(batch_rows=1024):
        rows.extend(tokenize_batch(batch, columns).to_pylist())
    assert rows[0]["customer_id"] == 1
    assert len(rows[0]["email_token"]) == 64
    assert "a@example.com" not in rows[0].values()

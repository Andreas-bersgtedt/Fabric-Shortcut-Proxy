"""
Object-store tokenizer tests (issue #12).

Covers the proxy-side tokenizer module and the format capability/validation matrix:
deterministic token construction (parity with the pushdown dialects), null/empty
semantics, normalization, random tokens, fail-closed behavior, batch projection,
and the import-boundary rule that keeps the tokenizer decoupled from the relational
engine.
"""
from __future__ import annotations

import hashlib
import pathlib
import re

import pyarrow as pa
import pytest

from config import ColumnDef, ColumnTransform
from storage.tokenizer import (
    TokenizerError,
    get_tokenizer,
    register_tokenizer,
    supported_kinds,
    tokenize_batch,
)
from storage.objectstore_capabilities import (
    SUPPORTED_FORMATS,
    get_format_capabilities,
    validate_object_store_policy,
)

_KEY_ENV = "FSP_TOKENIZATION_KEY_CUSTOMER_PII_V1"


def _det_col() -> ColumnDef:
    return ColumnDef(
        field_id=2, name="email_token", source="email", iceberg_type="string",
        transform=ColumnTransform(
            kind="deterministic_hash", key_ref="customer-pii-v1",
            domain="customer-email", normalization="trim_lower",
        ),
    )


def _rand_col() -> ColumnDef:
    return ColumnDef(
        field_id=3, name="support_token", source="support_note", iceberg_type="string",
        transform=ColumnTransform(kind="random_token"),
    )


# --- deterministic tokenizer -------------------------------------------------

def test_deterministic_token_matches_pushdown_construction(monkeypatch):
    monkeypatch.setenv(_KEY_ENV, "uat-secret")
    col = _det_col()
    values = pa.array(["Alice@Example.com", " alice@example.com ", None])
    out = get_tokenizer("deterministic_hash").apply(
        values, transform=col.transform, column=col
    ).to_pylist()

    expected = hashlib.sha256(
        "uat-secret|customer-email|alice@example.com".encode("utf-8")
    ).hexdigest().upper()
    assert out[0] == expected            # documented key|domain|value SHA-256 hex-upper
    assert out[1] == expected            # trim_lower normalizes both inputs equally
    assert out[2] is None                # null in -> null out
    assert len(out[0]) == 64


def test_deterministic_empty_string_is_distinct_from_null(monkeypatch):
    monkeypatch.setenv(_KEY_ENV, "uat-secret")
    col = ColumnDef(
        field_id=2, name="email_token", source="email", iceberg_type="string",
        transform=ColumnTransform(
            kind="deterministic_hash", key_ref="customer-pii-v1", domain="d",
            normalization="none",
        ),
    )
    out = get_tokenizer("deterministic_hash").apply(
        pa.array(["", None]), transform=col.transform, column=col
    ).to_pylist()
    assert out[0] is not None and len(out[0]) == 64
    assert out[1] is None


def test_deterministic_domain_separation(monkeypatch):
    monkeypatch.setenv(_KEY_ENV, "uat-secret")
    base = dict(kind="deterministic_hash", key_ref="customer-pii-v1", normalization="none")
    col_a = ColumnDef(field_id=2, name="a", source="v", iceberg_type="string",
                      transform=ColumnTransform(domain="domain-a", **base))
    col_b = ColumnDef(field_id=3, name="b", source="v", iceberg_type="string",
                      transform=ColumnTransform(domain="domain-b", **base))
    values = pa.array(["same"])
    tok = get_tokenizer("deterministic_hash")
    a = tok.apply(values, transform=col_a.transform, column=col_a).to_pylist()[0]
    b = tok.apply(values, transform=col_b.transform, column=col_b).to_pylist()[0]
    assert a != b


def test_deterministic_key_rotation_changes_tokens(monkeypatch):
    col = _det_col()
    values = pa.array(["bob@example.com"])
    monkeypatch.setenv(_KEY_ENV, "key-one")
    first = get_tokenizer("deterministic_hash").apply(
        values, transform=col.transform, column=col).to_pylist()[0]
    monkeypatch.setenv(_KEY_ENV, "key-two")
    second = get_tokenizer("deterministic_hash").apply(
        values, transform=col.transform, column=col).to_pylist()[0]
    assert first != second


def test_deterministic_missing_key_fails(monkeypatch):
    monkeypatch.delenv(_KEY_ENV, raising=False)
    col = _det_col()
    with pytest.raises(ValueError):
        get_tokenizer("deterministic_hash").apply(
            pa.array(["x"]), transform=col.transform, column=col)


# --- random tokenizer --------------------------------------------------------

def test_random_token_shape_and_nulls():
    col = _rand_col()
    out = get_tokenizer("random_token").apply(
        pa.array(["x", "y", None]), transform=col.transform, column=col).to_pylist()
    assert out[2] is None
    assert len(out[0]) == 36 and out[0].count("-") == 4
    assert out[0] != out[1]


# --- registry / fail-closed --------------------------------------------------

def test_get_tokenizer_unknown_kind_fails_closed():
    with pytest.raises(TokenizerError):
        get_tokenizer("does_not_exist")


def test_registry_is_extensible():
    class UpperTokenizer:
        kind = "upper_stub"

        def apply(self, values, *, transform, column):
            return pa.array([None if v is None else str(v).upper() for v in values.to_pylist()],
                            type=pa.string())

    register_tokenizer(UpperTokenizer())
    assert "upper_stub" in supported_kinds()
    out = get_tokenizer("upper_stub").apply(pa.array(["ab", None]),
                                            transform=None, column=None).to_pylist()
    assert out == ["AB", None]


# --- batch projection --------------------------------------------------------

def test_tokenize_batch_projects_renames_and_drops(monkeypatch):
    monkeypatch.setenv(_KEY_ENV, "uat-secret")
    columns = [
        ColumnDef(field_id=1, name="customer_id", iceberg_type="long", nullable=False),
        _det_col(),  # email -> email_token
    ]
    batch = pa.record_batch({
        "customer_id": pa.array([1, 2], type=pa.int64()),
        "email": pa.array(["a@example.com", None]),
        "ssn": pa.array(["111-11-1111", "222-22-2222"]),  # omitted -> must be dropped
    })
    out = tokenize_batch(batch, columns)
    assert out.schema.names == ["customer_id", "email_token"]  # ssn dropped, email renamed
    rows = out.to_pylist()
    assert rows[0]["customer_id"] == 1
    assert len(rows[0]["email_token"]) == 64
    assert rows[1]["email_token"] is None


def test_tokenize_batch_missing_source_fails():
    columns = [ColumnDef(field_id=1, name="x", source="missing", iceberg_type="string")]
    batch = pa.record_batch({"present": pa.array([1])})
    with pytest.raises(TokenizerError):
        tokenize_batch(batch, columns)


# --- capability matrix / validation -----------------------------------------

def test_supported_formats():
    assert set(SUPPORTED_FORMATS) == {"delta", "iceberg"}
    assert get_format_capabilities("DELTA").format == "delta"
    assert get_format_capabilities("parquet") is None


def test_validate_rejects_unknown_format():
    with pytest.raises(ValueError):
        validate_object_store_policy(format="parquet", key_column="id", columns=[_det_col()])


def test_validate_rejects_transform_on_key_column():
    key_col = ColumnDef(
        field_id=1, name="customer_id", source="customer_id", iceberg_type="string",
        transform=ColumnTransform(kind="random_token"),
    )
    with pytest.raises(ValueError):
        validate_object_store_policy(format="delta", key_column="customer_id", columns=[key_col])


def test_validate_accepts_valid_policy():
    validate_object_store_policy(
        format="delta", key_column="customer_id", columns=[_det_col(), _rand_col()]
    )


# --- Delta reader round-trip (needs the objectstore extra) -------------------

def test_delta_reader_tokenizes_local_table(tmp_path, monkeypatch):
    deltalake = pytest.importorskip("deltalake")
    from storage.objectstore_reader import DeltaTableReader

    monkeypatch.setenv(_KEY_ENV, "uat-secret")
    table = pa.table({
        "customer_id": pa.array([1, 2], type=pa.int64()),
        "email": pa.array(["a@example.com", None]),
    })
    deltalake.write_deltalake(str(tmp_path), table)

    columns = [
        ColumnDef(field_id=1, name="customer_id", iceberg_type="long", nullable=False),
        _det_col(),
    ]
    reader = DeltaTableReader(str(tmp_path))
    rows: list[dict] = []
    for batch in reader.read_batches(batch_rows=1024):
        rows.extend(tokenize_batch(batch, columns).to_pylist())

    rows.sort(key=lambda r: r["customer_id"])
    assert [r["customer_id"] for r in rows] == [1, 2]
    assert len(rows[0]["email_token"]) == 64      # tokenized, not plaintext
    assert "a@example.com" not in {rows[0]["email_token"], rows[1]["email_token"]}


# --- containment: no engine imports -----------------------------------------

def test_tokenizer_module_does_not_import_engine():
    src = pathlib.Path(__file__).resolve().parents[1] / "storage" / "tokenizer.py"
    text = src.read_text(encoding="utf-8")
    for module in ("planner", "db", "iceberg", "delta", "runtime"):
        assert not re.search(rf"^\s*(from|import)\s+{module}(\.|\s|$)", text, re.M), (
            f"storage/tokenizer.py must not import {module} (engine containment rule)"
        )

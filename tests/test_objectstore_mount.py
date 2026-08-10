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
_HAS_PYICEBERG = importlib.util.find_spec("pyiceberg") is not None


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


def _iceberg_mount(root: str) -> Mount:
    return Mount(
        bucket="customers-iceberg", backend="local", root=root,
        format="iceberg", key_column="customer_id",
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


@pytest.mark.skipif(not (_HAS_DELTALAKE and _HAS_PYICEBERG),
                    reason="needs the objectstore extra (deltalake + pyiceberg)")
def test_tokenizing_store_materializes_iceberg_source(tmp_path, monkeypatch):
    import pathlib
    import deltalake
    from pyiceberg.catalog.sql import SqlCatalog

    impl = {"py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"}
    warehouse = tmp_path / "iceberg_wh"
    warehouse.mkdir()
    catalog = SqlCatalog("t", uri=f"sqlite:///{(tmp_path / 'cat.db').as_posix()}",
                         warehouse=pathlib.Path(warehouse).as_uri(), **impl)
    catalog.create_namespace("db")
    source = pa.table({
        "customer_id": pa.array([1, 2], type=pa.int64()),
        "email": pa.array(["Alice@Example.com", None]),
    })
    table = catalog.create_table("db.customers", schema=source.schema)
    table.append(source)
    location = table.location()
    root = location[len("file://"):] if location.startswith("file://") else location
    root = root.lstrip("/") if os.name == "nt" else root

    monkeypatch.setenv(_KEY_ENV, "uat-secret")
    monkeypatch.setenv("FSP_TOKENIZING_CACHE_DIR", str(tmp_path / "cache"))

    cache_dir = tokenizing_store.ensure_materialized(_iceberg_mount(root))

    served = deltalake.DeltaTable(cache_dir).to_pyarrow_table().sort_by("customer_id")
    assert served.column_names == ["customer_id", "email_token"]
    tokens = served.column("email_token").to_pylist()
    assert len(tokens[0]) == 64 and tokens[1] is None
    assert "Alice@Example.com" not in tokens              # Iceberg source read + tokenized


@pytest.mark.skipif(not (_HAS_DELTALAKE and _HAS_PYICEBERG),
                    reason="needs the objectstore extra (deltalake + pyiceberg)")
def test_tokenizing_store_iceberg_output(tmp_path, monkeypatch):
    import deltalake
    from storage.objectstore_reader import IcebergTableReader, _discover_iceberg_metadata

    src = tmp_path / "src"
    deltalake.write_deltalake(str(src), pa.table({
        "customer_id": pa.array([1, 2], type=pa.int64()),
        "email": pa.array(["Alice@Example.com", None]),
    }))

    monkeypatch.setenv(_KEY_ENV, "uat-secret")
    monkeypatch.setenv("FSP_TOKENIZING_CACHE_DIR", str(tmp_path / "cache"))
    mount = Mount(
        bucket="customers-iceberg-out", backend="local", root=str(src),
        format="delta", key_column="customer_id", output_format="iceberg",
        columns=(
            ColumnDef(field_id=1, name="customer_id", iceberg_type="long", nullable=False),
            ColumnDef(field_id=2, name="email_token", source="email", iceberg_type="string",
                      transform=ColumnTransform(kind="deterministic_hash", key_ref="customer-pii-v1",
                                                domain="customer-email", normalization="trim_lower")),
        ),
    )

    served = tokenizing_store.ensure_materialized(mount)
    assert os.path.isdir(os.path.join(served, "metadata"))          # a real Iceberg table

    reader = IcebergTableReader(_discover_iceberg_metadata(served))
    rows: list[dict] = []
    for batch in reader.read_batches(batch_rows=1024):
        rows.extend(batch.to_pylist())
    rows.sort(key=lambda r: r["customer_id"])
    assert [r["customer_id"] for r in rows] == [1, 2]
    assert set(rows[0].keys()) == {"customer_id", "email_token"}    # Delta source served as Iceberg
    assert len(rows[0]["email_token"]) == 64 and rows[1]["email_token"] is None
    assert "Alice@Example.com" not in {rows[0]["email_token"], rows[1]["email_token"]}

    # Cache hit returns the same served root (stable output, no re-materialization).
    assert tokenizing_store.ensure_materialized(mount) == served


def test_resolve_output_format():
    from storage.tokenizing_store import _resolve_output_format

    def _m(fmt, out):
        return Mount(bucket="b", backend="local", root="/x", format=fmt, output_format=out)

    assert _resolve_output_format(_m("delta", "")) == "delta"       # unset -> safe Delta default
    assert _resolve_output_format(_m("iceberg", "")) == "delta"     # unset stays Delta even for iceberg src
    assert _resolve_output_format(_m("iceberg", "auto")) == "iceberg"   # auto mirrors source
    assert _resolve_output_format(_m("delta", "auto")) == "delta"
    assert _resolve_output_format(_m("delta", "iceberg")) == "iceberg"  # explicit override


@pytest.mark.skipif(not _HAS_DELTALAKE, reason="needs the objectstore extra (deltalake)")
async def test_tokenizing_mount_serves_over_http(tmp_path, monkeypatch):
    import deltalake
    import httpx
    from fastapi import FastAPI

    import storage.mounts as mounts
    from s3.router import router as s3_router

    src = tmp_path / "src"
    deltalake.write_deltalake(str(src), pa.table({
        "customer_id": pa.array([1, 2], type=pa.int64()),
        "email": pa.array(["Alice@Example.com", "bob@example.com"]),
    }))

    monkeypatch.setenv(_KEY_ENV, "uat-secret")
    monkeypatch.setenv("FSP_TOKENIZING_CACHE_DIR", str(tmp_path / "cache"))
    monkeypatch.setenv("ENABLE_STORAGE_PROXY", "1")
    monkeypatch.setattr(mounts, "MOUNTS", {"customers-safe": _delta_mount(str(src))})
    monkeypatch.setattr(mounts, "_backends", {})

    app = FastAPI()
    app.include_router(s3_router)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        listing = await client.get("/customers-safe?list-type=2")
        assert listing.status_code == 200
        assert "_delta_log/" in listing.text                # served as a real Delta table

        import re as _re
        parquet_keys = _re.findall(r"<Key>([^<]+\.parquet)</Key>", listing.text)
        assert parquet_keys, listing.text

        got = await client.get(f"/customers-safe/{parquet_keys[0]}")
        assert got.status_code == 200
        assert b"Alice@Example.com" not in got.content       # no plaintext over the wire
        assert b"bob@example.com" not in got.content

        head = await client.head(f"/customers-safe/{parquet_keys[0]}")
        assert head.status_code == 200
        assert head.headers["Content-Length"] == str(len(got.content))


async def test_readyz_surfaces_tokenizing_mounts(monkeypatch):
    import httpx
    from fastapi import FastAPI

    import storage.mounts as mounts
    from observability.endpoints import router as obs_router

    monkeypatch.setenv("ENABLE_STORAGE_PROXY", "1")
    monkeypatch.setattr(mounts, "MOUNTS", {"customers-safe": _delta_mount("/src")})

    app = FastAPI()
    app.include_router(obs_router)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        body = (await client.get("/readyz")).json()

    tokenizer = body["object_store_tokenizer"]
    assert tokenizer["mounts"][0]["bucket"] == "customers-safe"
    assert tokenizer["mounts"][0]["format"] == "delta"
    assert tokenizer["mounts"][0]["transforms"] == 1
    assert set(tokenizer["formats"]) == {"delta", "iceberg"}


# --- s3 delta-rs storage_options mapping (no network) ------------------------

def test_s3_storage_options_anonymous_custom_endpoint():
    from storage.objectstore_reader import _s3_storage_options, _s3_table_uri

    mount = Mount(bucket="b", backend="s3", root="lake", prefix="curated/",
                  format="delta", auth="anonymous",
                  endpoint="http://minio:9000", region="us-west-1")
    options = _s3_storage_options(mount)
    assert options["AWS_REGION"] == "us-west-1"
    assert options["AWS_ENDPOINT_URL"] == "http://minio:9000"
    assert options["AWS_ALLOW_HTTP"] == "true"
    assert options["AWS_SKIP_SIGNATURE"] == "true"
    assert options["AWS_VIRTUAL_HOSTED_STYLE_REQUEST"] == "false"   # custom endpoint => path-style
    assert _s3_table_uri(mount, "") == "s3://lake/curated"


def test_s3_storage_options_instance_default_region_and_uri():
    from storage.objectstore_reader import _s3_storage_options, _s3_table_uri

    mount = Mount(bucket="b", backend="s3", root="lake", format="delta", auth="instance")
    options = _s3_storage_options(mount)
    assert options["AWS_REGION"] == "us-east-1"                     # default when unset
    assert "AWS_ACCESS_KEY_ID" not in options and "AWS_SKIP_SIGNATURE" not in options
    assert _s3_table_uri(mount, "customers") == "s3://lake/customers"


def test_s3_storage_options_rejects_unsupported_mode():
    from storage.objectstore_reader import _s3_storage_options, ObjectStoreReaderUnavailable

    mount = Mount(bucket="b", backend="s3", root="lake", format="delta", auth="sso")
    with pytest.raises(ObjectStoreReaderUnavailable):
        _s3_storage_options(mount)


# --- azure (ADLS Gen2) delta-rs storage_options mapping (no network) ---------

class _FakeSecretStore:
    def __init__(self, blob):
        self._blob = blob

    def get_secret(self, cid):
        return self._blob


def test_azure_storage_options_account_key():
    from storage.objectstore_reader import _azure_storage_options, _azure_table_uri

    mount = Mount(bucket="b", backend="azure", root="lakefs", account="acct",
                  prefix="curated/", format="delta", credential="azv")
    options = _azure_storage_options(
        mount, store=_FakeSecretStore({"mode": "account_key", "account_key": "KEY=="}))
    assert options["AZURE_STORAGE_ACCOUNT_NAME"] == "acct"
    assert options["AZURE_STORAGE_ACCOUNT_KEY"] == "KEY=="
    assert _azure_table_uri(mount, "") == "az://lakefs/curated"


def test_azure_storage_options_connection_string_parses_account():
    from storage.objectstore_reader import _azure_storage_options

    cs = "DefaultEndpointsProtocol=https;AccountName=acct;AccountKey=abc==;EndpointSuffix=core.windows.net"
    mount = Mount(bucket="b", backend="azure", root="container", format="delta", credential="azv")
    options = _azure_storage_options(
        mount, store=_FakeSecretStore({"mode": "connection_string", "connection_string": cs}))
    assert options["AZURE_STORAGE_ACCOUNT_NAME"] == "acct"
    assert options["AZURE_STORAGE_ACCOUNT_KEY"] == "abc=="


def test_azure_storage_options_service_principal():
    from storage.objectstore_reader import _azure_storage_options

    mount = Mount(bucket="b", backend="azure", root="container", account="acct",
                  format="delta", credential="azv")
    options = _azure_storage_options(mount, store=_FakeSecretStore(
        {"mode": "aad_client_secret", "tenant_id": "t", "client_id": "c", "client_secret": "s"}))
    assert options["AZURE_STORAGE_CLIENT_ID"] == "c"
    assert options["AZURE_STORAGE_TENANT_ID"] == "t"
    assert options["AZURE_STORAGE_CLIENT_SECRET"] == "s"


def test_azure_storage_options_rejects_managed_identity():
    from storage.objectstore_reader import _azure_storage_options, ObjectStoreReaderUnavailable

    mount = Mount(bucket="b", backend="azure", root="container", account="acct",
                  format="delta", auth="managed_identity")
    with pytest.raises(ObjectStoreReaderUnavailable):
        _azure_storage_options(mount)

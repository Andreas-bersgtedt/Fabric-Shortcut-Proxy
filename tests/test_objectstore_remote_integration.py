"""Opt-in end-to-end Iceberg reads against loopback MinIO and Azurite services."""

from __future__ import annotations

import io
import os
import types
import uuid
from dataclasses import replace
from urllib.parse import urlsplit

import pyarrow as pa
import pytest

pytest.importorskip("pyiceberg")
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.io import FileIO, InputFile, OutputFile

from storage.mounts import Mount
from storage.objectstore_reader import reader_for_mount


class _StoredFileIO(FileIO):
    def __init__(self, properties):
        super().__init__(properties)
        self._objects = _OBJECTS

    def new_input(self, location):
        return _StoredInput(location)

    def new_output(self, location):
        return _StoredOutput(location)

    def delete(self, location):
        name = location.location if hasattr(location, "location") else location
        del self._objects[name]


class _StoredInput(InputFile):
    def __len__(self):
        return len(_OBJECTS[self.location])

    def exists(self):
        return self.location in _OBJECTS

    def open(self, seekable=True):
        if not seekable:
            raise ValueError("integration fixture requires seekable input")
        return io.BytesIO(_OBJECTS[self.location])


class _StoredOutput(OutputFile):
    def __len__(self):
        return len(_OBJECTS[self.location])

    def exists(self):
        return self.location in _OBJECTS

    def to_input_file(self):
        return _StoredInput(self.location)

    def create(self, overwrite=False):
        if self.exists() and not overwrite:
            raise FileExistsError(self.location)

        class _CommitStream(io.BytesIO):
            def close(stream):
                if not stream.closed:
                    _OBJECTS[self.location] = stream.getvalue()
                super().close()

        return _CommitStream()


_OBJECTS: dict[str, bytes] = {}
_FILEIO_MODULE = types.ModuleType("_issue93_remote_integration_fileio")
_FILEIO_MODULE.StoredFileIO = _StoredFileIO
_FILEIO_IMPL = f"{_FILEIO_MODULE.__name__}.StoredFileIO"


def _loopback_endpoint(value: str) -> str:
    parsed = urlsplit(value)
    if parsed.hostname not in {"localhost", "127.0.0.1", "::1"}:
        pytest.fail("remote Iceberg integration endpoints must be loopback-only")
    return value


@pytest.mark.parametrize("backend", ["s3", "azure"])
def test_remote_iceberg_reads_current_and_pinned_snapshots(
    tmp_path, monkeypatch, backend
):
    monkeypatch.setitem(
        __import__("sys").modules, _FILEIO_MODULE.__name__, _FILEIO_MODULE
    )
    _OBJECTS.clear()
    suffix = uuid.uuid4().hex[:12]
    schema = pa.schema(
        [
            pa.field("customer_id", pa.int64(), nullable=False),
            pa.field("email", pa.string()),
        ]
    )
    parts = (
        pa.table(
            {
                "customer_id": pa.array([1], type=pa.int64()),
                "email": ["first@example.com"],
            },
            schema=schema,
        ),
        pa.table(
            {
                "customer_id": pa.array([2], type=pa.int64()),
                "email": ["second@example.com"],
            },
            schema=schema,
        ),
    )

    if backend == "s3":
        endpoint = os.environ.get("ISSUE93_MINIO_ENDPOINT", "")
        access_key = os.environ.get("ISSUE93_MINIO_ACCESS_KEY", "")
        secret_key = os.environ.get("ISSUE93_MINIO_SECRET_KEY", "")
        if not endpoint or not access_key or not secret_key:
            pytest.skip("set ISSUE93_MINIO_ENDPOINT/ACCESS_KEY/SECRET_KEY for MinIO")
        endpoint = _loopback_endpoint(endpoint)
        import boto3
        from botocore.config import Config

        root = f"issue93-{suffix}"
        service = boto3.client(
            "s3",
            endpoint_url=endpoint,
            region_name="us-east-1",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(s3={"addressing_style": "path"}),
        )
        credentials = {
            "mode": "static",
            "access_key": access_key,
            "secret_key": secret_key,
        }
        mount = Mount(
            bucket="issue93-integration",
            backend="s3",
            root=root,
            endpoint=endpoint,
            region="us-east-1",
            addressing_style="path",
            prefix="curated",
            credential="issue93-integration-s3",
            format="iceberg",
            key_column="customer_id",
        )
        scheme = "s3"
    else:
        account_url = os.environ.get("ISSUE93_AZURITE_ACCOUNT_URL", "")
        account = os.environ.get("ISSUE93_AZURITE_ACCOUNT", "")
        account_key = os.environ.get("ISSUE93_AZURITE_KEY", "")
        if not account_url or not account or not account_key:
            pytest.skip("set ISSUE93_AZURITE_ACCOUNT_URL/ACCOUNT/KEY for Azurite")
        account_url = _loopback_endpoint(account_url)
        from azure.storage.blob import BlobServiceClient

        root = f"issue93{suffix}"
        service = BlobServiceClient(account_url=account_url, credential=account_key)
        credentials = {"mode": "account_key", "account_key": account_key}
        mount = Mount(
            bucket="issue93-integration",
            backend="azure",
            root=root,
            account=account,
            endpoint=account_url,
            prefix="curated",
            credential="issue93-integration-azure",
            format="iceberg",
            key_column="customer_id",
        )
        scheme = "az"

    class CredentialStore:
        def get_secret(self, credential_id):
            expected = (
                "issue93-integration-s3"
                if backend == "s3"
                else "issue93-integration-azure"
            )
            return credentials if credential_id == expected else None

    catalog = SqlCatalog(
        f"issue93_{backend}_{suffix}",
        uri=f"sqlite:///{(tmp_path / f'{backend}.db').as_posix()}",
        warehouse=f"{scheme}://{root}/curated/warehouse",
        **{"py-io-impl": _FILEIO_IMPL},
    )
    catalog.create_namespace("db")
    table = catalog.create_table("db.customers", schema=schema)
    table.append(parts[0])
    pinned_snapshot_id = table.current_snapshot().snapshot_id
    table.append(parts[1])
    table_prefix = urlsplit(table.location()).path.lstrip("/")

    try:
        if backend == "s3":
            service.create_bucket(Bucket=root)
        else:
            service.create_container(root)
        for location, payload in list(_OBJECTS.items()):
            parsed = urlsplit(location)
            assert parsed.scheme == scheme and parsed.netloc == root
            key = parsed.path.lstrip("/")
            if backend == "s3":
                service.put_object(Bucket=root, Key=key, Body=payload)
            else:
                service.get_blob_client(root, key).upload_blob(payload, overwrite=True)

        mount = replace(mount, prefix=table_prefix)
        current = reader_for_mount(
            replace(mount, snapshot_id=None),
            store=CredentialStore(),
        )
        pinned = reader_for_mount(
            replace(mount, snapshot_id=pinned_snapshot_id),
            store=CredentialStore(),
        )

        def read_rows(reader):
            rows = [
                row
                for batch in reader.read_batches(batch_rows=16)
                for row in batch.to_pylist()
            ]
            return sorted(rows, key=lambda row: row["customer_id"])

        assert current.schema().field("customer_id").type == pa.int64()
        assert read_rows(current) == [
            {"customer_id": 1, "email": "first@example.com"},
            {"customer_id": 2, "email": "second@example.com"},
        ]
        assert [row["customer_id"] for row in read_rows(pinned)] == [1]
    finally:
        if backend == "s3":
            keys = service.list_objects_v2(Bucket=root).get("Contents") or []
            if keys:
                service.delete_objects(
                    Bucket=root,
                    Delete={"Objects": [{"Key": item["Key"]} for item in keys]},
                )
            service.delete_bucket(Bucket=root)
        else:
            service.delete_container(root)

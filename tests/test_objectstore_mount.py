"""
Object-store transforming-mount tests (issue #12).

Covers mount config parsing (format/key_column/columns), the tokenizing cache
store's policy hashing + key-rotation invalidation, and an end-to-end Delta
materialization (guarded by the ``objectstore`` extra).
"""
from __future__ import annotations

import importlib.util
import os
from datetime import UTC

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


def test_mount_from_json_parses_and_validates_snapshot_id():
    mount = _mount_from_json({
        "bucket": "b", "backend": "local", "root": "/x",
        "format": "iceberg", "snapshot_id": "123",
    })
    assert mount.snapshot_id == 123
    for value in (True, 0, -1, 1.5, "latest"):
        with pytest.raises(ValueError, match="snapshot_id"):
            _mount_from_json({
                "bucket": "b", "backend": "local", "root": "/x",
                "format": "iceberg", "snapshot_id": value,
            })


# --- tokenizing store policy hashing ----------------------------------------

def test_policy_hash_is_stable_and_scoped(monkeypatch):
    monkeypatch.setenv(_KEY_ENV, "uat-secret")
    mount = _delta_mount("/src")
    assert tokenizing_store._policy_hash(mount) == tokenizing_store._policy_hash(mount)
    cache_dir = tokenizing_store.cache_dir_for(mount)
    assert mount.bucket in cache_dir.replace("\\", "/")


def test_snapshot_pin_invalidates_materialized_cache(tmp_path, monkeypatch):
    from dataclasses import replace

    monkeypatch.setenv(_KEY_ENV, "uat-secret")
    monkeypatch.setenv("FSP_TOKENIZING_CACHE_DIR", str(tmp_path / "cache"))
    calls = []

    def materialize(mount, table_dir):
        os.makedirs(table_dir, exist_ok=True)
        calls.append(mount.snapshot_id)
        return table_dir

    monkeypatch.setattr(tokenizing_store, "_materialize", materialize)
    mount = replace(_iceberg_mount("/src"), snapshot_id=123)
    first = tokenizing_store.ensure_materialized(mount)
    assert tokenizing_store.ensure_materialized(mount) == first
    second = tokenizing_store.ensure_materialized(replace(mount, snapshot_id=456))
    current = tokenizing_store.ensure_materialized(replace(mount, snapshot_id=None))
    assert len({first, second, current}) == 3
    assert calls == [123, 456, None]


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


@pytest.mark.skipif(not _HAS_PYICEBERG, reason="needs pyiceberg")
def test_iceberg_reader_honors_pinned_snapshot(tmp_path):
    import pathlib

    from pyiceberg.catalog.sql import SqlCatalog

    from storage.objectstore_reader import reader_for_mount

    warehouse = tmp_path / "iceberg_wh"
    warehouse.mkdir()
    catalog = SqlCatalog(
        "snapshot-test", uri=f"sqlite:///{(tmp_path / 'catalog.db').as_posix()}",
        warehouse=pathlib.Path(warehouse).as_uri(),
        **{"py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"},
    )
    catalog.create_namespace("db")
    source_schema = pa.schema([
        pa.field("customer_id", pa.int64(), nullable=False),
        pa.field("email", pa.string()),
    ])
    table = catalog.create_table("db.customers", schema=source_schema)
    table.append(pa.Table.from_arrays(
        [pa.array([1], type=pa.int64()), pa.array(["first@example.com"])],
        schema=source_schema,
    ))
    first_snapshot_id = table.current_snapshot().snapshot_id
    table.append(pa.Table.from_arrays(
        [pa.array([2], type=pa.int64()), pa.array(["second@example.com"])],
        schema=source_schema,
    ))
    location = table.location()
    root = location.removeprefix("file://")
    root = root.lstrip("/") if os.name == "nt" else root

    pinned = _iceberg_mount(root)
    from dataclasses import replace
    pinned = replace(pinned, snapshot_id=first_snapshot_id)
    current_reader = reader_for_mount(_iceberg_mount(root))
    pinned_reader = reader_for_mount(pinned)

    def rows(reader):
        result = [
            row for batch in reader.read_batches(batch_rows=32)
            for row in batch.to_pylist()
        ]
        return sorted(result, key=lambda row: row["customer_id"])

    assert [row["customer_id"] for row in rows(current_reader)] == [1, 2]
    assert [row["customer_id"] for row in rows(pinned_reader)] == [1]


@pytest.mark.skipif(not _HAS_PYICEBERG, reason="needs pyiceberg")
def test_local_s3_and_azure_iceberg_reads_are_equivalent(tmp_path, monkeypatch):
    import io
    import pathlib
    import sys
    import types
    from datetime import datetime
    from urllib.parse import urlsplit

    import pyarrow.parquet as pq
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.io import FileIO, InputFile, OutputFile
    from pyiceberg.manifest import (
        DataFile,
        DataFileContent,
        FileFormat,
    )
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.table import DataScan, FileScanTask
    from pyiceberg.transforms import IdentityTransform
    from pyiceberg.types import LongType, NestedField, StringType

    from storage.objectstore_reader import reader_for_mount

    class StoredInput(InputFile):
        def __init__(self, location):
            super().__init__(location)

        def __len__(self):
            return len(objects[self.location])

        def exists(self):
            return self.location in objects

        def open(self, seekable=True):
            if not seekable:
                raise ValueError("fixture FileIO only supports seekable reads")
            return io.BytesIO(objects[self.location])

    class StoredOutput(OutputFile):
        def __init__(self, location):
            super().__init__(location)

        def __len__(self):
            return len(objects[self.location])

        def exists(self):
            return self.location in objects

        def to_input_file(self):
            return StoredInput(self.location)

        def create(self, overwrite=False):
            if self.exists() and not overwrite:
                raise FileExistsError(self.location)

            class CommitStream(io.BytesIO):
                def close(stream):
                    if not stream.closed:
                        objects[self.location] = stream.getvalue()
                    super().close()

            return CommitStream()

    class StoredFileIO(FileIO):
        def new_input(self, location):
            return StoredInput(location)

        def new_output(self, location):
            return StoredOutput(location)

        def delete(self, location):
            name = location.location if hasattr(location, "location") else location
            del objects[name]

    class MemoryRemoteClient:
        def __init__(self, scheme, container):
            self.scheme = scheme
            self.container = container

        def _location(self, key):
            return f"{self.scheme}://{self.container}/{key}"

        def get_object(self, *, Bucket, Key, Range=None):
            location = self._location(Key)
            if location not in objects:
                error = type("NotFound", (Exception,), {"status_code": 404})
                raise error("missing object")
            data = objects[location]
            if Range:
                start, end = map(int, Range.removeprefix("bytes=").split("-"))
                data = data[start:end + 1]
            return {"Body": io.BytesIO(data)}

        def head_object(self, *, Bucket, Key):
            location = self._location(Key)
            if location not in objects:
                error = type("NotFound", (Exception,), {"status_code": 404})
                raise error("missing object")
            return {"ContentLength": len(objects[location])}

        def list_objects_v2(self, *, Bucket, Prefix, **kwargs):
            keys = [
                urlsplit(uri).path.lstrip("/")
                for uri in objects
                if uri.startswith(f"{self.scheme}://{self.container}/")
                and urlsplit(uri).path.lstrip("/").startswith(Prefix)
            ]
            return {
                "Contents": [
                    {"Key": key, "LastModified": datetime.now(UTC)}
                    for key in sorted(keys)
                ],
                "IsTruncated": False,
            }

        def get_blob_client(self, key):
            parent = self

            class BlobClient:
                def get_blob_properties(self):
                    location = parent._location(key)
                    if location not in objects:
                        error = type("NotFound", (Exception,), {"status_code": 404})
                        raise error("missing blob")
                    return type("Properties", (), {"size": len(objects[location])})()

                def download_blob(self, *, offset=None, length=None):
                    location = parent._location(key)
                    if location not in objects:
                        error = type("NotFound", (Exception,), {"status_code": 404})
                        raise error("missing blob")
                    data = objects[location]
                    if offset is not None:
                        data = data[offset:offset + length if length is not None else None]
                    return type("Download", (), {
                        "readall": lambda _: data,
                    })()

            return BlobClient()

        def list_blobs(self, *, name_starts_with):
            for uri in sorted(objects):
                if not uri.startswith(f"{self.scheme}://{self.container}/"):
                    continue
                key = urlsplit(uri).path.lstrip("/")
                if key.startswith(name_starts_with):
                    yield type("BlobItem", (), {
                        "name": key,
                        "last_modified": datetime.now(UTC),
                    })()

    objects = {}
    module = types.ModuleType("_issue93_test_fileio")
    module.StoredFileIO = StoredFileIO
    monkeypatch.setitem(sys.modules, module.__name__, module)
    monkeypatch.setattr(
        "storage.objectstore_reader._remote_client",
        lambda mount, store=None: MemoryRemoteClient(
            "s3" if mount.backend == "s3" else "az", mount.root,
        ),
    )
    fileio_impl = "_issue93_test_fileio.StoredFileIO"
    schema = Schema(
        NestedField(field_id=1, name="customer_id", field_type=LongType(), required=True),
        NestedField(field_id=2, name="email", field_type=StringType()),
        NestedField(field_id=3, name="region", field_type=StringType()),
    )
    arrow_schema = pa.schema([
        pa.field("customer_id", pa.int64(), nullable=False),
        pa.field("email", pa.string()),
        pa.field("region", pa.string()),
    ])
    evolved_schema = pa.schema([
        *arrow_schema,
        pa.field("account_status", pa.string()),
    ])
    partition_spec = PartitionSpec(
        PartitionField(
            source_id=3, field_id=1000, transform=IdentityTransform(),
            name="region",
        ),
    )
    parts = [
        pa.Table.from_arrays(
            [
                pa.array([1], type=pa.int64()),
                pa.array(["first@example.com"]),
                pa.array(["west"]),
            ],
            schema=arrow_schema,
        ),
        pa.Table.from_arrays(
            [
                pa.array([2], type=pa.int64()),
                pa.array(["second@example.com"]),
                pa.array(["west"]),
                pa.array(["active"]),
            ],
            schema=evolved_schema,
        )
    ]

    local_warehouse = tmp_path / "equivalent-local"
    local_warehouse.mkdir()
    local_catalog = SqlCatalog(
        "equiv-local", uri=f"sqlite:///{(tmp_path / 'equiv-local.db').as_posix()}",
        warehouse=pathlib.Path(local_warehouse).as_uri(),
        **{"py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"},
    )
    local_catalog.create_namespace("db")
    local_table = local_catalog.create_table(
        "db.customers", schema=schema, partition_spec=partition_spec,
    )
    local_table.append(parts[0])
    local_table.update_schema().add_column("account_status", StringType()).commit()
    local_table.append(parts[1])
    local_location = urlsplit(local_table.location()).path
    local_root = local_location.lstrip("/") if os.name == "nt" else local_location
    local_reader = reader_for_mount(_iceberg_mount(local_root))

    remote_readers = []
    remote_tables = []
    remote_table_keys = []
    pinned_mounts = []
    for backend, scheme in (("s3", "s3"), ("azure", "az")):
        remote_catalog = SqlCatalog(
            f"equiv-{backend}",
            uri=f"sqlite:///{(tmp_path / f'equiv-{backend}.db').as_posix()}",
            warehouse=f"{scheme}://source/curated/warehouse",
            **{"py-io-impl": fileio_impl},
        )
        remote_catalog.create_namespace("db")
        remote_table = remote_catalog.create_table(
            "db.customers", schema=schema, partition_spec=partition_spec,
        )
        remote_table.append(parts[0])
        first_snapshot_id = remote_table.current_snapshot().snapshot_id
        remote_table.update_schema().add_column("account_status", StringType()).commit()
        remote_table.append(parts[1])
        remote_key = urlsplit(remote_table.location()).path.lstrip("/")
        remote_tables.append(remote_table)
        remote_table_keys.append(remote_key)
        mount = Mount(
            bucket=f"proxy-{backend}", backend=backend, root="source",
            prefix=remote_key, format="iceberg", key_column="customer_id",
            columns=_iceberg_mount("").columns, account="storageacct",
        )
        remote_readers.append(reader_for_mount(mount))
        pinned_mount = Mount(
            bucket=f"proxy-{backend}-pinned", backend=backend, root="source",
            prefix=remote_key, format="iceberg", key_column="customer_id",
            columns=_iceberg_mount("").columns, account="storageacct",
            snapshot_id=first_snapshot_id,
        )
        pinned_mounts.append(pinned_mount)

    def read_rows(reader):
        result = [
            row for batch in reader.read_batches(batch_rows=16)
            for row in batch.to_pylist()
        ]
        return sorted(result, key=lambda row: row["customer_id"])

    local_rows = read_rows(local_reader)
    local_schema = local_reader.schema()
    for remote_reader in remote_readers:
        assert remote_reader.schema().equals(local_schema, check_metadata=True)
        assert read_rows(remote_reader) == local_rows
    for pinned_mount in pinned_mounts:
        assert [row["customer_id"] for row in read_rows(reader_for_mount(pinned_mount))] == [1]

    scan_targets = [("local", local_reader, local_table, local_root)]
    scan_targets.extend(
        (backend, reader, table, table_key)
        for backend, reader, table, table_key in zip(
            ("s3", "azure"), remote_readers, remote_tables, remote_table_keys,
        )
    )
    for backend, reader, table, table_key in scan_targets:
        tasks = list(table.scan().plan_files())
        target_task = None
        for task in tasks:
            with table.io.new_input(task.file.file_path).open() as source:
                if pq.read_table(source)["customer_id"][0].as_py() == 1:
                    target_task = task
                    break
        assert target_task is not None

        local_delete_path = pathlib.Path(local_root) / "position-deletes.parquet"
        delete_location = (
            local_delete_path.as_uri()
            if backend == "local"
            else f"{'s3' if backend == 's3' else 'az'}://source/{table_key}/position-deletes.parquet"
        )
        delete_bytes = io.BytesIO()
        pq.write_table(
            pa.table({
                "file_path": [target_task.file.file_path],
                "pos": pa.array([0], type=pa.int64()),
            }),
            delete_bytes,
        )
        if backend == "local":
            local_delete_path.write_bytes(delete_bytes.getvalue())
        else:
            objects[delete_location] = delete_bytes.getvalue()
        delete_file = DataFile.from_args(
            content=DataFileContent.POSITION_DELETES,
            file_path=delete_location,
            file_format=FileFormat.PARQUET,
            partition=target_task.file.partition,
            record_count=1,
            file_size_in_bytes=delete_bytes.tell(),
        )
        injected_tasks = [
            FileScanTask(
                task.file,
                {delete_file} if task.file.file_path == target_task.file.file_path
                else task.delete_files,
                residual=task.residual,
            )
            for task in tasks
        ]
        with monkeypatch.context() as patch:
            patch.setattr(
                DataScan, "plan_files",
                lambda _scan, injected_tasks=injected_tasks: injected_tasks,
            )
            assert [row["customer_id"] for row in read_rows(reader)] == [2]

    # Reclassify real manifest records without depending on planner internals.
    with monkeypatch.context() as patch:
        patch.setattr(
            DataFile, "content",
            property(lambda _file: DataFileContent.EQUALITY_DELETES),
        )
        with pytest.raises(ValueError, match="equality deletes"):
            list(remote_readers[0].read_batches(batch_rows=16))


def test_remote_iceberg_fileio_reads_confined_s3_and_azure_objects():
    import io

    from storage.iceberg_fileio import ScopedIcebergFileIO
    from storage.objectstore_reader import (
        _iceberg_fileio_properties,
        _remote_object_key,
        _remote_table_root,
    )

    class Body(io.BytesIO):
        pass

    class S3Client:
        def __init__(self):
            self.objects = {"curated/table/data.bin": b"iceberg-s3"}
            self.calls = []

        def head_object(self, *, Bucket, Key):
            self.calls.append(("head", Bucket, Key))
            return {"ContentLength": len(self.objects[Key])}

        def get_object(self, *, Bucket, Key, Range):
            self.calls.append(("get", Bucket, Key, Range))
            start, end = map(int, Range.removeprefix("bytes=").split("-"))
            return {"Body": Body(self.objects[Key][start:end + 1])}

    class Blob:
        def __init__(self, parent, key):
            self.parent = parent
            self.key = key

        def get_blob_properties(self):
            self.parent.calls.append(("head", self.key))
            return type("Properties", (), {"size": len(self.parent.objects[self.key])})()

        def download_blob(self, *, offset, length):
            self.parent.calls.append(("get", self.key, offset, length))
            return type("Download", (), {
                "readall": lambda _: self.parent.objects[self.key][offset:offset + length],
            })()

    class AzureClient:
        def __init__(self):
            self.objects = {"curated/table/data.bin": b"iceberg-azure"}
            self.calls = []

        def get_blob_client(self, key):
            return Blob(self, key)

    for backend, client, expected in (
        ("s3", S3Client(), b"iceberg-s3"),
        ("azure", AzureClient(), b"iceberg-azure"),
    ):
        mount = Mount(
            bucket="proxy-bucket", backend=backend, root="source",
            prefix="curated", account="storageacct",
        )
        table_root = _remote_table_root(mount, "table")
        props = _iceberg_fileio_properties(
            mount, table_root=table_root, client=client,
        )
        file_io = ScopedIcebergFileIO(props)
        path = f"{table_root}/data.bin"
        source = file_io.new_input(path)
        assert source.exists()
        assert source.open().read() == expected
        calls_before = len(client.calls)
        with pytest.raises(ValueError, match="outside the configured"):
            file_io.new_input(f"{'s3' if backend == 's3' else 'az'}://source/other/table/outside.bin")
        with pytest.raises(ValueError, match="traversal|confined"):
            file_io.new_input(f"{table_root}/../outside.bin")
        assert len(client.calls) == calls_before

    azure_mount = Mount(
        bucket="proxy-bucket", backend="azure", root="source",
        prefix="curated", account="storageacct",
    )
    assert _remote_object_key(
        azure_mount,
        "abfs://source@storageacct.dfs.core.windows.net/curated/table/data.parquet",
        table_root="az://source/curated/table",
    ) == "curated/table/data.parquet"
    with pytest.raises(ValueError, match="account"):
        _remote_object_key(
            azure_mount,
            "abfs://source@other.dfs.core.windows.net/curated/table/data.parquet",
            table_root="az://source/curated/table",
        )


@pytest.mark.parametrize(
    ("backend", "location"),
    [
        ("s3", "s3://other-bucket/curated/table/file.parquet"),
        ("s3", "https://source/curated/table/file.parquet"),
        ("s3", "s3://source/other/table/file.parquet"),
        ("azure", "az://other-container/curated/table/file.parquet"),
        ("azure", "az://source/other/table/file.parquet"),
        ("azure", "abfs://source@other.dfs.core.windows.net/curated/table/file.parquet"),
        ("azure", "abfs://source@storageacct.dfs.core.windows.net/curated/%252e%252e/file.parquet"),
    ],
)
def test_remote_iceberg_rejects_untrusted_metadata_locations(backend, location):
    from storage.objectstore_reader import _remote_object_key

    mount = Mount(
        bucket="proxy-bucket", backend=backend, root="source",
        prefix="curated", account="storageacct",
    )
    with pytest.raises(ValueError):
        _remote_object_key(
            mount, location,
            table_root=("s3://source/curated/table" if backend == "s3"
                        else "az://source/curated/table"),
        )


def test_remote_iceberg_fileio_propagates_denied_access():
    from storage.iceberg_fileio import ScopedIcebergFileIO
    from storage.objectstore_reader import (
        _iceberg_fileio_properties,
        _remote_table_root,
    )

    class Denied(Exception):
        status_code = 403

    class DeniedS3:
        def head_object(self, **kwargs):
            raise Denied("access denied")

    mount = Mount(
        bucket="proxy-bucket", backend="s3", root="source", prefix="curated",
    )
    table_root = _remote_table_root(mount, "table")
    file_io = ScopedIcebergFileIO(_iceberg_fileio_properties(
        mount, table_root=table_root, client=DeniedS3(),
    ))
    with pytest.raises(Denied, match="access denied"):
        file_io.new_input(f"{table_root}/metadata/v1.metadata.json").exists()


def test_local_iceberg_metadata_discovery_rejects_symlinked_version_hint(tmp_path):
    from storage.objectstore_reader import _discover_iceberg_metadata

    table_root = tmp_path / "table"
    metadata = table_root / "metadata"
    metadata.mkdir(parents=True)
    outside = tmp_path / "outside-hint"
    outside.write_text("1", encoding="utf-8")
    hint = metadata / "version-hint.text"
    try:
        hint.symlink_to(outside)
    except (OSError, NotImplementedError):
        pytest.skip("symlink creation is not permitted")
    metadata_file = metadata / "00001.metadata.json"
    metadata_file.write_text("{}", encoding="utf-8")

    assert _discover_iceberg_metadata(
        str(table_root), scope_root=str(table_root),
    ) == str(metadata_file)


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
    root = location.removeprefix("file://")
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

    from storage.objectstore_reader import (
        IcebergTableReader,
        _discover_iceberg_metadata,
    )

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
def test_sample_delta_fixture_tokenizes(tmp_path, monkeypatch):
    import pathlib

    import deltalake

    sample = pathlib.Path(__file__).resolve().parents[1] / "demo" / "sample_delta" / "customers"
    if not (sample / "_delta_log").is_dir():
        pytest.skip("run demo/seed_delta.py to generate the sample table")

    monkeypatch.setenv(_KEY_ENV, "uat-secret")
    monkeypatch.setenv("FSP_TOKENIZING_CACHE_DIR", str(tmp_path / "cache"))
    mount = Mount(
        bucket="customers-safe", backend="local", root=str(sample),
        format="delta", key_column="customer_id",
        columns=(
            ColumnDef(field_id=1, name="customer_id", iceberg_type="long", nullable=False),
            ColumnDef(field_id=2, name="full_name", iceberg_type="string"),
            ColumnDef(field_id=3, name="email_token", source="email", iceberg_type="string",
                      transform=ColumnTransform(kind="deterministic_hash", key_ref="customer-pii-v1",
                                                domain="customer-email", normalization="trim_lower")),
            ColumnDef(field_id=4, name="ssn_token", source="ssn", iceberg_type="string",
                      transform=ColumnTransform(kind="random_token")),
            ColumnDef(field_id=5, name="city", iceberg_type="string"),
        ),
    )

    served = tokenizing_store.ensure_materialized(mount)
    table = deltalake.DeltaTable(served).to_pyarrow_table().sort_by("customer_id")

    assert set(table.column_names) == {"customer_id", "full_name", "email_token", "ssn_token", "city"}
    emails = dict(zip(table.column("customer_id").to_pylist(),
                      table.column("email_token").to_pylist()))
    assert emails[1] == emails[2]          # trim_lower normalizes rows 1 & 2 to the same token
    assert emails[1] != emails[3]
    assert emails[5] is None               # null email -> null token

    served_strings = {v for name in table.column_names
                      for v in table.column(name).to_pylist() if isinstance(v, str)}
    assert "Alice@Example.com" not in served_strings   # source PII never served
    assert "111-11-1111" not in served_strings


@pytest.mark.skipif(not _HAS_DELTALAKE, reason="needs the objectstore extra (deltalake)")
async def test_tokenizing_mount_serves_over_http(tmp_path, monkeypatch):
    import deltalake
    import httpx
    from fastapi import FastAPI

    from s3.router import router as s3_router
    from storage import mounts

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

    from observability.endpoints import router as obs_router
    from storage import mounts

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
    assert tokenizer["reader_backends"]["iceberg"] == ["local", "s3", "azure"]
    assert set(tokenizer["reader_backend_available"]) == {"local", "s3", "azure"}


async def test_readyz_reports_pinned_iceberg_mount(monkeypatch):
    from dataclasses import replace

    import httpx
    from fastapi import FastAPI

    from observability.endpoints import router as obs_router
    from storage import mounts

    monkeypatch.setenv("ENABLE_STORAGE_PROXY", "1")
    iceberg = replace(_iceberg_mount("/src"), snapshot_id=456)
    monkeypatch.setattr(mounts, "MOUNTS", {iceberg.bucket: iceberg})
    app = FastAPI()
    app.include_router(obs_router)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        body = (await client.get("/readyz")).json()
    assert body["object_store_tokenizer"]["mounts"][0]["snapshot_id"] == 456


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
    from storage.objectstore_reader import (
        ObjectStoreReaderUnavailable,
        _s3_storage_options,
    )

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
    from storage.objectstore_reader import (
        ObjectStoreReaderUnavailable,
        _azure_storage_options,
    )

    mount = Mount(bucket="b", backend="azure", root="container", account="acct",
                  format="delta", auth="managed_identity")
    with pytest.raises(ObjectStoreReaderUnavailable):
        _azure_storage_options(mount)

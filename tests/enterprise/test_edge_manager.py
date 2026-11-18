from contextlib import closing
import io
import json
import sqlite3

from fastavro import reader
import pyarrow.parquet as pq
import pytest
from sqlalchemy.engine import URL

from enterprise.control import edge_manager as edge_manager_module
from enterprise.control.edge_journal import EdgeJournal
from enterprise.control.edge_manager import EdgeManager
from enterprise.control.edge_transfer import copy_edge_generation
from fabric_shortcut_proxy import config
from fabric_shortcut_proxy.config import ColumnDef, TableDef
from fabric_shortcut_proxy.db import executor
from fabric_shortcut_proxy.runtime.artifact_store import LocalDirStore


async def test_local_refresh_journals_complete_iceberg_generations(tmp_path, monkeypatch):
    source = tmp_path / "source.db"
    with closing(sqlite3.connect(source)) as db, db:
        db.execute("CREATE TABLE sales (id INTEGER PRIMARY KEY, name TEXT)")
        db.execute("INSERT INTO sales VALUES (1, 'first')")
    monkeypatch.setattr(config, "DB_URL", str(URL.create("sqlite+aiosqlite", database=str(source))))
    monkeypatch.setattr(config, "TABLE_FORMAT", "iceberg")
    monkeypatch.setattr(config, "BUCKET_NAME", "test-bucket")
    monkeypatch.setattr(config, "WAREHOUSE_PREFIX", f"edge-test-{tmp_path.name}")
    monkeypatch.setattr(config, "OBJECT_PATH_LAYOUT", "legacy")
    monkeypatch.setattr(config, "ARTIFACT_STORE_SERVING", False)
    monkeypatch.setattr(config, "PARQUET_DISK_CACHE", False)
    monkeypatch.setattr(config, "PIN_MATERIALIZED_SPLITS", False)
    monkeypatch.setattr(config, "ICEBERG_MANIFEST_STATS", False)
    monkeypatch.setattr(executor, "_engine", None)
    table = TableDef(
        name="sales",
        source_table="sales",
        schema=[
            ColumnDef(field_id=1, name="id", iceberg_type="long", nullable=False),
            ColumnDef(field_id=2, name="name", iceberg_type="string", nullable=True),
        ],
        num_splits=1,
        key_column="id",
        split_strategy="modulo",
    )
    store = LocalDirStore(str(tmp_path / "artifacts"))
    journal_path = tmp_path / "edge.db"
    manager = EdgeManager(table, EdgeJournal(journal_path, dataset_id="default::sales"), store)

    try:
        assert await manager.refresh_once() == 1
        first = EdgeJournal(journal_path, dataset_id="default::sales").next_pending()
        assert first is not None
        destination = LocalDirStore(str(tmp_path / "regional-staging"))
        prefix = first.manifest.splits[0].object_key.split("/.fsp/", 1)[0]
        assert copy_edge_generation(
            first, store, destination, dataset_id="default::sales", allowed_prefix=prefix,
        ) == 5
        assert manager.journal.next_pending() == first
        for item in first.metadata:
            assert destination.verify(item.key, size=item.size_bytes, content_hash=item.sha256)
        with closing(sqlite3.connect(source)) as db, db:
            db.execute("UPDATE sales SET name = 'second' WHERE id = 1")
        assert await manager.refresh_once() == 2
        manager.journal.acknowledge(
            first.sequence, first.manifest.generation_id, first.manifest_sha256
        )
        second = EdgeJournal(journal_path, dataset_id="default::sales").next_pending()
        assert second is not None
        assert second.sequence == 2
        assert first.manifest.generation_id != second.manifest.generation_id
        assert first.manifest.generation_fence == second.manifest.generation_fence == 0
        for entry, name in [(first, "first"), (second, "second")]:
            assert {item.key for item in entry.metadata} == set(entry.manifest.metadata_keys)
            for item in entry.metadata:
                assert store.verify(item.key, size=item.size_bytes, content_hash=item.sha256)
            split = entry.manifest.splits[0]
            assert store.verify(
                split.object_key, size=split.size_bytes, content_hash=split.content_hash
            )
            table_data = pq.read_table(io.BytesIO(store.get(split.object_key)))
            assert table_data.to_pylist() == [{"id": 1, "name": name}]
            metadata_key = next(
                key for key in entry.manifest.metadata_keys if key.endswith(".json")
            )
            metadata = json.loads(store.get(metadata_key))
            list_key = metadata["snapshots"][0]["manifest-list"].split("/", 3)[3]
            list_entry = next(iter(reader(io.BytesIO(store.get(list_key)))))
            assert isinstance(list_entry, dict)
            manifest_path = list_entry["manifest_path"]
            assert isinstance(manifest_path, str)
            manifest_key = manifest_path.split("/", 3)[3]
            data_entry = next(iter(reader(io.BytesIO(store.get(manifest_key)))))
            assert isinstance(data_entry, dict)
            data_file = data_entry["data_file"]
            assert isinstance(data_file, dict)
            file_path = data_file["file_path"]
            assert isinstance(file_path, str)
            assert file_path.endswith(split.object_key)

        def fail_write(*_args, **_kwargs):
            raise OSError("local store unavailable")

        monkeypatch.setattr(edge_manager_module, "_publish_snapshot_objects", fail_write)
        with pytest.raises(OSError, match="local store unavailable"):
            await manager.refresh_once()
        assert manager.journal.next_pending() == second
    finally:
        if executor._engine is not None:
            await executor._engine.dispose()


def test_edge_manager_rejects_remote_or_unsupported_store(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "TABLE_FORMAT", "iceberg")
    monkeypatch.setattr(config, "ARTIFACT_STORE_SERVING", True)
    table = TableDef(
        name="sales", source_table="sales", num_splits=1,
        schema=[ColumnDef(field_id=1, name="id", iceberg_type="long", nullable=False)],
        split_strategy="modulo",
    )
    journal = EdgeJournal(tmp_path / "edge.db", dataset_id="default::sales")
    with pytest.raises(ValueError, match="write-through"):
        EdgeManager(table, journal, LocalDirStore(str(tmp_path / "artifacts")))

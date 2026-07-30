"""Phase 1 path-layout tests: canonical pathing + legacy alias compatibility."""
from __future__ import annotations

import config
from config import ColumnDef, TableDef
import iceberg.state_store as ss
from s3 import router as s3_router


_SCHEMA = [
    ColumnDef(field_id=1, name="id", iceberg_type="long", nullable=False),
]


def test_active_table_path_legacy(monkeypatch):
    monkeypatch.setattr(config, "OBJECT_PATH_LAYOUT", "legacy", raising=False)
    t = TableDef(name="orders", source_table="sales.orders", num_splits=2)
    p = ss.active_table_path(t, "warehouse/db")
    assert p == "warehouse/db/orders"


def test_active_table_path_canonical(monkeypatch):
    monkeypatch.setattr(config, "OBJECT_PATH_LAYOUT", "canonical", raising=False)
    monkeypatch.setattr(config, "DB_URL", "postgresql+asyncpg://u:p@pg-host:5432/salesdb", raising=False)
    t = TableDef(name="orders", source_table="sales.orders", num_splits=2)
    p = ss.active_table_path(t, "warehouse/db")
    assert p == "warehouse/db/pg-host/salesdb/sales/orders"


def test_alias_to_active_key_when_canonical(monkeypatch):
    monkeypatch.setattr(config, "OBJECT_PATH_LAYOUT", "canonical", raising=False)
    monkeypatch.setattr(config, "ENABLE_LEGACY_PATH_ALIASES", True, raising=False)
    monkeypatch.setattr(config, "DB_URL", "postgresql+asyncpg://u:p@pg-host:5432/salesdb", raising=False)

    ss._snapshots.clear()
    ss._history.clear()

    t = TableDef(name="orders", source_table="sales.orders", num_splits=1, schema=_SCHEMA)
    snap = ss.build_table_snapshot(t, "bucket", "warehouse/db")

    old = f"warehouse/db/orders/metadata/v1.metadata.json"
    new = f"{snap.table_path}/metadata/v1.metadata.json"
    assert ss.alias_to_active_key(old) == new


def test_snapshot_objects_include_legacy_aliases(monkeypatch):
    monkeypatch.setattr(config, "OBJECT_PATH_LAYOUT", "canonical", raising=False)
    monkeypatch.setattr(config, "ENABLE_LEGACY_PATH_ALIASES", True, raising=False)
    monkeypatch.setattr(config, "DB_URL", "postgresql+asyncpg://u:p@pg-host:5432/salesdb", raising=False)

    ss._snapshots.clear()
    ss._history.clear()

    t = TableDef(name="orders", source_table="sales.orders", num_splits=1, schema=_SCHEMA)
    snap = ss.build_table_snapshot(t, "bucket", "warehouse/db")

    objs = s3_router._objects_for_snapshot(snap)
    legacy_split = ss.active_to_legacy_key(snap, snap.splits[0].object_key)
    assert snap.metadata_key in objs
    assert "warehouse/db/orders/metadata/v1.metadata.json" in objs
    assert snap.splits[0].object_key in objs
    assert legacy_split in objs

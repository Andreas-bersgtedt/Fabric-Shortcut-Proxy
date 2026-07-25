"""Phase 6: serving-image publisher (enables the stateless/C++ Agent)."""
from __future__ import annotations

import os

os.environ.setdefault("DB_URL", "sqlite+aiosqlite:///:memory:")
os.environ.setdefault("S3_BUCKET", "test-bucket")

from runtime import serving_image
from runtime.artifact_store import MemoryStore


def test_publish_serving_image_writes_data_and_metadata(monkeypatch):
    store = MemoryStore()
    # Metadata objects carry inline bytes; data splits carry data=None (peeked from cache).
    objs = {
        "warehouse/db/sales/metadata/v1.metadata.json": {"data": b"{meta}"},
        "warehouse/db/sales/metadata/snap.avro": {"data": b"AVRO"},
        "warehouse/db/sales/data/split-0-1.parquet": {"data": None},
        "warehouse/db/sales/data/split-1-1.parquet": {"data": None},
    }
    import s3.router as router
    monkeypatch.setattr(router, "_snapshot_objects", lambda: objs, raising=True)
    import cache.lru_cache as cache
    monkeypatch.setattr(cache, "peek_parquet",
                        lambda k: b"PARQ" if k.endswith(".parquet") else None, raising=True)

    result = serving_image.publish_serving_image(store)
    assert result["written"] == 4 and result["skipped"] == 0
    # Every object key is now servable straight from the store.
    assert store.get("warehouse/db/sales/metadata/v1.metadata.json") == b"{meta}"
    assert store.get("warehouse/db/sales/metadata/snap.avro") == b"AVRO"
    assert store.get("warehouse/db/sales/data/split-0-1.parquet") == b"PARQ"
    assert store.get("warehouse/db/sales/data/split-1-1.parquet") == b"PARQ"


def test_publish_skips_objects_with_no_bytes(monkeypatch):
    store = MemoryStore()
    import s3.router as router
    monkeypatch.setattr(router, "_snapshot_objects",
                        lambda: {"warehouse/db/sales/data/x.parquet": {"data": None}}, raising=True)
    import cache.lru_cache as cache
    monkeypatch.setattr(cache, "peek_parquet", lambda k: None, raising=True)

    result = serving_image.publish_serving_image(store)
    assert result["written"] == 0 and result["skipped"] == 1

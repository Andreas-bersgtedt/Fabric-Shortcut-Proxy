"""Tests for the runtime artifact store (Phase 0 seam)."""
from __future__ import annotations

import os

import pytest

from runtime.artifact_store import (
    ArtifactStore, LocalDirStore, MemoryStore, ObjectNotFound, ObjectStat,
    build_store, get_default_store, set_default_store, reset_default_store,
)

KEY = "warehouse/db/sales/data/split-0-abc123.parquet"
BODY = b"PAR1" + bytes(range(256)) + b"PAR1"


@pytest.fixture(params=["memory", "local"])
def store(request, tmp_path) -> ArtifactStore:
    if request.param == "memory":
        return MemoryStore()
    return LocalDirStore(str(tmp_path / "artifacts"))


def test_put_get_roundtrip(store):
    stat = store.put(KEY, BODY)
    assert stat == ObjectStat(KEY, len(BODY))
    assert store.get(KEY) == BODY


def test_ranged_get(store):
    store.put(KEY, BODY)
    assert store.get(KEY, offset=0, length=4) == b"PAR1"
    assert store.get(KEY, offset=len(BODY) - 4) == b"PAR1"        # to end
    assert store.get(KEY, offset=4, length=3) == BODY[4:7]


def test_head_and_exists(store):
    assert store.head(KEY) is None
    assert store.exists(KEY) is False
    store.put(KEY, BODY)
    stat = store.head(KEY)
    assert stat.key == KEY and stat.size == len(BODY)   # mtime_ms is optional/backend-dependent
    assert store.exists(KEY) is True


def test_get_missing_raises(store):
    with pytest.raises(ObjectNotFound):
        store.get("warehouse/db/sales/data/nope.parquet")


def test_overwrite_is_idempotent_atomic(store):
    store.put(KEY, b"old-bytes")
    store.put(KEY, BODY)
    assert store.get(KEY) == BODY
    assert store.head(KEY).size == len(BODY)


def test_list_prefix_sorted(store):
    store.put("warehouse/db/sales/data/split-1.parquet", b"a")
    store.put("warehouse/db/sales/data/split-0.parquet", b"bb")
    store.put("warehouse/db/other/data/split-0.parquet", b"ccc")
    keys = [s.key for s in store.list("warehouse/db/sales/")]
    assert keys == [
        "warehouse/db/sales/data/split-0.parquet",
        "warehouse/db/sales/data/split-1.parquet",
    ]
    # sizes are reported
    assert store.list("warehouse/db/sales/")[0].size == 2
    # empty prefix lists everything
    assert len(store.list()) == 3


def test_delete(store):
    store.put(KEY, BODY)
    assert store.delete(KEY) is True
    assert store.exists(KEY) is False
    assert store.delete(KEY) is False   # already gone


def test_key_traversal_rejected(store):
    # '..' segments must never be accepted (path-escape). A leading slash is
    # tolerated (normalized to a relative key), so it is NOT in this list.
    for bad in ("../escape", "warehouse/../../etc/passwd", "..\\win", "a/b/../../.."):
        with pytest.raises(ValueError):
            store.put(bad, b"x")


def test_key_normalization(store):
    # leading slash and backslashes normalize to the same object
    store.put("/warehouse/db/t/data/f.parquet", BODY)
    assert store.get("warehouse\\db\\t\\data\\f.parquet") == BODY


def test_negative_offset_rejected(store):
    store.put(KEY, BODY)
    with pytest.raises(ValueError):
        store.get(KEY, offset=-1)


# ---- LocalDirStore specifics ------------------------------------------------

def test_localdir_atomic_no_tmp_leftovers(tmp_path):
    root = tmp_path / "art"
    s = LocalDirStore(str(root))
    s.put(KEY, BODY)
    tmps = [p for p in root.rglob("*.tmp")]
    assert tmps == []
    # the object exists at the mapped path
    assert (root / "warehouse" / "db" / "sales" / "data" / "split-0-abc123.parquet").is_file()


def test_localdir_root_created_lazily(tmp_path):
    root = tmp_path / "not-yet"
    s = LocalDirStore(str(root))
    assert not os.path.exists(root)     # nothing created on construct
    assert s.list() == []               # listing a missing root is empty, no error
    s.put(KEY, BODY)
    assert os.path.isdir(root)


# ---- Factory + default singleton -------------------------------------------

def test_build_store_factory(tmp_path):
    assert isinstance(build_store("memory"), MemoryStore)
    assert isinstance(build_store("local", local_dir=str(tmp_path)), LocalDirStore)
    with pytest.raises(ValueError):
        build_store("s3")


def test_default_store_singleton_and_reset():
    reset_default_store()
    try:
        s1 = get_default_store()
        s2 = get_default_store()
        assert s1 is s2                 # cached
        mem = MemoryStore()
        set_default_store(mem)
        assert get_default_store() is mem
    finally:
        reset_default_store()

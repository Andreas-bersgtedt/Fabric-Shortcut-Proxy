"""
Size-weighted split assignment tests (devplan/shardweight.md).

Covers the pure assignment/weight helpers and the sidecar round-trip, plus the
config surface (validation + enum choices in the settings catalog).
"""
from __future__ import annotations

from dataclasses import dataclass

import config
from planner import shard_weight as sw
from runtime.artifact_store import MemoryStore


# ---------------------------------------------------------------------------
# pure helpers
# ---------------------------------------------------------------------------

def test_stable_key():
    assert sw.stable_key("sales", 0) == "sales#0"
    assert sw.stable_key("db.Orders", 3) == "db.Orders#3"


def test_default_weight():
    assert sw.default_weight([]) == 1.0
    assert sw.default_weight([10.0, 20.0, 30.0]) == 20.0
    assert sw.default_weight([0.0, -5.0]) == 1.0        # ignore non-positive


def test_assign_owners_equal_weights_balances_count():
    keys = [f"t#{i}" for i in range(8)]
    weights = {k: 1.0 for k in keys}
    a = sw.assign_owners(keys, 4, weights)
    counts = [sum(1 for v in a.values() if v == s) for s in range(4)]
    assert counts == [2, 2, 2, 2]


def test_assign_owners_skew_balances_bytes():
    # One giant split + many tiny ones: LPT keeps per-shard bytes close.
    weights = {"g#0": 1000.0}
    for i in range(1, 21):
        weights[f"g#{i}"] = 10.0
    keys = list(weights)
    a = sw.assign_owners(keys, 4, weights)
    loads = [0.0] * 4
    for k, s in a.items():
        loads[s] += weights[k]
    spread = max(loads) - min(loads)
    # Giant unavoidably makes one shard heavier, but the rest pack to compensate.
    assert max(loads) <= 1000.0 + 10.0          # giant shard gets nothing extra beyond ~1 tiny
    assert spread <= 1000.0                      # dominated by the single giant item


def test_assign_owners_deterministic():
    weights = {f"t#{i}": float((i * 37) % 11 + 1) for i in range(30)}
    keys = list(weights)
    a1 = sw.assign_owners(keys, 3, weights)
    a2 = sw.assign_owners(list(reversed(keys)), 3, weights)
    assert a1 == a2                              # order-independent, deterministic


def test_assign_owners_single_shard_owns_all():
    keys = [f"t#{i}" for i in range(5)]
    a = sw.assign_owners(keys, 1, {})
    assert set(a.values()) == {0}


# ---------------------------------------------------------------------------
# per-table assignment over snapshots
# ---------------------------------------------------------------------------

@dataclass
class _T:
    name: str


@dataclass
class _Split:
    split_index: int
    table: _T


@dataclass
class _Snap:
    table: _T
    splits: list


def _snap(name, n):
    t = _T(name)
    return _Snap(t, [_Split(i, t) for i in range(n)])


def test_build_assignment_is_per_table():
    snaps = [_snap("a", 4), _snap("b", 4)]
    weights = {}                                 # cold: uniform => balanced count per table
    a = sw.build_assignment(snaps, 2, weights)
    # Each table's 4 splits split 2/2 across the 2 shards.
    for name in ("a", "b"):
        owners = [a[f"{name}#{i}"] for i in range(4)]
        assert owners.count(0) == 2 and owners.count(1) == 2


def test_build_assignment_covers_all_splits_once():
    snaps = [_snap("a", 5), _snap("b", 3)]
    a = sw.build_assignment(snaps, 3, {})
    assert len(a) == 8
    assert all(0 <= v < 3 for v in a.values())


def test_shard_loads_summary():
    a = {"a#0": 0, "a#1": 1, "a#2": 0}
    weights = {"a#0": 100.0, "a#1": 50.0, "a#2": 25.0}
    rows = sw.shard_loads(a, 2, weights)
    assert rows[0] == {"shard": 0, "splits": 2, "bytes": 125}
    assert rows[1] == {"shard": 1, "splits": 1, "bytes": 50}


# ---------------------------------------------------------------------------
# sidecar round-trip
# ---------------------------------------------------------------------------

def test_weights_roundtrip_and_merge():
    store = MemoryStore()
    assert sw.load_weights(store) == {}          # cold start
    assert sw.save_weights(store, {"a#0": 100, "a#1": 200}) is True
    assert sw.load_weights(store) == {"a#0": 100.0, "a#1": 200.0}
    # Merge: new table added, existing preserved, updated value overwrites.
    sw.save_weights(store, {"a#0": 150, "b#0": 300})
    assert sw.load_weights(store) == {"a#0": 150.0, "a#1": 200.0, "b#0": 300.0}


def test_load_weights_ignores_garbage():
    store = MemoryStore()
    store.put(sw.SHARD_WEIGHTS_KEY, b"not json")
    assert sw.load_weights(store) == {}
    store.put(sw.SHARD_WEIGHTS_KEY, b'{"a#0": "x", "a#1": -3, "a#2": 5}')
    assert sw.load_weights(store) == {"a#2": 5.0}   # keep only positive numbers


def test_save_weights_noop_on_empty():
    store = MemoryStore()
    assert sw.save_weights(store, {}) is False
    assert sw.load_weights(store) == {}


# ---------------------------------------------------------------------------
# config surface
# ---------------------------------------------------------------------------

def test_validate_setting_updates_accepts_and_rejects_shard_strategy():
    clean, errors = config.validate_setting_updates({"shard_strategy": "weighted"})
    assert not errors and clean["shard_strategy"] == "weighted"

    clean, errors = config.validate_setting_updates({"shard_strategy": "bogus"})
    assert errors and not clean
    assert any("shard_strategy" in e for e in errors)


def test_settings_catalog_exposes_choices():
    cat = {s["key"]: s for s in config.settings_catalog()}
    assert "shard_strategy" in cat
    assert cat["shard_strategy"]["choices"] == ["modulo", "weighted"]
    assert cat["shard_strategy"]["category"] == "Cluster (scale)"

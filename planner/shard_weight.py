"""
Size-weighted split assignment (see devplan/shardweight.md).

Replaces pure round-robin (``split_index % N``) materialization ownership with a
deterministic, coordinator-free assignment that balances each Agent shard's share
of *work* (observed split bytes), not just its *count* of splits.

Design properties:
  * **Deterministic & coordinator-free.** ``assign_owners`` is a pure function of
    (keys, shard_count, weights); every Agent that reads the same weights sidecar
    computes the same assignment.
  * **Duplicate-tolerant.** Minor disagreement is harmless (the artifact store is
    idempotent/content-addressed and non-owners fall back to generating locally),
    so the assignment only needs to be good, not perfectly consensual.
  * **Graceful default.** With no history all weights are equal, so LPT degrades to
    balanced-by-count (as good or better than modulo).

Weights are the previous run's observed sizes, persisted in the shared artifact
store as ``_control/shard_weights.json`` mapping ``"{table}#{split_index}" -> bytes``.
"""
from __future__ import annotations

import json

from observability.logging import get_logger

log = get_logger(__name__)

# Root-level sidecar key: never matches a table/serving prefix, so it is neither
# served nor swept by retention GC.
SHARD_WEIGHTS_KEY = "_control/shard_weights.json"


def stable_key(table_name: str, split_index: int) -> str:
    """A run-stable identity for a split (stable while ``num_splits`` is fixed)."""
    return f"{table_name}#{int(split_index)}"


def default_weight(known: list[float]) -> float:
    """Weight for a split with no history: the mean of known weights, else ``1.0``."""
    vals = [float(v) for v in known if v is not None and float(v) > 0.0]
    if not vals:
        return 1.0
    return sum(vals) / len(vals)


def assign_owners(keys: list[str], shard_count: int, weights: dict[str, float]) -> dict[str, int]:
    """Greedy LPT assignment of ``keys`` to ``shard_count`` shards.

    Sorts keys by descending weight (deterministic tie-break on the key) and places
    each on the currently least-loaded shard (tie-break: lowest shard index).
    Returns ``key -> shard_index``.
    """
    n = max(1, int(shard_count))
    if n == 1:
        return {k: 0 for k in keys}
    loads = [0.0] * n
    assignment: dict[str, int] = {}
    for k in sorted(keys, key=lambda x: (-float(weights.get(x, 1.0)), x)):
        shard = min(range(n), key=lambda i: (loads[i], i))
        assignment[k] = shard
        loads[shard] += float(weights.get(k, 1.0))
    return assignment


def build_assignment(snapshots, shard_count: int, weights: dict[str, float]) -> dict[str, int]:
    """Per-table LPT assignment across all snapshots (tables build sequentially, so
    each table's splits are balanced independently across the fleet)."""
    assignment: dict[str, int] = {}
    for snap in snapshots:
        keys = [stable_key(snap.table.name, s.split_index) for s in snap.splits]
        if not keys:
            continue
        dflt = default_weight([weights[k] for k in keys if k in weights])
        w = {k: float(weights.get(k, dflt)) for k in keys}
        assignment.update(assign_owners(keys, shard_count, w))
    return assignment


def shard_loads(assignment: dict[str, int], shard_count: int,
                weights: dict[str, float]) -> list[dict]:
    """Per-shard summary (split count + estimated bytes) for logging/observability."""
    n = max(1, int(shard_count))
    out = [{"shard": i, "splits": 0, "bytes": 0.0} for i in range(n)]
    for k, shard in assignment.items():
        if 0 <= shard < n:
            out[shard]["splits"] += 1
            out[shard]["bytes"] += float(weights.get(k, 0.0))
    for row in out:
        row["bytes"] = int(row["bytes"])
    return out


# ---------------------------------------------------------------------------
# Sidecar persistence (shared artifact store)
# ---------------------------------------------------------------------------

def load_weights(store, key: str = SHARD_WEIGHTS_KEY) -> dict[str, float]:
    """Load the weights sidecar; returns an empty dict when absent/unreadable."""
    if store is None:
        return {}
    try:
        raw = store.get(key)
    except Exception:  # noqa: BLE001 - ObjectNotFound or backend error => cold start
        return {}
    try:
        data = json.loads(raw.decode("utf-8"))
        if isinstance(data, dict):
            return {str(k): float(v) for k, v in data.items()
                    if isinstance(v, (int, float)) and float(v) > 0.0}
    except (ValueError, TypeError) as exc:
        log.warning("shard_weights_unreadable", error=str(exc))
    return {}


def save_weights(store, sizes: dict[str, float], key: str = SHARD_WEIGHTS_KEY) -> bool:
    """Merge ``sizes`` into the sidecar and persist it. Best-effort; returns success."""
    if store is None or not sizes:
        return False
    merged = load_weights(store, key)
    for k, v in sizes.items():
        try:
            fv = float(v)
        except (TypeError, ValueError):
            continue
        if fv > 0.0:
            merged[str(k)] = fv
    try:
        store.put(key, json.dumps(merged, separators=(",", ":")).encode("utf-8"))
        return True
    except Exception as exc:  # noqa: BLE001 - never let weight persistence break startup
        log.warning("shard_weights_save_failed", error=str(exc))
        return False

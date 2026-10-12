"""Freshness derived from durable publication metadata, never Agent liveness."""
from __future__ import annotations

import time
from typing import Iterable

from enterprise.control.contract import SnapshotManifest
from enterprise.control.placement import PlacementConfig
from enterprise.control.work_queue import DurableWorkQueue
from fabric_shortcut_proxy.observability.metrics import DatasetFreshnessSample


def dataset_freshness(
    queue: DurableWorkQueue,
    placement: PlacementConfig,
    datasets: Iterable[tuple[str, str]] = (),
    *,
    now_ms: int | None = None,
) -> list[DatasetFreshnessSample]:
    if not any(
        policy.freshness_target_ms is not None
        for policy in (*placement.connections.values(), *placement.tables.values())
    ):
        return []
    now = time.time_ns() // 1_000_000 if now_ms is None else now_ms
    latest: dict[str, SnapshotManifest] = {}
    identities = set(datasets) | set(placement.tables)
    for manifest in queue.list_snapshot_manifests():
        connection, separator, table = manifest.dataset_id.partition("::")
        if not separator or not connection or not table:
            continue
        identities.add((connection, table))
        current = latest.get(manifest.dataset_id)
        if current is None or (manifest.published_at_ms, manifest.epoch) > (
            current.published_at_ms, current.epoch
        ):
            latest[manifest.dataset_id] = manifest
    samples: list[DatasetFreshnessSample] = []
    for connection, table in sorted(identities):
        policy = placement.policy_for(connection, table)
        if policy.freshness_target_ms is None:
            continue
        dataset_id = f"{connection}::{table}"
        manifest = latest.get(dataset_id)
        produced = manifest.produced_at_ms if manifest is not None else 0
        known = 0 < produced <= now
        age = (now - produced) / 1_000 if known else None
        profile = placement.stores.get(policy.required_storage_profile)
        pool = placement.pools.get(policy.required_pool)
        location = (
            profile.location if profile is not None else
            pool.location if pool is not None else policy.required_location or "unknown"
        )
        samples.append({
            "dataset_id": dataset_id,
            "location": location,
            "produced_at_ms": produced if known else None,
            "age_seconds": age,
            "target_seconds": policy.freshness_target_ms / 1_000,
            "stale": not known or now - produced > policy.freshness_target_ms,
        })
    return samples

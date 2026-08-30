"""Manager-less generation coordination and fenced activation records."""
from __future__ import annotations

import json
import secrets
import time
from dataclasses import dataclass

from runtime.artifact_store import ObjectNotFound

COORDINATOR_KEY = ".fsp/generation-coordinator.json"
BUILD_KEY = ".fsp/generation-build.json"
CURRENT_KEY = "CURRENT"


class GenerationError(RuntimeError):
    pass


@dataclass(frozen=True)
class GenerationContext:
    generation_id: str
    fence: int
    lease_token: str
    shard_count: int
    expires_at_ms: int
    source_consistency: str = "best_effort"


def _encode(value: dict) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _read_json(store, key: str) -> dict | None:
    try:
        raw = store.get(key)
    except ObjectNotFound:
        return None
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise GenerationError(f"invalid generation record {key}: {exc}") from exc
    if not isinstance(value, dict):
        raise GenerationError(f"invalid generation record {key}: expected object")
    return value


def _context(record: dict) -> GenerationContext:
    try:
        return GenerationContext(
            generation_id=str(record["generation_id"]),
            fence=int(record["fence"]),
            lease_token=str(record["lease_token"]),
            shard_count=int(record["shard_count"]),
            expires_at_ms=int(record["expires_at_ms"]),
            source_consistency=str(record.get("source_consistency", "best_effort")),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise GenerationError(f"invalid generation context: {exc}") from exc


def acquire_generation(
    store,
    shard_count: int,
    *,
    lease_seconds: int = 300,
    source_consistency: str = "best_effort",
) -> GenerationContext:
    """Fence any prior coordinator and create one immutable build generation."""
    previous = _read_json(store, COORDINATOR_KEY) or {}
    fence = int(previous.get("fence", 0)) + 1
    now_ms = int(time.time() * 1000)
    context = GenerationContext(
        generation_id=f"{fence:020d}-{secrets.token_hex(8)}",
        fence=fence,
        lease_token=secrets.token_hex(16),
        shard_count=shard_count,
        expires_at_ms=now_ms + lease_seconds * 1000,
        source_consistency=source_consistency,
    )
    record = {
        "version": 1,
        "generation_id": context.generation_id,
        "fence": context.fence,
        "lease_token": context.lease_token,
        "shard_count": context.shard_count,
        "expires_at_ms": context.expires_at_ms,
        "source_consistency": context.source_consistency,
    }
    store.put(COORDINATOR_KEY, _encode(record))
    confirmed = _read_json(store, COORDINATOR_KEY)
    if confirmed != record:
        raise GenerationError("coordinator lease was lost while being acquired")
    store.put(BUILD_KEY, _encode({**record, "state": "STAGING"}))
    return context


def join_generation(store, shard_count: int, *, timeout_seconds: float) -> GenerationContext:
    """Wait for shard 0's live generation, rejecting stale build records.

    A worker may start after a small generation has already become ACTIVE.  The
    coordinator record remains the authoritative lease in that case, while the
    build record is intentionally replaced by serving-image metadata.
    """
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        build = _read_json(store, BUILD_KEY)
        lease = _read_json(store, COORDINATOR_KEY)
        if build and lease and build.get("state") in {"STAGING", "ACTIVE"}:
            context = _context(lease)
            if (
                context.shard_count == shard_count
                and context.generation_id == build.get("generation_id")
                and context.lease_token == lease.get("lease_token")
                and context.fence == int(lease.get("fence", -1))
                and context.expires_at_ms > int(time.time() * 1000)
            ):
                return context
        time.sleep(0.25)
    raise TimeoutError("timed out waiting for shard 0 generation coordination")


def assert_generation_lease(store, context: GenerationContext) -> None:
    lease = _read_json(store, COORDINATOR_KEY)
    if not lease:
        raise GenerationError("coordinator lease is missing")
    if lease.get("lease_token") != context.lease_token or int(lease.get("fence", -1)) != context.fence:
        raise GenerationError("coordinator lease was fenced by a newer generation")
    if int(lease.get("expires_at_ms", 0)) <= int(time.time() * 1000):
        raise GenerationError("coordinator lease expired")


def assert_generation_identity(store, generation_id: str, fence: int, lease_token: str) -> None:
    """Fail a worker whose build was fenced by a replacement coordinator."""
    lease = _read_json(store, COORDINATOR_KEY)
    if not lease:
        raise GenerationError("coordinator lease is missing")
    if (
        lease.get("generation_id") != generation_id
        or int(lease.get("fence", -1)) != fence
        or lease.get("lease_token") != lease_token
    ):
        raise GenerationError("worker generation was fenced by a newer coordinator")
    if int(lease.get("expires_at_ms", 0)) <= int(time.time() * 1000):
        raise GenerationError("worker generation lease expired")


def renew_generation(store, context: GenerationContext, *, lease_seconds: int = 300) -> GenerationContext:
    assert_generation_lease(store, context)
    renewed = GenerationContext(
        generation_id=context.generation_id,
        fence=context.fence,
        lease_token=context.lease_token,
        shard_count=context.shard_count,
        expires_at_ms=int(time.time() * 1000) + lease_seconds * 1000,
        source_consistency=context.source_consistency,
    )
    record = {
        "version": 1,
        "generation_id": renewed.generation_id,
        "fence": renewed.fence,
        "lease_token": renewed.lease_token,
        "shard_count": renewed.shard_count,
        "expires_at_ms": renewed.expires_at_ms,
        "source_consistency": renewed.source_consistency,
    }
    store.put(COORDINATOR_KEY, _encode(record))
    store.put(BUILD_KEY, _encode({**record, "state": "STAGING"}))
    return renewed


def assign_generation(snapshots, context: GenerationContext) -> None:
    for snapshot in snapshots:
        for split in snapshot.splits:
            split.generation_id = context.generation_id
            split.generation_fence = context.fence
            split.generation_token = context.lease_token
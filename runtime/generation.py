"""Manager-less generation coordination and fenced activation records."""
from __future__ import annotations

import json
import hashlib
import secrets
import time
from dataclasses import dataclass, replace

from db.read_points import TableReadPlan
from runtime.artifact_store import ObjectNotFound

COORDINATOR_KEY = ".fsp/generation-coordinator.json"
BUILD_KEY = ".fsp/generation-build.json"
CURRENT_KEY = "CURRENT"
GENERATION_VERSION = 2
PLAN_PREPARING = "PREPARING"
PLAN_READY = "READY"
PLAN_FAILED = "FAILED"
PLAN_ACTIVE = "ACTIVE"


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
    version: int = GENERATION_VERSION
    plan_state: str = PLAN_READY
    table_plans: tuple[TableReadPlan, ...] = ()
    plan_sha256: str = ""


def _encode(value: dict) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _read_raw_json(store, key: str) -> tuple[bytes | None, dict | None]:
    try:
        raw = store.get(key)
    except ObjectNotFound:
        return None, None
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise GenerationError(f"invalid generation record {key}: {exc}") from exc
    if not isinstance(value, dict):
        raise GenerationError(f"invalid generation record {key}: expected object")
    return raw, value


def _read_json(store, key: str) -> dict | None:
    return _read_raw_json(store, key)[1]


def _plan_payload(plans: tuple[TableReadPlan, ...]) -> list[dict]:
    return [
        plan.to_state_dict()
        for plan in sorted(plans, key=lambda item: item.table_id)
    ]


def _plan_digest(plans: tuple[TableReadPlan, ...]) -> str:
    return hashlib.sha256(_encode({"table_plans": _plan_payload(plans)})).hexdigest()


def _record(context: GenerationContext) -> dict:
    record = {
        "version": context.version,
        "generation_id": context.generation_id,
        "fence": context.fence,
        "lease_token": context.lease_token,
        "shard_count": context.shard_count,
        "expires_at_ms": context.expires_at_ms,
        "source_consistency": context.source_consistency,
    }
    if context.version >= 2:
        record.update({
            "plan_state": context.plan_state,
            "plan_sha256": context.plan_sha256,
            "table_plans": _plan_payload(context.table_plans),
        })
    return record


def _context(record: dict) -> GenerationContext:
    try:
        version = int(record.get("version", 1))
        plans = (
            tuple(TableReadPlan.from_dict(item) for item in record.get("table_plans", []))
            if version >= 2 else ()
        )
        context = GenerationContext(
            generation_id=str(record["generation_id"]),
            fence=int(record["fence"]),
            lease_token=str(record["lease_token"]),
            shard_count=int(record["shard_count"]),
            expires_at_ms=int(record["expires_at_ms"]),
            source_consistency=str(record.get("source_consistency", "best_effort")),
            version=version,
            plan_state=(
                str(record.get("plan_state", PLAN_READY))
                if version >= 2 else PLAN_READY
            ),
            table_plans=plans,
            plan_sha256=(
                str(record.get("plan_sha256") or _plan_digest(plans))
                if version >= 2 else ""
            ),
        )
        if version >= 2 and context.plan_sha256 != _plan_digest(plans):
            raise GenerationError("generation table-plan digest mismatch")
        return context
    except (KeyError, TypeError, ValueError) as exc:
        raise GenerationError(f"invalid generation context: {exc}") from exc


def current_generation(store) -> GenerationContext | None:
    """Return the current generation coordinator record, if one exists."""
    return current_generation_record(store)[1]


def current_generation_record(
    store,
) -> tuple[bytes | None, GenerationContext | None]:
    """Return the exact coordinator bytes and their parsed context."""
    raw, record = _read_raw_json(store, COORDINATOR_KEY)
    return raw, _context(record) if record is not None else None


def acquire_generation(
    store,
    shard_count: int,
    *,
    lease_seconds: int = 300,
    source_consistency: str = "best_effort",
    prepare_plan: bool = False,
) -> GenerationContext:
    """Fence any prior coordinator and create one immutable build generation."""
    previous_raw, previous_record = _read_raw_json(store, COORDINATOR_KEY)
    previous = previous_record or {}
    fence = int(previous.get("fence", 0)) + 1
    now_ms = int(time.time() * 1000)
    context = GenerationContext(
        generation_id=f"{fence:020d}-{secrets.token_hex(8)}",
        fence=fence,
        lease_token=secrets.token_hex(16),
        shard_count=shard_count,
        expires_at_ms=now_ms + lease_seconds * 1000,
        source_consistency=source_consistency,
        plan_state=PLAN_PREPARING if prepare_plan else PLAN_READY,
    )
    context = replace(context, plan_sha256=_plan_digest(context.table_plans))
    record = _record(context)
    if not store.compare_and_swap(
        COORDINATOR_KEY,
        previous_raw,
        _encode(record),
    ):
        raise GenerationError("coordinator lease was lost while being acquired")
    confirmed = _read_json(store, COORDINATOR_KEY)
    if confirmed != record:
        raise GenerationError("coordinator lease was lost while being acquired")
    store.put(BUILD_KEY, _encode({**record, "state": context.plan_state}))
    return context


def publish_generation_plan(
    store,
    context: GenerationContext,
    table_plans: list[TableReadPlan] | tuple[TableReadPlan, ...],
) -> GenerationContext:
    """Publish one complete immutable table plan and let workers join it."""
    assert_generation_lease(store, context)
    if context.version < 2:
        raise GenerationError("generation plans require state version 2")
    if context.plan_state != PLAN_PREPARING:
        raise GenerationError(
            f"generation plan cannot be published from state {context.plan_state!r}"
        )
    plans = tuple(sorted(table_plans, key=lambda item: item.table_id))
    table_ids = [plan.table_id for plan in plans]
    if len(set(table_ids)) != len(table_ids):
        raise GenerationError("generation plan contains duplicate table IDs")
    for plan in plans:
        descriptor = plan.descriptor
        if (
            descriptor.generation_id != context.generation_id
            or descriptor.generation_fence != context.fence
        ):
            raise GenerationError(
                f"table plan {plan.table_id!r} belongs to a different generation"
            )
        if descriptor.owner_shard >= context.shard_count:
            raise GenerationError(
                f"table plan {plan.table_id!r} owner shard is outside the generation"
            )
    ready = replace(
        context,
        plan_state=PLAN_READY,
        table_plans=plans,
        plan_sha256=_plan_digest(plans),
    )
    record = _record(ready)
    current_raw, current_record = _read_raw_json(store, COORDINATOR_KEY)
    if current_record is None or _context(current_record) != context:
        raise GenerationError("generation changed while plan was prepared")
    if not store.compare_and_swap(
        COORDINATOR_KEY,
        current_raw,
        _encode(record),
    ):
        raise GenerationError("generation plan changed while being published")
    store.put(BUILD_KEY, _encode({**record, "state": PLAN_READY}))
    confirmed = _context(_read_json(store, COORDINATOR_KEY) or {})
    if confirmed != ready:
        raise GenerationError("generation plan changed while being published")
    return ready


def fail_generation_plan(store, context: GenerationContext, reason: str) -> None:
    """Mark planning failed without serializing provider or lease tokens."""
    assert_generation_lease(store, context)
    safe_reason = str(reason)
    sensitive = [context.lease_token]
    sensitive.extend(
        plan.descriptor.token
        for plan in context.table_plans
        if plan.descriptor.token
    )
    for value in sensitive:
        safe_reason = safe_reason.replace(value, "[REDACTED]")
    failure = {
        "version": max(2, context.version),
        "state": PLAN_FAILED,
        "plan_state": PLAN_FAILED,
        "generation_id": context.generation_id,
        "fence": context.fence,
        "shard_count": context.shard_count,
        "source_consistency": context.source_consistency,
        "plan_sha256": context.plan_sha256,
        "table_ids": [plan.table_id for plan in context.table_plans],
        "error": safe_reason[:500],
    }
    store.put(BUILD_KEY, _encode(failure))


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
        if build and lease and build.get("state") == PLAN_FAILED:
            if (
                build.get("generation_id") == lease.get("generation_id")
                and int(build.get("fence", -1)) == int(lease.get("fence", -2))
            ):
                raise GenerationError(
                    f"generation planning failed: {build.get('error') or 'unknown error'}"
                )
        if build and lease and build.get("state") in {
            "STAGING", PLAN_READY, "ACTIVE"
        }:
            try:
                context = _context(lease)
            except GenerationError:
                time.sleep(0.25)
                continue
            build_version = int(build.get("version", 1))
            plan_matches = (
                context.version == 1
                or (
                    build_version >= 2
                    and context.plan_state == PLAN_READY
                    and build.get("plan_sha256") == context.plan_sha256
                )
            )
            if (
                context.shard_count == shard_count
                and context.generation_id == build.get("generation_id")
                and context.lease_token == lease.get("lease_token")
                and context.fence == int(lease.get("fence", -1))
                and context.expires_at_ms > int(time.time() * 1000)
                and plan_matches
            ):
                return (
                    replace(context, plan_state=PLAN_ACTIVE)
                    if build.get("state") == PLAN_ACTIVE
                    else context
                )
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


def assert_generation_identity(
    store,
    generation_id: str,
    fence: int,
    lease_token: str,
    plan_sha256: str = "",
) -> None:
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
    if plan_sha256 and lease.get("plan_sha256") != plan_sha256:
        raise GenerationError("worker generation table plan does not match coordinator")


def renew_generation(store, context: GenerationContext, *, lease_seconds: int = 300) -> GenerationContext:
    assert_generation_lease(store, context)
    renewed = replace(
        context,
        expires_at_ms=int(time.time() * 1000) + lease_seconds * 1000,
    )
    record = _record(renewed)
    current_raw, current_record = _read_raw_json(store, COORDINATOR_KEY)
    if current_record is None or _context(current_record) != context:
        raise GenerationError("generation lease was lost before renewal")
    if not store.compare_and_swap(
        COORDINATOR_KEY,
        current_raw,
        _encode(record),
    ):
        raise GenerationError("generation lease was lost while renewing")
    build = _read_json(store, BUILD_KEY)
    if (
        build
        and build.get("generation_id") == renewed.generation_id
        and int(build.get("fence", -1)) == renewed.fence
    ):
        updated_build = dict(build)
        updated_build.update({
            "expires_at_ms": renewed.expires_at_ms,
            "lease_token": renewed.lease_token,
        })
        if renewed.version >= 2 and updated_build.get("state") != "ACTIVE":
            updated_build.update({
                "version": renewed.version,
                "plan_state": renewed.plan_state,
                "plan_sha256": renewed.plan_sha256,
                "table_plans": _plan_payload(renewed.table_plans),
            })
        store.put(BUILD_KEY, _encode(updated_build))
    else:
        store.put(BUILD_KEY, _encode({**record, "state": renewed.plan_state}))
    return renewed


def assign_generation(snapshots, context: GenerationContext) -> None:
    for snapshot in snapshots:
        for split in snapshot.splits:
            split.generation_id = context.generation_id
            split.generation_fence = context.fence
            split.generation_token = context.lease_token
            split.generation_plan_sha256 = context.plan_sha256
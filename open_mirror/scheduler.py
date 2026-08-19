"""Background Open Mirroring loop with Fabric replication preflight."""
from __future__ import annotations

import asyncio
import os
import random
import time
from dataclasses import dataclass

import config
from observability.logging import get_logger
from open_mirror.config import OpenMirrorTarget
from open_mirror.fabric_api import (
    FabricApiError,
    get_mirroring_status,
    start_mirroring,
    update_mirrored_database_retention,
)
from open_mirror.source import TargetResult, publish_target

log = get_logger(__name__)
_LAST_START_ATTEMPT: dict[str, float] = {}


@dataclass
class ReplicationPreflight:
    ready: bool
    status: str | None
    action: str
    error: str | None = None
    request_id: str | None = None


def _status(payload: dict) -> str | None:
    value = payload.get("status") or payload.get("mirroringStatus")
    if isinstance(value, dict):
        value = value.get("status")
    return str(value) if value is not None else None


async def _fabric_call(function, *args, deadline: float | None = None):
    """Retry only bounded transport/5xx/retriable/429 failures."""
    attempts = int(getattr(config, "OPEN_MIRROR_FABRIC_RETRY_ATTEMPTS", 3) or 3)
    for attempt in range(attempts):
        try:
            return await asyncio.to_thread(function, *args)
        except FabricApiError as exc:
            if not exc.retriable or attempt + 1 >= attempts:
                raise
            delay = exc.retry_after
            if delay is None:
                delay = min(8.0, 0.5 * (2 ** attempt)) + random.uniform(0, 0.25)
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise
                delay = min(delay, remaining)
            await asyncio.sleep(delay)
    raise AssertionError("unreachable")


def _self_healing_enabled(target: OpenMirrorTarget) -> bool:
    if target.self_healing is not None:
        return target.self_healing
    if not target.landing_zone_root.lower().startswith(
        "https://onelake.dfs.fabric.microsoft.com/"
    ):
        return False
    return bool(getattr(config, "OPEN_MIRROR_SELF_HEALING", True))


async def ensure_replication_running(target: OpenMirrorTarget) -> ReplicationPreflight:
    """Apply Fabric's documented status decision table for one target."""
    if (target.fabric_retention_days is not None or _self_healing_enabled(target)) and (
        not target.workspace_id or not target.mirrored_database_id
    ):
        return ReplicationPreflight(
            False, None, "configuration_error",
            "Fabric retention and self-healing require workspace_id and mirrored_database_id",
        )
    if target.fabric_retention_days is not None:
        try:
            await _fabric_call(
                update_mirrored_database_retention,
                target.workspace_id,
                target.mirrored_database_id,
                target.fabric_retention_days,
            )
        except FabricApiError as exc:
            return ReplicationPreflight(
                False, None, "retention_error", str(exc), exc.request_id
            )
    if not _self_healing_enabled(target):
        return ReplicationPreflight(True, None, "disabled")

    deadline_seconds = float(
        getattr(config, "OPEN_MIRROR_PREFLIGHT_TIMEOUT_SECONDS", 60) or 60
    )
    deadline = time.monotonic() + max(0.1, deadline_seconds)
    cooldown = float(
        getattr(config, "OPEN_MIRROR_START_COOLDOWN_SECONDS", 300) or 300
    )
    delay = 1.0
    started = False
    workspace = target.workspace_id
    database = target.mirrored_database_id

    try:
        while time.monotonic() < deadline:
            payload = await _fabric_call(
                get_mirroring_status, workspace, database, deadline=deadline
            )
            status = _status(payload)
            if status == "Running":
                return ReplicationPreflight(
                    True, status, "started" if started else "already_running"
                )
            if status in {"Initialized", "Paused", "Stopped"}:
                last_attempt = _LAST_START_ATTEMPT.get(target.id)
                now = time.monotonic()
                if last_attempt is not None and now - last_attempt < cooldown:
                    return ReplicationPreflight(
                        False, status, "cooldown",
                        "mirroring is not running and the target is in start cooldown",
                    )
                _LAST_START_ATTEMPT[target.id] = now
                await _fabric_call(
                    start_mirroring, workspace, database, deadline=deadline
                )
                started = True
            elif status in {"Initializing", "Starting"}:
                pass
            elif status == "Stopping":
                return ReplicationPreflight(
                    False, status, "deferred",
                    "mirroring is stopping; start is deferred to a later cycle",
                )
            else:
                return ReplicationPreflight(
                    False, status, "unknown_status",
                    f"unknown Fabric mirroring status {status!r}; no mutation attempted",
                )
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            await asyncio.sleep(
                min(remaining, delay + random.uniform(0, min(0.25, delay / 4)))
            )
            delay = min(8.0, delay * 2)
    except FabricApiError as exc:
        return ReplicationPreflight(
            False, None, "permission_error" if exc.status_code in {401, 403}
            else "api_error",
            str(exc), exc.request_id,
        )
    return ReplicationPreflight(
        False, "Starting" if started else None, "deadline",
        "Fabric mirroring did not reach Running before the preflight deadline",
    )


def _summarize(results: list[TargetResult]) -> dict:
    published = sum(
        1 for target in results for result in target.results
        if result.action in ("initial", "incremental", "recovery")
    )
    errors = sum(1 for target in results if target.error) + sum(
        1 for target in results for result in target.results
        if result.action == "error"
    )
    return {
        "targets": len(results),
        "published_tables": published,
        "pages_read": sum(r.pages_read for t in results for r in t.results),
        "rows_scanned": sum(r.rows_scanned for t in results for r in t.results),
        "rows_published": sum(r.rows_published for t in results for r in t.results),
        "dropped": sum(len(target.dropped) for target in results),
        "errors": errors,
        "replication_unavailable": sum(
            1 for target in results
            if target.replication_action not in {None, "disabled", "already_running", "started"}
        ),
    }


async def run_cycle(*, dry_run: bool = False) -> list[TargetResult]:
    from open_mirror.config import load_targets

    results = await publish_targets_with_preflight(load_targets(), dry_run=dry_run)
    log.info("open_mirror_cycle", dry_run=dry_run, **_summarize(results))
    return results


async def publish_targets_with_preflight(
    targets: list[OpenMirrorTarget], *, dry_run: bool = False, mode: str | None = None
) -> list[TargetResult]:
    """Preflight and publish targets once; shared by scheduler and publish-now."""
    results: list[TargetResult] = []
    for target in targets:
        preflight = await ensure_replication_running(target)
        if not preflight.ready:
            result = TargetResult(
                target.id, skipped=True, error=preflight.error,
                replication_status=preflight.status,
                replication_action="replication_unavailable",
            )
        else:
            result = await publish_target(target, dry_run=dry_run, mode=mode)
            result.replication_status = preflight.status
            result.replication_action = preflight.action
        results.append(result)
    return results


class OpenMirrorScheduler:
    def __init__(self, interval_seconds: int | None = None) -> None:
        self.interval = int(
            interval_seconds if interval_seconds is not None
            else getattr(config, "OPEN_MIRROR_INTERVAL_SECONDS", 300)
        )
        self._task: asyncio.Task | None = None

    async def _loop(self) -> None:
        log.info(
            "open_mirror_loop_started", interval_seconds=self.interval,
            mode=getattr(config, "OPEN_MIRROR_MODE", "incremental"),
            state_path=os.path.abspath(
                str(getattr(config, "OPEN_MIRROR_STATE_DIR", "./.open_mirror_state"))
            ),
        )
        try:
            while True:
                try:
                    await run_cycle()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("open_mirror_cycle_failed")
                await asyncio.sleep(max(5, self.interval))
        except asyncio.CancelledError:
            log.info("open_mirror_loop_stopped")
            raise

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._loop(), name="open-mirror-publish")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

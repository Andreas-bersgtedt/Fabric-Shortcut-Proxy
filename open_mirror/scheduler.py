"""Background Open Mirroring publish loop (runs on the Manager).

Periodically publishes every configured target into its Fabric landing zone. The
loop is gated by ``OPEN_MIRROR_PUBLISH`` and reads its interval/mode from config.
It fails soft: a bad target or table is quarantined by :mod:`open_mirror.source`
so one broken target never stops the loop.
"""
from __future__ import annotations

import asyncio

import config
from observability.logging import get_logger
from open_mirror.source import TargetResult, publish_all

log = get_logger(__name__)


def _summarize(results: list[TargetResult]) -> dict:
    published = sum(
        1 for tr in results for r in tr.results if r.action in ("initial", "incremental")
    )
    errors = sum(1 for tr in results if tr.error) + sum(
        1 for tr in results for r in tr.results if r.action == "error"
    )
    rows = sum(r.rows for tr in results for r in tr.results)
    dropped = sum(len(tr.dropped) for tr in results)
    return {"targets": len(results), "published_tables": published, "rows": rows,
            "dropped": dropped, "errors": errors}


async def run_cycle(*, dry_run: bool = False) -> list[TargetResult]:
    """Publish all configured targets once and log a summary."""
    results = await publish_all(dry_run=dry_run)
    log.info("open_mirror_cycle", dry_run=dry_run, **_summarize(results))
    return results


class OpenMirrorScheduler:
    """Owns the periodic publish task for the Manager lifespan."""

    def __init__(self, interval_seconds: int | None = None) -> None:
        self.interval = int(
            interval_seconds if interval_seconds is not None
            else getattr(config, "OPEN_MIRROR_INTERVAL_SECONDS", 300)
        )
        self._task: asyncio.Task | None = None

    async def _loop(self) -> None:
        log.info("open_mirror_loop_started", interval_seconds=self.interval,
                 mode=getattr(config, "OPEN_MIRROR_MODE", "incremental"))
        try:
            while True:
                try:
                    await run_cycle()
                except asyncio.CancelledError:
                    raise
                except Exception:  # noqa: BLE001 - never let one cycle kill the loop
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

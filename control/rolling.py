"""
Rolling upgrade/restart — recycle a fleet one Agent at a time (Phase 5).

Restart Agents sequentially, waiting for each to become **healthy** (re-registered
+ heartbeating) before touching the next, so at most one Agent is down at any moment
and >= N-1 keep serving throughout — an upgrade or config roll with no read gap
behind the gateway/LB. The health check is injected (the Manager passes
``registry.is_alive``) so this is deterministically testable without real processes.
"""
from __future__ import annotations

import asyncio
from typing import Callable

from observability.logging import get_logger

log = get_logger(__name__)


async def _await_healthy(sup, is_healthy: Callable[[str], bool],
                         timeout: float, poll: float) -> bool:
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if sup.is_alive and is_healthy(sup.name):
            return True
        await asyncio.sleep(poll)
    return bool(sup.is_alive and is_healthy(sup.name))


async def rolling_restart(
    supervisors,
    *,
    is_healthy: Callable[[str], bool],
    health_timeout: float = 30.0,
    poll: float = 0.25,
    settle: float = 0.0,
    before_stop: Callable[[str], None] | None = None,
    on_event: Callable[[str, str, bool], None] | None = None,
) -> list[tuple[str, bool]]:
    """Restart each supervisor in turn, gating on health between steps.

    Returns ``[(agent_name, became_healthy), ...]``. Only one Agent is ever
    stopped at a time, so the rest of the fleet keeps serving. ``before_stop`` is
    called with the Agent name just before it is stopped — the Manager passes
    ``registry.remove`` so the gateway drops it from rotation **immediately**
    (no ~heartbeat-miss window of routing to a dead Agent).
    """
    results: list[tuple[str, bool]] = []
    for sup in supervisors:
        log.info("rolling_restart_step", agent=sup.name)
        if on_event:
            on_event("restarting", sup.name, False)
        if before_stop is not None:
            try:
                before_stop(sup.name)
            except Exception as exc:  # noqa: BLE001
                log.warning("rolling_restart_before_stop_error", agent=sup.name, error=str(exc))
        await sup.stop()
        await sup.start()
        healthy = await _await_healthy(sup, is_healthy, health_timeout, poll)
        if settle:
            await asyncio.sleep(settle)
        results.append((sup.name, healthy))
        log.info("rolling_restart_step_done", agent=sup.name, healthy=healthy, pid=sup.pid)
        if on_event:
            on_event("restarted", sup.name, healthy)
    log.info("rolling_restart_complete",
             agents=len(results), healthy=sum(1 for _, h in results if h))
    return results

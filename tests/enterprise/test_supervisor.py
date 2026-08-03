"""Phase 1 supervisor tests — real (trivial) child processes, cross-platform."""
from __future__ import annotations

import asyncio
import sys

from enterprise.control.supervisor import AgentSupervisor


async def test_supervisor_restarts_crashing_child_then_trips_guard():
    events: list[str] = []
    sup = AgentSupervisor(
        [sys.executable, "-c", "import sys; sys.exit(3)"],
        poll_interval=0.02, restart_backoff=0.02,
        max_rapid_restarts=3, rapid_window_seconds=30.0,
        on_event=lambda e, f: events.append(e),
    )
    await sup.start()
    try:
        for _ in range(250):                      # ~5s cap
            if sup.crash_looped:
                break
            await asyncio.sleep(0.02)
        assert sup.crash_looped, "crash-loop guard should trip on a fast-exiting child"
        assert sup.restart_count >= 3
        assert "crash_loop" in events
        assert events.count("spawned") >= 1 and events.count("restart") >= 1
    finally:
        await sup.stop()


async def test_supervisor_keeps_healthy_child_and_stops_cleanly():
    sup = AgentSupervisor(
        [sys.executable, "-c", "import time; time.sleep(30)"],
        poll_interval=0.05, restart_backoff=0.05,
    )
    await sup.start()
    await asyncio.sleep(0.4)
    assert sup.is_alive
    assert sup.restart_count == 0
    await sup.stop()
    assert not sup.is_alive
    assert sup.pid is None


async def test_supervisor_stops_permanently_on_config_error_code():
    """EX_CONFIG (78) is a permanent config error — do not restart, don't crash-loop."""
    events: list[str] = []
    sup = AgentSupervisor(
        [sys.executable, "-c", "import sys; sys.exit(78)"],
        poll_interval=0.02, restart_backoff=0.02,
        max_rapid_restarts=3, rapid_window_seconds=30.0,
        on_event=lambda e, f: events.append(e),
    )
    await sup.start()
    try:
        for _ in range(250):                      # ~5s cap
            if "config_error" in events:
                break
            await asyncio.sleep(0.02)
        assert "config_error" in events, "config error (78) should emit a config_error event"
        assert not sup.crash_looped, "config error must not trip the crash-loop guard"
        assert sup.restart_count == 0, "config error must not trigger any restart"
        assert "restart" not in events
    finally:
        await sup.stop()

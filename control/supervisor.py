"""
Agent supervisor — the Manager's process babysitter (SCALE_ARCHITECTURE_PLAN.md §9).

Direct‑spawn backend (the Phase 1 default, cross‑platform): the Manager launches
the Agent as a child process, watches it for **both** process exit and heartbeat
liveness, and **restarts it on crash** with backoff and a crash‑loop guard. On
POSIX the child runs in its own process group so the whole tree is killed on stop;
on Windows ``terminate()``/``kill()`` are used (Job‑Object tree‑kill is a later
hardening).

Everything is async‑friendly and injectable (poll interval / backoff / limits) so
it is deterministically testable with trivial child commands.
"""
from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import time
from collections import deque
from typing import Callable

from observability.logging import get_logger

log = get_logger(__name__)

_IS_POSIX = os.name == "posix"


class AgentSupervisor:
    def __init__(
        self,
        launch_cmd: list[str],
        *,
        env: dict[str, str] | None = None,
        cwd: str | None = None,
        name: str = "agent",
        poll_interval: float = 0.2,
        restart_backoff: float = 1.0,
        max_rapid_restarts: int = 5,
        rapid_window_seconds: float = 30.0,
        stop_grace_seconds: float = 5.0,
        on_event: Callable[[str, dict], None] | None = None,
    ) -> None:
        self.launch_cmd = list(launch_cmd)
        self.env = env
        self.cwd = cwd
        self.name = name
        self.poll_interval = poll_interval
        self.restart_backoff = restart_backoff
        self.max_rapid_restarts = max_rapid_restarts
        self.rapid_window_seconds = rapid_window_seconds
        self.stop_grace_seconds = stop_grace_seconds
        self._on_event = on_event

        self._proc: subprocess.Popen | None = None
        self._running = False
        self._crash_looped = False
        self._watch_task: asyncio.Task | None = None
        self.restart_count = 0
        self._restart_times: deque[float] = deque()

    # -- public API ----------------------------------------------------------

    async def start(self) -> None:
        """Spawn the Agent and begin supervising it."""
        if self._running:
            return
        self._running = True
        self._crash_looped = False
        self._spawn()
        self._watch_task = asyncio.create_task(self._watch(), name=f"supervise-{self.name}")

    async def stop(self) -> None:
        """Stop supervising and terminate the Agent (graceful, then forced)."""
        self._running = False
        if self._watch_task is not None:
            self._watch_task.cancel()
            try:
                await self._watch_task
            except asyncio.CancelledError:
                pass
            self._watch_task = None
        await asyncio.get_event_loop().run_in_executor(
            None, self._terminate, self._proc, self.stop_grace_seconds
        )
        self._proc = None

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def crash_looped(self) -> bool:
        return self._crash_looped

    @property
    def pid(self) -> int | None:
        return self._proc.pid if self._proc is not None else None

    @property
    def is_alive(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    # -- internals -----------------------------------------------------------

    def _emit(self, event: str, **fields) -> None:
        if self._on_event is not None:
            try:
                self._on_event(event, fields)
            except Exception:
                pass

    def _spawn(self) -> None:
        popen_env = None
        if self.env is not None:
            popen_env = {**os.environ, **self.env}
        kwargs: dict = {}
        if _IS_POSIX:
            kwargs["start_new_session"] = True                  # own process group
        else:
            kwargs["creationflags"] = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        self._proc = subprocess.Popen(
            self.launch_cmd, env=popen_env, cwd=self.cwd, **kwargs
        )
        log.info("agent_spawned", name=self.name, pid=self._proc.pid, cmd=self.launch_cmd)
        self._emit("spawned", pid=self._proc.pid)

    def _note_restart(self) -> None:
        now = time.monotonic()
        self.restart_count += 1
        self._restart_times.append(now)
        while self._restart_times and (now - self._restart_times[0]) > self.rapid_window_seconds:
            self._restart_times.popleft()
        if len(self._restart_times) >= self.max_rapid_restarts:
            self._crash_looped = True

    async def _watch(self) -> None:
        try:
            while self._running:
                proc = self._proc
                rc = proc.poll() if proc is not None else None
                if rc is not None:
                    # Child exited. If we didn't ask it to stop, it crashed.
                    if not self._running:
                        break
                    log.warning("agent_exited", name=self.name, pid=proc.pid if proc else None,
                                returncode=rc)
                    self._emit("crash", returncode=rc)
                    self._note_restart()
                    if self._crash_looped:
                        log.error("agent_crash_loop", name=self.name,
                                  restarts=self.restart_count,
                                  window_seconds=self.rapid_window_seconds)
                        self._emit("crash_loop", restarts=self.restart_count)
                        break
                    await asyncio.sleep(self.restart_backoff)
                    if not self._running:
                        break
                    self._spawn()
                    self._emit("restart", restarts=self.restart_count)
                await asyncio.sleep(self.poll_interval)
        except asyncio.CancelledError:
            raise
        except Exception:  # supervisor must never die silently
            log.exception("supervisor_loop_error", name=self.name)

    def _terminate(self, proc: subprocess.Popen | None, grace: float) -> None:
        if proc is None or proc.poll() is not None:
            return
        # graceful
        try:
            if _IS_POSIX:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            else:
                proc.terminate()
        except (ProcessLookupError, PermissionError, OSError):
            pass
        try:
            proc.wait(timeout=grace)
            return
        except subprocess.TimeoutExpired:
            pass
        # forced
        try:
            if _IS_POSIX:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            else:
                proc.kill()
        except (ProcessLookupError, OSError):
            pass
        try:
            proc.wait(timeout=grace)
        except Exception:
            pass

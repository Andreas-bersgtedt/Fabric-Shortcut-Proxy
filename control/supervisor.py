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

Memory monitoring: tracks RSS (resident memory) per agent process with history
for trend analysis. Can trigger automatic restarts on high memory thresholds.
"""
from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import time
from collections import deque
from typing import Callable

try:
    import psutil
except ImportError:
    psutil = None

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
        memory_alert_threshold_mb: int = 0,
        memory_restart_threshold_mb: int = 0,
        memory_history_samples: int = 60,
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
        
        # Memory monitoring
        self.memory_alert_threshold_mb = memory_alert_threshold_mb
        self.memory_restart_threshold_mb = memory_restart_threshold_mb
        self._memory_history: deque[int] = deque(maxlen=memory_history_samples)  # RSS in bytes
        self._last_memory_alert_time = 0.0

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

    @property
    def rss_bytes(self) -> int:
        """Current process RSS (resident memory) in bytes, or 0 if unavailable."""
        if psutil is None or self._proc is None or not self.is_alive:
            return 0
        try:
            proc = psutil.Process(self._proc.pid)
            return proc.memory_info().rss
        except (psutil.NoSuchProcess, psutil.AccessDenied, OSError):
            return 0

    @property
    def rss_mb(self) -> float:
        """Current process RSS in megabytes."""
        return self.rss_bytes / (1024 * 1024)

    @property
    def avg_rss_mb(self) -> float:
        """Average RSS over memory history (MB), or 0 if no samples."""
        if not self._memory_history:
            return 0.0
        return sum(self._memory_history) / len(self._memory_history) / (1024 * 1024)

    @property
    def peak_rss_mb(self) -> float:
        """Peak RSS from memory history (MB), or 0 if no samples."""
        if not self._memory_history:
            return 0.0
        return max(self._memory_history) / (1024 * 1024)

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

    def _sample_memory(self) -> None:
        """Capture current RSS and check thresholds (no-op if psutil unavailable)."""
        if psutil is None:
            return
        rss = self.rss_bytes
        if rss > 0:
            self._memory_history.append(rss)
        # Check restart threshold (high-memory shutdown)
        if self.memory_restart_threshold_mb > 0 and self.rss_mb >= self.memory_restart_threshold_mb:
            log.warning("agent_memory_restart_triggered",
                        name=self.name, pid=self._proc.pid if self._proc else None,
                        rss_mb=self.rss_mb, threshold_mb=self.memory_restart_threshold_mb)
            self._emit("memory_restart", rss_mb=self.rss_mb, threshold_mb=self.memory_restart_threshold_mb)
            # Initiate graceful restart
            self._trigger_memory_restart()
        # Check alert threshold (but throttle alerts to ~once per 60s to avoid spam)
        elif self.memory_alert_threshold_mb > 0 and self.rss_mb >= self.memory_alert_threshold_mb:
            now = time.monotonic()
            if now - self._last_memory_alert_time > 60.0:
                log.warning("agent_memory_alert",
                            name=self.name, pid=self._proc.pid if self._proc else None,
                            rss_mb=self.rss_mb, threshold_mb=self.memory_alert_threshold_mb)
                self._emit("memory_alert", rss_mb=self.rss_mb, threshold_mb=self.memory_alert_threshold_mb)
                self._last_memory_alert_time = now

    def _trigger_memory_restart(self) -> None:
        """Schedule graceful termination + restart due to high memory."""
        if self._proc is not None and self._proc.poll() is None:
            try:
                if _IS_POSIX:
                    os.killpg(os.getpgid(self._proc.pid), signal.SIGTERM)
                else:
                    self._proc.terminate()
            except (ProcessLookupError, PermissionError, OSError):
                pass

    async def _watch(self) -> None:
        try:
            while self._running:
                proc = self._proc
                rc = proc.poll() if proc is not None else None
                if rc is not None:
                    # Child exited. If we didn't ask it to stop, it crashed.
                    if not self._running:
                        break
                    # EX_CONFIG (78): a PERMANENT config/connectivity error (e.g. a
                    # bad source-DB credential). Restarting can't fix it, so stop this
                    # agent cleanly instead of crash-looping — the Manager UI stays up.
                    if rc == 78:
                        log.error("agent_config_error", name=self.name,
                                  pid=proc.pid if proc else None, returncode=rc,
                                  hint="source DB/config error — not restarting; fix the "
                                       "connection (config builder / DB_URL) then restart the Manager")
                        self._emit("config_error", returncode=rc)
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
                # Sample memory stats every poll
                self._sample_memory()
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

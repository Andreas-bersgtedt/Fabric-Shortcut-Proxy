"""
Table quarantine registry (resilient agent startup).

When a warehouse table can't be brought online at startup (unreachable source,
bad credential, missing column, materialization error), it is *quarantined* here
instead of crashing the whole agent. The agent then serves every healthy table
and every storage-proxy mount, and a background task retries quarantined tables
so they self-heal when the source recovers.

Thread-safe: the retry task and request handlers may touch it concurrently.
"""
from __future__ import annotations

import threading
import time

_lock = threading.Lock()
_quarantined: dict[str, dict] = {}


def quarantine(name: str, reason: str) -> None:
    """Mark a table quarantined (or refresh its reason), preserving first-seen time."""
    with _lock:
        entry = _quarantined.get(name)
        if entry is None:
            _quarantined[name] = {
                "reason": reason, "since": time.time(),
                "attempts": 0, "last_attempt": None,
            }
        else:
            entry["reason"] = reason


def record_attempt(name: str) -> None:
    with _lock:
        entry = _quarantined.get(name)
        if entry is not None:
            entry["attempts"] += 1
            entry["last_attempt"] = time.time()


def release(name: str) -> bool:
    """Clear a table's quarantine. Returns True if it had been quarantined."""
    with _lock:
        return _quarantined.pop(name, None) is not None


def is_quarantined(name: str) -> bool:
    with _lock:
        return name in _quarantined


def names() -> list[str]:
    with _lock:
        return list(_quarantined)


def snapshot() -> dict[str, dict]:
    """Copy of the registry for surfacing in /readyz and admin endpoints."""
    with _lock:
        return {name: dict(entry) for name, entry in _quarantined.items()}


def clear() -> None:
    with _lock:
        _quarantined.clear()

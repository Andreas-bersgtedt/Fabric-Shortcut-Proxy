"""In-process usage counters for explicitly enabled Arrow tokenization fallback."""
from __future__ import annotations

import threading

_lock = threading.Lock()
_fallbacks: dict[str, dict[str, object]] = {}


def record_arrow_fallback(
    *, table: str, column: str, flavor: str, kind: str
) -> None:
    """Record the use of plaintext-through-proxy tokenization, without values/secrets."""
    with _lock:
        state = _fallbacks.setdefault(
            table, {"count": 0, "columns": set(), "flavors": set(), "kinds": set()}
        )
        state["count"] = int(state["count"]) + 1
        state["columns"].add(column)  # type: ignore[union-attr]
        state["flavors"].add(flavor)  # type: ignore[union-attr]
        state["kinds"].add(kind)  # type: ignore[union-attr]


def snapshot() -> dict[str, dict]:
    """Return a JSON-safe fallback summary keyed by exposed table name."""
    with _lock:
        return {
            table: {
                "count": int(state["count"]),
                "columns": sorted(state["columns"]),
                "flavors": sorted(state["flavors"]),
                "kinds": sorted(state["kinds"]),
            }
            for table, state in _fallbacks.items()
        }


def reset() -> None:
    """Clear counters for isolated tests."""
    with _lock:
        _fallbacks.clear()

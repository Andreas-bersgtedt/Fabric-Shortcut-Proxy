"""
In-memory rolling buffer of the most recent log lines.

Feeds the monitor dashboard's log viewer (``GET /_monitor/api/logs``). Captures
two sources into one bounded, thread-safe deque:

  - structlog output, via :func:`capture_renderer` wrapping the console renderer
  - stdlib logging (uvicorn, third-party libs), via :class:`RingBufferHandler`

ANSI colour codes are stripped so the stored text is clean for the web viewer.
"""
from __future__ import annotations

import collections
import logging
import re
import threading

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
_MAX_LINES = 1000


class LogRingBuffer:
    """Bounded, thread-safe ring buffer of the last ``maxlen`` log lines."""

    def __init__(self, maxlen: int = _MAX_LINES) -> None:
        self._buf: collections.deque[str] = collections.deque(maxlen=maxlen)
        self._lock = threading.Lock()
        self.maxlen = maxlen

    def append(self, text: str) -> None:
        clean = _ANSI_RE.sub("", text)
        with self._lock:
            for line in clean.splitlines():
                self._buf.append(line)

    def tail(self, limit: int | None = None, query: str | None = None) -> list[str]:
        with self._lock:
            lines = list(self._buf)
        if query:
            q = query.lower()
            lines = [ln for ln in lines if q in ln.lower()]
        if limit is not None and limit >= 0:
            lines = lines[-limit:]
        return lines

    def clear(self) -> None:
        with self._lock:
            self._buf.clear()

    def __len__(self) -> int:
        with self._lock:
            return len(self._buf)


_buffer = LogRingBuffer()


def get_buffer() -> LogRingBuffer:
    return _buffer


class RingBufferHandler(logging.Handler):
    """Mirror stdlib log records into the shared ring buffer."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            _buffer.append(self.format(record))
        except Exception:  # pragma: no cover - never let logging crash the app
            self.handleError(record)


def capture_renderer(inner):
    """Wrap a structlog renderer so each rendered line is also buffered."""

    def render(logger, name, event_dict):
        rendered = inner(logger, name, event_dict)
        try:
            _buffer.append(rendered if isinstance(rendered, str) else str(rendered))
        except Exception:  # pragma: no cover
            pass
        return rendered

    return render

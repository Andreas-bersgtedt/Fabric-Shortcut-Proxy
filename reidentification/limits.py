"""In-process request quotas for the optional re-identification API."""
from __future__ import annotations

import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import UTC, datetime


@dataclass(frozen=True)
class LimitDecision:
    allowed: bool
    reason: str


class RequestLimiter:
    """Bounded per-principal rolling-minute and UTC-day quota tracker."""

    def __init__(self) -> None:
        self._minute: dict[str, deque[float]] = defaultdict(deque)
        self._day: dict[tuple[str, str], int] = defaultdict(int)
        self._lock = threading.Lock()

    def allow(self, identity: str, *, per_minute: int, per_day: int, now: float | None = None) -> LimitDecision:
        if per_minute < 1 or per_day < 1:
            return LimitDecision(False, "re-identification quota is not configured")
        moment = time.time() if now is None else now
        day = datetime.fromtimestamp(moment, UTC).date().isoformat()
        with self._lock:
            recent = self._minute[identity]
            while recent and recent[0] <= moment - 60:
                recent.popleft()
            if len(recent) >= per_minute:
                return LimitDecision(False, "re-identification minute quota exceeded")
            day_key = (identity, day)
            if self._day[day_key] >= per_day:
                return LimitDecision(False, "re-identification daily quota exceeded")
            recent.append(moment)
            self._day[day_key] += 1
        return LimitDecision(True, "allowed")
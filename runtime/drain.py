"""
Process-local drain state (external-LB graceful draining).

When the Manager sends a ``drain`` command (or an operator drains this agent), the
runtime flips into "draining": ``/readyz`` returns 503 so an external load balancer
deregisters this backend before the process exits, letting in-flight requests
finish. ``/healthz`` stays 200 (the process is still alive) so liveness probes do
not flap during the drain window.
"""
from __future__ import annotations

import threading

_draining = threading.Event()


def set_draining(value: bool = True) -> None:
    """Mark this agent as draining (True) or serving (False)."""
    if value:
        _draining.set()
    else:
        _draining.clear()


def is_draining() -> bool:
    """True once a drain has begun; readiness probes should then fail."""
    return _draining.is_set()

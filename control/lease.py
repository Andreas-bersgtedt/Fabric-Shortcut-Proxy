"""
Leader lease over the shared artifact store — Phase 5 Manager failover
(SCALE_ARCHITECTURE_PLAN.md §14 Phase 5, Risk "Manager is a SPOF").

A best-effort TTL leader election so exactly one Manager is **primary** (supervises
Agents + serves the gateway) while others run as warm **standbys**. The primary
renews the lease on a cadence; if it dies, the lease expires and a standby takes
over. Backed by :class:`~runtime.artifact_store.ArtifactStore` so it works over
any shared backend (local dir / NFS / SMB / Blob later) with no extra infra.

The store guarantees atomic *per-key* writes but not compare-and-swap, so this
uses read -> check -> write -> read-back-verify. A brief dual-holder window is
possible right at expiry; that's acceptable for v1 because Agents serve reads
regardless and the gateway is idempotent. Strict single-writer election
(Raft/etcd) is future work (Phase 6-adjacent).
"""
from __future__ import annotations

import json
import os
import socket
import time
import uuid

from runtime.artifact_store import ArtifactStore, ObjectNotFound
from observability.logging import get_logger

log = get_logger(__name__)

LEASE_KEY = "_control/leader.json"


def _now_ms() -> int:
    return int(time.time() * 1000)


def default_owner_id() -> str:
    """A stable-ish, unique id for this Manager process."""
    try:
        host = socket.gethostname()
    except Exception:
        host = "manager"
    return f"{host}:{os.getpid()}:{uuid.uuid4().hex[:8]}"


class LeaderLease:
    """A TTL leader lease. ``acquire_or_renew`` returns True iff we hold it."""

    def __init__(
        self,
        store: ArtifactStore,
        owner_id: str | None = None,
        *,
        ttl_ms: int = 10_000,
        key: str = LEASE_KEY,
    ) -> None:
        self._store = store
        self.owner_id = owner_id or default_owner_id()
        self.ttl_ms = max(1, ttl_ms)
        self._key = key
        self._is_leader = False

    @property
    def is_leader(self) -> bool:
        return self._is_leader

    def _read(self) -> dict | None:
        try:
            raw = self._store.get(self._key)
        except ObjectNotFound:
            return None
        except Exception as exc:  # noqa: BLE001 - a store blip must not crash the loop
            log.warning("leader_lease_read_error", error=str(exc))
            return None
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception:
            return None

    def _write(self, now_ms: int) -> None:
        rec = {"owner_id": self.owner_id, "renew_ms": now_ms, "ttl_ms": self.ttl_ms}
        self._store.put(self._key, json.dumps(rec).encode("utf-8"))

    def _expired(self, rec: dict, now_ms: int) -> bool:
        return (now_ms - int(rec.get("renew_ms", 0))) > int(rec.get("ttl_ms", self.ttl_ms))

    def acquire_or_renew(self, now_ms: int | None = None) -> bool:
        """Take the lease if free/ours/expired, else stand by. Returns leadership."""
        now_ms = now_ms if now_ms is not None else _now_ms()
        rec = self._read()
        can_take = (
            rec is None
            or rec.get("owner_id") == self.owner_id
            or self._expired(rec, now_ms)
        )
        if not can_take:
            was = self._is_leader
            self._is_leader = False
            if was:
                log.warning("leader_lease_lost", owner_id=self.owner_id,
                            holder=rec.get("owner_id") if rec else None)
            return False
        try:
            self._write(now_ms)
        except Exception as exc:  # noqa: BLE001
            log.warning("leader_lease_write_error", error=str(exc))
            self._is_leader = False
            return False
        back = self._read()
        won = bool(back and back.get("owner_id") == self.owner_id)
        if won and not self._is_leader:
            log.info("leader_lease_acquired", owner_id=self.owner_id)
        self._is_leader = won
        return won

    def release(self) -> None:
        """Voluntarily give up the lease (graceful shutdown) if we hold it."""
        rec = self._read()
        if rec and rec.get("owner_id") == self.owner_id:
            try:
                self._store.delete(self._key)
                log.info("leader_lease_released", owner_id=self.owner_id)
            except Exception as exc:  # noqa: BLE001
                log.warning("leader_lease_release_error", error=str(exc))
        self._is_leader = False

    def current_owner(self) -> str | None:
        rec = self._read()
        return rec.get("owner_id") if rec else None

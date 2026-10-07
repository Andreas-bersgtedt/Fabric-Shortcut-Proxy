"""
Leader lease over the shared artifact store — Phase 5 Manager failover
(docs/SCALE_ARCHITECTURE_PLAN.md §14 Phase 5, Risk "Manager is a SPOF").

A TTL active/passive election over an artifact store with compare-and-swap.
Each takeover increments a fence. Leader mutations validate the current owner,
fence, and expiry against the durable record before committing.
"""
from __future__ import annotations

import json
import hashlib
import os
import socket
import time
import uuid

from fabric_shortcut_proxy.runtime.artifact_store import ArtifactStore, ObjectNotFound
from fabric_shortcut_proxy.observability.logging import get_logger

log = get_logger(__name__)

LEASE_KEY = "_control/leader.json"
PRESENCE_PREFIX = "_control/managers"


def _now_ms() -> int:
    return int(time.time() * 1000)


def default_owner_id() -> str:
    """A stable-ish, unique id for this Manager process."""
    try:
        host = socket.gethostname()
    except Exception:
        host = "manager"
    return f"{host}:{os.getpid()}:{uuid.uuid4().hex[:8]}"


class LeaseStoreError(RuntimeError):
    """The durable lease record could not be read or conditionally written."""


class StaleLeaderError(RuntimeError):
    """A Manager attempted a mutation outside its active leadership term."""


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
        self.fence = 0
        self.last_error = ""
        self._presence_key = (
            f"{PRESENCE_PREFIX}/"
            f"{hashlib.sha256(self.owner_id.encode('utf-8')).hexdigest()}.json"
        )

    @property
    def is_leader(self) -> bool:
        return self._is_leader

    def _read_raw(self) -> tuple[bytes | None, dict | None]:
        try:
            raw = self._store.get(self._key)
        except ObjectNotFound:
            return None, None
        except Exception as exc:  # noqa: BLE001
            raise LeaseStoreError(f"leader lease read failed: {exc}") from exc
        try:
            record = json.loads(raw.decode("utf-8"))
        except Exception as exc:
            raise LeaseStoreError("leader lease record is invalid") from exc
        if not isinstance(record, dict):
            raise LeaseStoreError("leader lease record must be an object")
        return raw, record

    def _record(
        self,
        now_ms: int,
        fence: int,
        owner_id: str | None = None,
        *,
        state: dict | None = None,
    ) -> bytes:
        record = {
            "owner_id": self.owner_id if owner_id is None else owner_id,
            "renew_ms": now_ms,
            "ttl_ms": self.ttl_ms,
            "fence": fence,
            "state": dict(state or {}),
        }
        return json.dumps(record, sort_keys=True, separators=(",", ":")).encode("utf-8")

    def _expired(self, rec: dict, now_ms: int) -> bool:
        return (now_ms - int(rec.get("renew_ms", 0))) > int(rec.get("ttl_ms", self.ttl_ms))

    def acquire_or_renew(self, now_ms: int | None = None) -> bool:
        """Take the lease if free/ours/expired, else stand by. Returns leadership."""
        now_ms = now_ms if now_ms is not None else _now_ms()
        for _attempt in range(4):
            try:
                raw, rec = self._read_raw()
                expired = bool(rec and self._expired(rec, now_ms))
                same_live_owner = bool(
                    rec
                    and rec.get("owner_id") == self.owner_id
                    and not expired
                )
                can_take = (
                    rec is None
                    or not rec.get("owner_id")
                    or expired
                    or same_live_owner
                )
                if not can_take:
                    self._lose(rec.get("owner_id") if rec else None)
                    self.last_error = ""
                    return False
                previous_fence = int((rec or {}).get("fence", 0))
                fence = previous_fence if same_live_owner else previous_fence + 1
                if self._store.compare_and_swap(
                    self._key,
                    raw,
                    self._record(
                        now_ms,
                        fence,
                        state=(rec or {}).get("state"),
                    ),
                ):
                    if not self._is_leader:
                        log.info(
                            "leader_lease_acquired",
                            owner_id=self.owner_id,
                            fence=fence,
                        )
                    self._is_leader = True
                    self.fence = fence
                    self.last_error = ""
                    return True
            except Exception as exc:  # noqa: BLE001
                self.last_error = str(exc)
                log.warning("leader_lease_store_error", error=str(exc))
                self._lose(None)
                return False
        self.last_error = "leader lease compare-and-swap contention"
        self._lose(None)
        return False

    def _lose(self, holder: str | None) -> None:
        was = self._is_leader
        self._is_leader = False
        self.fence = 0
        if was:
            log.warning(
                "leader_lease_lost",
                owner_id=self.owner_id,
                holder=holder,
            )

    def validate(
        self,
        *,
        owner_id: str | None = None,
        fence: int | None = None,
        now_ms: int | None = None,
    ) -> dict:
        """Return the live record or reject a stale Manager mutation."""
        expected_owner = owner_id or self.owner_id
        expected_fence = self.fence if fence is None else int(fence)
        now_ms = _now_ms() if now_ms is None else int(now_ms)
        try:
            _raw, rec = self._read_raw()
        except LeaseStoreError:
            self._lose(None)
            raise
        if (
            rec is None
            or rec.get("owner_id") != expected_owner
            or int(rec.get("fence", -1)) != expected_fence
            or self._expired(rec, now_ms)
        ):
            self._lose(rec.get("owner_id") if rec else None)
            raise StaleLeaderError(
                f"Manager leadership term is stale: owner={expected_owner!r}, "
                f"fence={expected_fence}"
            )
        return rec

    def release(self) -> None:
        """Voluntarily give up the lease (graceful shutdown) if we hold it."""
        try:
            raw, rec = self._read_raw()
            if (
                rec
                and rec.get("owner_id") == self.owner_id
                and int(rec.get("fence", -1)) == self.fence
            ):
                released = self._store.compare_and_swap(
                    self._key,
                    raw,
                    self._record(
                        0,
                        int(rec.get("fence", self.fence)),
                        owner_id="",
                        state=rec.get("state"),
                    ),
                )
                if released:
                    log.info("leader_lease_released", owner_id=self.owner_id)
        except Exception as exc:  # noqa: BLE001
            self.last_error = str(exc)
            log.warning("leader_lease_release_error", error=str(exc))
        finally:
            self._is_leader = False
            self.fence = 0

    def status(self, now_ms: int | None = None) -> dict:
        now_ms = _now_ms() if now_ms is None else int(now_ms)
        try:
            _raw, rec = self._read_raw()
            self.last_error = ""
        except LeaseStoreError as exc:
            self.last_error = str(exc)
            return {
                "available": False,
                "degraded": True,
                "reason": self.last_error,
                "owner_id": None,
                "fence": 0,
                "lease_age_ms": None,
                "remaining_ms": None,
                "expired": True,
                "healthy_standbys": 0,
                "managers": [],
            }
        renew_ms = int((rec or {}).get("renew_ms", 0))
        ttl_ms = int((rec or {}).get("ttl_ms", self.ttl_ms))
        age_ms = max(0, now_ms - renew_ms) if renew_ms else None
        expired = rec is None or not rec.get("owner_id") or self._expired(rec, now_ms)
        managers = self._manager_presence(now_ms)
        healthy_standbys = sum(
            1
            for manager in managers
            if manager["healthy"] and manager["role"] == "standby"
        )
        degraded_reasons = []
        if expired:
            degraded_reasons.append("leader lease is missing or expired")
        if not expired and healthy_standbys == 0:
            degraded_reasons.append("no healthy standby Manager is reporting")
        return {
            "available": True,
            "degraded": bool(degraded_reasons),
            "reason": "; ".join(degraded_reasons),
            "owner_id": (rec or {}).get("owner_id") or None,
            "fence": int((rec or {}).get("fence", 0)),
            "lease_age_ms": age_ms,
            "remaining_ms": (
                max(0, ttl_ms - age_ms) if age_ms is not None else 0
            ),
            "expired": expired,
            "healthy_standbys": healthy_standbys,
            "managers": managers,
        }

    def current_owner(self) -> str | None:
        try:
            _raw, rec = self._read_raw()
        except LeaseStoreError:
            return None
        return (rec.get("owner_id") or None) if rec else None

    def read_state(self, name: str):
        """Return a durable Manager-state value, including while on standby."""
        _raw, rec = self._read_raw()
        return ((rec or {}).get("state") or {}).get(name)

    def mutate_state(self, name: str, value, *, now_ms: int | None = None) -> int:
        """Commit Manager state in the same CAS record as the active lease."""
        now_ms = _now_ms() if now_ms is None else int(now_ms)
        for _attempt in range(8):
            raw, rec = self._read_raw()
            if (
                rec is None
                or rec.get("owner_id") != self.owner_id
                or int(rec.get("fence", -1)) != self.fence
                or self._expired(rec, now_ms)
            ):
                self._lose(rec.get("owner_id") if rec else None)
                raise StaleLeaderError(
                    f"Manager leadership term is stale: owner={self.owner_id!r}, "
                    f"fence={self.fence}"
                )
            state = dict(rec.get("state") or {})
            state[name] = value
            updated = dict(rec)
            updated["state"] = state
            data = json.dumps(
                updated,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
            if self._store.compare_and_swap(self._key, raw, data):
                self.last_error = ""
                return int(updated["fence"])
        raise LeaseStoreError("Manager state compare-and-swap contention")

    def publish_presence(
        self,
        role: str,
        *,
        now_ms: int | None = None,
    ) -> None:
        now_ms = _now_ms() if now_ms is None else int(now_ms)
        record = {
            "owner_id": self.owner_id,
            "role": "primary" if role == "primary" else "standby",
            "observed_ms": now_ms,
            "fence": self.fence if role == "primary" else 0,
        }
        self._store.put(
            self._presence_key,
            json.dumps(
                record,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8"),
        )

    def remove_presence(self) -> None:
        try:
            self._store.delete(self._presence_key)
        except Exception as exc:  # noqa: BLE001
            log.warning("manager_presence_remove_error", error=str(exc))

    def _manager_presence(self, now_ms: int) -> list[dict]:
        managers = []
        try:
            items = self._store.list(f"{PRESENCE_PREFIX}/")
            for item in items:
                try:
                    value = json.loads(self._store.get(item.key).decode("utf-8"))
                    observed_ms = int(value.get("observed_ms", 0))
                    managers.append({
                        "owner_id": str(value.get("owner_id", "")),
                        "role": str(value.get("role", "standby")),
                        "fence": int(value.get("fence", 0)),
                        "age_ms": max(0, now_ms - observed_ms),
                        "healthy": (
                            observed_ms > 0
                            and now_ms - observed_ms <= self.ttl_ms * 2
                        ),
                    })
                except (ObjectNotFound, UnicodeDecodeError, ValueError, TypeError):
                    continue
        except Exception as exc:  # noqa: BLE001
            log.warning("manager_presence_read_error", error=str(exc))
        return sorted(managers, key=lambda item: item["owner_id"])

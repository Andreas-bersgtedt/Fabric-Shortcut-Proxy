"""
Agent registry — the Manager's in‑memory view of the Agent fleet (Phase 1).

Tracks each registered Agent's lease, last heartbeat, health, and the epochs it
serves, and decides liveness from heartbeat freshness. It also holds a per‑Agent
queue of pending Manager→Agent :class:`ControlCommand`s that ride back on the
heartbeat response (the REST "Agent‑pull" model — open decision #1).

Phase 1 is single‑Manager, in‑memory (durable/Raft is Phase 5). Thread‑safe so
the control‑plane server and the supervisor can touch it concurrently.
"""
from __future__ import annotations

import threading
import time
import uuid
import ipaddress
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from enterprise.control.contract import (
    CONTRACT_VERSION,
    RegisterRequest,
    RegisterResponse,
    HeartbeatRequest,
    AgentHealth,
    ControlCommand,
    contract_compatible,
)

if TYPE_CHECKING:
    from enterprise.control.lease import LeaderLease


def _now() -> float:
    return time.monotonic()


@dataclass
class AgentRecord:
    agent_id: str
    lease_id: str
    host: str
    port: int
    os: str
    version: str
    capacity_hint: int
    registered_at: float
    last_seen: float
    advertise_host: str = ""
    health: AgentHealth = field(default_factory=AgentHealth)
    serving_tables: list[str] = field(default_factory=list)
    epochs: dict[str, int] = field(default_factory=dict)
    capabilities: list[str] = field(default_factory=list)
    shard_index: int = -1
    commands: list[ControlCommand] = field(default_factory=list)
    draining: bool = False

    def to_public(self) -> dict:
        """A JSON‑able view for admin/observability (no internal lease).

        ``host`` is the routable address the LB/gateway should dial: the advertised
        host when the agent supplied one, else its registered bind host.
        """
        return {
            "agent_id": self.agent_id,
            "host": self.advertise_host or self.host,
            "bind_host": self.host,
            "port": self.port,
            "os": self.os,
            "version": self.version,
            "capacity_hint": self.capacity_hint,
            "serving_tables": list(self.serving_tables),
            "epochs": dict(self.epochs),
            "capabilities": list(self.capabilities),
            "shard_index": self.shard_index,
            "health": self.health.to_dict(),
            "age_seconds": round(_now() - self.registered_at, 1),
            "seconds_since_heartbeat": round(_now() - self.last_seen, 1),
            "pending_commands": len(self.commands),
            "draining": self.draining,
        }


class LeaseError(Exception):
    """Raised when a heartbeat presents an unknown/expired lease."""


class Registry:
    """Thread‑safe registry of Agents with heartbeat‑based liveness."""

    def __init__(
        self, *, heartbeat_ms: int = 2000, miss_limit: int = 3,
        allowed_hosts: tuple[str, ...] | None = None,
    ) -> None:
        self.heartbeat_ms = heartbeat_ms
        self.miss_limit = max(1, miss_limit)
        self._agents: dict[str, AgentRecord] = {}
        self._lock = threading.Lock()
        self._allowed_hosts = tuple(x.strip().lower() for x in (allowed_hosts or ()) if x.strip())
        self._durable_lease: LeaderLease | None = None

    def _snapshot_locked(self) -> dict:
        now = _now()
        agents = []
        for record in sorted(self._agents.values(), key=lambda item: item.agent_id):
            agents.append({
                "agent_id": record.agent_id,
                "lease_id": record.lease_id,
                "host": record.host,
                "port": record.port,
                "os": record.os,
                "version": record.version,
                "capacity_hint": record.capacity_hint,
                "registered_age_ms": max(0, int((now - record.registered_at) * 1000)),
                "last_seen_age_ms": max(0, int((now - record.last_seen) * 1000)),
                "advertise_host": record.advertise_host,
                "health": record.health.to_dict(),
                "serving_tables": list(record.serving_tables),
                "epochs": dict(record.epochs),
                "capabilities": list(record.capabilities),
                "shard_index": record.shard_index,
                "commands": [command.to_dict() for command in record.commands],
                "draining": record.draining,
            })
        return {"version": 1, "agents": agents}

    def _restore_locked(self, snapshot: dict) -> None:
        if not isinstance(snapshot, dict) or snapshot.get("version") != 1:
            raise ValueError("unsupported durable registry snapshot")
        records = snapshot.get("agents")
        if not isinstance(records, list):
            raise ValueError("durable registry snapshot agents must be a list")
        now = _now()
        restored: dict[str, AgentRecord] = {}
        for value in records:
            if not isinstance(value, dict):
                raise ValueError("durable registry agent record must be an object")
            agent_id = str(value["agent_id"])
            restored[agent_id] = AgentRecord(
                agent_id=agent_id,
                lease_id=str(value["lease_id"]),
                host=str(value["host"]),
                port=int(value["port"]),
                os=str(value.get("os", "")),
                version=str(value.get("version", "")),
                capacity_hint=int(value.get("capacity_hint", 0)),
                registered_at=now - max(0, int(value.get("registered_age_ms", 0))) / 1000,
                last_seen=now - max(0, int(value.get("last_seen_age_ms", 0))) / 1000,
                advertise_host=str(value.get("advertise_host", "")),
                health=AgentHealth.from_dict(value.get("health") or {}),
                serving_tables=[str(item) for item in value.get("serving_tables", [])],
                epochs={
                    str(key): int(epoch)
                    for key, epoch in (value.get("epochs") or {}).items()
                },
                capabilities=sorted(
                    {str(item) for item in value.get("capabilities", [])}
                ),
                shard_index=int(value.get("shard_index", -1)),
                commands=[
                    ControlCommand.from_dict(item)
                    for item in value.get("commands", [])
                ],
                draining=bool(value.get("draining", False)),
            )
        self._agents = restored

    def _persist_locked(self, before: dict) -> None:
        if self._durable_lease is None:
            return
        try:
            self._durable_lease.mutate_state(
                "registry",
                self._snapshot_locked(),
            )
        except Exception:
            self._restore_locked(before)
            raise

    def attach_durable_state(
        self,
        lease: LeaderLease,
        *,
        restore: bool = True,
    ) -> int:
        """Attach the active term and restore the last durable fleet snapshot."""
        with self._lock:
            self._durable_lease = lease
            if restore:
                snapshot = lease.read_state("registry")
                if snapshot is not None:
                    self._restore_locked(snapshot)
            return lease.mutate_state("registry", self._snapshot_locked())

    def detach_durable_state(self) -> None:
        with self._lock:
            self._durable_lease = None

    def _host_allowed(self, host: str) -> bool:
        if not self._allowed_hosts:
            return True
        value = host.strip().lower().rstrip(".")
        for entry in self._allowed_hosts:
            if value == entry.rstrip("."):
                return True
            try:
                address = ipaddress.ip_address(value)
                if address in ipaddress.ip_network(entry, strict=False):
                    return True
            except ValueError:
                continue
        return False

    # -- registration --------------------------------------------------------

    def register(self, req: RegisterRequest) -> RegisterResponse:
        """Register (or re‑register) an Agent; returns its lease + heartbeat cadence.

        Re‑registration (same ``agent_id``) issues a fresh lease and clears stale
        queued commands — used when a crashed Agent is respawned.
        """
        advertised = req.advertise_host or req.host
        if not (1 <= req.port <= 65535):
            raise ValueError("agent port must be in 1..65535")
        if req.shard_index < -1:
            raise ValueError("agent shard_index must be >= -1")
        if not contract_compatible(req.contract_version):
            raise ValueError(
                f"incompatible control contract {req.contract_version!r}; "
                f"Manager uses {CONTRACT_VERSION}"
            )
        if not self._host_allowed(req.host) or not self._host_allowed(advertised):
            raise ValueError("agent host is not allowed by AGENT_HOST_ALLOWLIST")
        lease = uuid.uuid4().hex
        now = _now()
        with self._lock:
            before = self._snapshot_locked()
            self._agents[req.agent_id] = AgentRecord(
                agent_id=req.agent_id, lease_id=lease, host=req.host, port=req.port,
                os=req.os, version=req.version, capacity_hint=req.capacity_hint,
                advertise_host=req.advertise_host,
                capabilities=sorted(set(req.capabilities)),
                shard_index=req.shard_index,
                registered_at=now, last_seen=now,
            )
            self._persist_locked(before)
        return RegisterResponse(
            lease_id=lease,
            heartbeat_ms=self.heartbeat_ms,
            contract_version=CONTRACT_VERSION,
        )

    # -- heartbeat -----------------------------------------------------------

    def heartbeat(self, req: HeartbeatRequest) -> list[ControlCommand]:
        """Record a heartbeat and return any queued commands for the Agent.

        Raises :class:`LeaseError` if the agent is unknown or the lease mismatches
        (e.g. the Manager restarted, or a zombie Agent from a prior lease) — the
        Agent should re‑register on that error.
        """
        with self._lock:
            before = self._snapshot_locked()
            rec = self._agents.get(req.agent_id)
            if rec is None or rec.lease_id != req.lease_id:
                raise LeaseError(f"unknown or stale lease for agent {req.agent_id!r}")
            rec.last_seen = _now()
            rec.health = req.health
            rec.serving_tables = list(req.serving_tables)
            rec.epochs = dict(req.epochs)
            pending, rec.commands = rec.commands, []
            self._persist_locked(before)
            return pending

    # -- commands ------------------------------------------------------------

    def queue_command(self, agent_id: str, cmd: ControlCommand) -> bool:
        """Queue a Manager→Agent command; delivered on the next heartbeat."""
        with self._lock:
            before = self._snapshot_locked()
            rec = self._agents.get(agent_id)
            if rec is None:
                return False
            rec.commands.append(cmd)
            if cmd.kind == "drain":
                rec.draining = True
            self._persist_locked(before)
            return True

    def broadcast(self, cmd: ControlCommand) -> int:
        """Queue a command to every registered Agent. Returns the count."""
        with self._lock:
            before = self._snapshot_locked()
            for rec in self._agents.values():
                rec.commands.append(cmd)
                if cmd.kind == "drain":
                    rec.draining = True
            self._persist_locked(before)
            return len(self._agents)

    # -- liveness / introspection -------------------------------------------

    def _is_alive(self, rec: AgentRecord, now: float) -> bool:
        deadline = (self.heartbeat_ms / 1000.0) * self.miss_limit
        return (now - rec.last_seen) <= deadline

    def is_alive(self, agent_id: str) -> bool:
        with self._lock:
            rec = self._agents.get(agent_id)
            return rec is not None and self._is_alive(rec, _now())

    def dead_agents(self) -> list[str]:
        """Ids of registered Agents that have missed ``miss_limit`` heartbeats."""
        now = _now()
        with self._lock:
            return [a.agent_id for a in self._agents.values() if not self._is_alive(a, now)]

    def get(self, agent_id: str) -> AgentRecord | None:
        with self._lock:
            return self._agents.get(agent_id)

    def remove(self, agent_id: str) -> bool:
        with self._lock:
            before = self._snapshot_locked()
            removed = self._agents.pop(agent_id, None) is not None
            if removed:
                self._persist_locked(before)
            return removed

    def list_public(self) -> list[dict]:
        with self._lock:
            return [a.to_public() for a in self._agents.values()]

    def count(self) -> int:
        with self._lock:
            return len(self._agents)

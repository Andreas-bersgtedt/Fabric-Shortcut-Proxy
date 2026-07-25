"""
Manager ↔ Agent contract — transport‑neutral message shapes.

This is the **frozen logical contract** (SCALE_ARCHITECTURE_PLAN.md §5) expressed
as Python dataclasses with a dict/JSON codec. It mirrors ``control/proto/control.proto``
1:1 so the same messages can travel over gRPC/protobuf **or** REST/JSON — deferring
open decision #1 (transport) without blocking Phase 1.

Conventions:
  - Field names match the ``.proto`` (snake_case) so JSON ↔ protobuf is mechanical.
  - Every message has ``to_dict()`` / ``from_dict()`` (and ``to_json``/``from_json``).
  - Enumerations are plain strings (``os``: "windows"|"linux"; ``table_format``:
    "iceberg"|"delta") to stay language‑neutral.
  - **Epoch** is the monotonic published version of a table; the Manager never
    advertises an epoch until every :class:`SplitRef` is materialized with a real
    ``size_bytes`` (this is what removes the "declared size ≠ served size" class
    of bugs at scale).
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from typing import Any

# Bump only for breaking changes to the wire shapes. Agents and the Manager
# exchange this on Register so a mismatch fails fast.
CONTRACT_VERSION = "1.0"


# ---------------------------------------------------------------------------
# Value types
# ---------------------------------------------------------------------------

@dataclass
class KeyRange:
    """Half‑open range ``[lo, hi)`` on the integer split key (range‑based splits)."""
    lo: int
    hi: int

    def to_dict(self) -> dict[str, Any]:
        return {"lo": self.lo, "hi": self.hi}

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "KeyRange":
        return cls(lo=int(d["lo"]), hi=int(d["hi"]))


@dataclass
class Column:
    """One projected column of a table schema."""
    field_id: int
    name: str
    iceberg_type: str
    nullable: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {"field_id": self.field_id, "name": self.name,
                "iceberg_type": self.iceberg_type, "nullable": self.nullable}

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Column":
        return cls(field_id=int(d["field_id"]), name=str(d["name"]),
                   iceberg_type=str(d["iceberg_type"]), nullable=bool(d.get("nullable", True)))


@dataclass
class SplitRef:
    """A materialized data file in the artifact store (net‑current for an epoch)."""
    object_key: str
    size_bytes: int
    record_count: int
    content_hash: str
    range: KeyRange | None = None

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "object_key": self.object_key,
            "size_bytes": self.size_bytes,
            "record_count": self.record_count,
            "content_hash": self.content_hash,
        }
        if self.range is not None:
            d["range"] = self.range.to_dict()
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "SplitRef":
        rng = d.get("range")
        return cls(
            object_key=str(d["object_key"]),
            size_bytes=int(d["size_bytes"]),
            record_count=int(d["record_count"]),
            content_hash=str(d["content_hash"]),
            range=KeyRange.from_dict(rng) if rng else None,
        )


# ---------------------------------------------------------------------------
# Table state
# ---------------------------------------------------------------------------

@dataclass
class SnapshotManifest:
    """The published state of a table at a given epoch (what Agents serve)."""
    table: str
    epoch: int
    table_format: str            # "iceberg" | "delta"
    splits: list[SplitRef] = field(default_factory=list)
    metadata_keys: list[str] = field(default_factory=list)  # keys of metadata/_delta_log objects

    def to_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "epoch": self.epoch,
            "table_format": self.table_format,
            "splits": [s.to_dict() for s in self.splits],
            "metadata_keys": list(self.metadata_keys),
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "SnapshotManifest":
        return cls(
            table=str(d["table"]),
            epoch=int(d["epoch"]),
            table_format=str(d["table_format"]),
            splits=[SplitRef.from_dict(s) for s in d.get("splits", [])],
            metadata_keys=list(d.get("metadata_keys", [])),
        )


# ---------------------------------------------------------------------------
# Agent lifecycle
# ---------------------------------------------------------------------------

@dataclass
class RegisterRequest:
    agent_id: str
    host: str
    port: int
    os: str                      # "windows" | "linux"
    version: str                 # build/git sha
    capacity_hint: int = 0       # cores/mem hint for scheduling
    contract_version: str = CONTRACT_VERSION

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "RegisterRequest":
        return cls(
            agent_id=str(d["agent_id"]), host=str(d["host"]), port=int(d["port"]),
            os=str(d["os"]), version=str(d["version"]),
            capacity_hint=int(d.get("capacity_hint", 0)),
            contract_version=str(d.get("contract_version", CONTRACT_VERSION)),
        )


@dataclass
class RegisterResponse:
    lease_id: str
    heartbeat_ms: int
    contract_version: str = CONTRACT_VERSION

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "RegisterResponse":
        return cls(lease_id=str(d["lease_id"]), heartbeat_ms=int(d["heartbeat_ms"]),
                   contract_version=str(d.get("contract_version", CONTRACT_VERSION)))


@dataclass
class AgentHealth:
    cpu_pct: float = 0.0
    mem_bytes: int = 0
    cache_bytes: int = 0
    inflight: int = 0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "AgentHealth":
        return cls(cpu_pct=float(d.get("cpu_pct", 0.0)), mem_bytes=int(d.get("mem_bytes", 0)),
                   cache_bytes=int(d.get("cache_bytes", 0)), inflight=int(d.get("inflight", 0)))


@dataclass
class HeartbeatRequest:
    agent_id: str
    lease_id: str
    health: AgentHealth = field(default_factory=AgentHealth)
    serving_tables: list[str] = field(default_factory=list)
    epochs: dict[str, int] = field(default_factory=dict)   # table -> epoch currently served

    def to_dict(self) -> dict[str, Any]:
        return {
            "agent_id": self.agent_id,
            "lease_id": self.lease_id,
            "health": self.health.to_dict(),
            "serving_tables": list(self.serving_tables),
            "epochs": dict(self.epochs),
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "HeartbeatRequest":
        return cls(
            agent_id=str(d["agent_id"]), lease_id=str(d["lease_id"]),
            health=AgentHealth.from_dict(d.get("health", {})),
            serving_tables=list(d.get("serving_tables", [])),
            epochs={str(k): int(v) for k, v in d.get("epochs", {}).items()},
        )


# ---------------------------------------------------------------------------
# Materialization work queue
# ---------------------------------------------------------------------------

@dataclass
class MaterializeTask:
    table: str
    epoch: int
    split_index: int
    source_table: str
    output_key: str              # where to write in the artifact store
    schema: list[Column] = field(default_factory=list)
    range: KeyRange | None = None

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "table": self.table, "epoch": self.epoch, "split_index": self.split_index,
            "source_table": self.source_table, "output_key": self.output_key,
            "schema": [c.to_dict() for c in self.schema],
        }
        if self.range is not None:
            d["range"] = self.range.to_dict()
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "MaterializeTask":
        rng = d.get("range")
        return cls(
            table=str(d["table"]), epoch=int(d["epoch"]), split_index=int(d["split_index"]),
            source_table=str(d["source_table"]), output_key=str(d["output_key"]),
            schema=[Column.from_dict(c) for c in d.get("schema", [])],
            range=KeyRange.from_dict(rng) if rng else None,
        )


@dataclass
class TaskResult:
    agent_id: str
    table: str
    epoch: int
    split_index: int
    ok: bool
    size_bytes: int = 0
    record_count: int = 0
    content_hash: str = ""
    error: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "TaskResult":
        return cls(
            agent_id=str(d["agent_id"]), table=str(d["table"]), epoch=int(d["epoch"]),
            split_index=int(d["split_index"]), ok=bool(d["ok"]),
            size_bytes=int(d.get("size_bytes", 0)), record_count=int(d.get("record_count", 0)),
            content_hash=str(d.get("content_hash", "")), error=str(d.get("error", "")),
        )


# ---------------------------------------------------------------------------
# Assignment + Manager -> Agent commands (mirror the proto oneof)
# ---------------------------------------------------------------------------

@dataclass
class Assignment:
    agent_id: str
    tables: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {"agent_id": self.agent_id, "tables": list(self.tables)}

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Assignment":
        return cls(agent_id=str(d["agent_id"]), tables=list(d.get("tables", [])))


@dataclass
class Drain:
    grace_ms: int = 5000

    def to_dict(self) -> dict[str, Any]:
        return {"grace_ms": self.grace_ms}

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Drain":
        return cls(grace_ms=int(d.get("grace_ms", 5000)))


@dataclass
class ReloadConfig:
    reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {"reason": self.reason}

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "ReloadConfig":
        return cls(reason=str(d.get("reason", "")))


@dataclass
class PublishSnapshot:
    table: str
    epoch: int

    def to_dict(self) -> dict[str, Any]:
        return {"table": self.table, "epoch": self.epoch}

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "PublishSnapshot":
        return cls(table=str(d["table"]), epoch=int(d["epoch"]))


@dataclass
class ControlCommand:
    """A Manager -> Agent command (the proto ``oneof cmd``). Exactly one payload
    field is set, identified by ``kind``."""
    kind: str                                   # "drain" | "reload" | "materialize" | "publish"
    drain: Drain | None = None
    reload: ReloadConfig | None = None
    materialize: MaterializeTask | None = None
    publish: PublishSnapshot | None = None

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"kind": self.kind}
        if self.drain is not None:
            d["drain"] = self.drain.to_dict()
        if self.reload is not None:
            d["reload"] = self.reload.to_dict()
        if self.materialize is not None:
            d["materialize"] = self.materialize.to_dict()
        if self.publish is not None:
            d["publish"] = self.publish.to_dict()
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "ControlCommand":
        return cls(
            kind=str(d["kind"]),
            drain=Drain.from_dict(d["drain"]) if d.get("drain") else None,
            reload=ReloadConfig.from_dict(d["reload"]) if d.get("reload") else None,
            materialize=MaterializeTask.from_dict(d["materialize"]) if d.get("materialize") else None,
            publish=PublishSnapshot.from_dict(d["publish"]) if d.get("publish") else None,
        )


@dataclass
class Ack:
    ok: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {"ok": self.ok}

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Ack":
        return cls(ok=bool(d.get("ok", True)))


# ---------------------------------------------------------------------------
# JSON helpers (usable now for a REST transport; protobuf later maps the same)
# ---------------------------------------------------------------------------

def to_json(msg: Any) -> str:
    """Serialize any contract message that exposes ``to_dict()`` to JSON."""
    return json.dumps(msg.to_dict(), separators=(",", ":"))


def from_json(cls: Any, text: str) -> Any:
    """Deserialize JSON into ``cls`` via its ``from_dict()``."""
    return cls.from_dict(json.loads(text))

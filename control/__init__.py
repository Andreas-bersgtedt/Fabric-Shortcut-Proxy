"""
Manager / Controller role package (docs/SCALE_ARCHITECTURE_PLAN.md §4.1).

The **control plane** (the "Primary") owns configuration + secrets, the
authoritative per‑table **published epoch** (Iceberg metadata / Delta
``_delta_log``), split planning, materialization orchestration, and Agent
supervision (heartbeat + restart).

Phase 0 establishes the seam by freezing the Manager↔Agent **contract** in two
equivalent forms:
  - :mod:`control.contract` — transport‑neutral Python dataclasses + a
    dict/JSON codec, usable immediately (Phase 1) regardless of whether the
    transport is gRPC or REST.
  - ``control/proto/control.proto`` — the gRPC/protobuf form of the same
    contract, frozen for the future C++ Agent.

No Manager process exists yet; this package only defines the shared shapes so
later phases can build against a stable contract.
"""
from __future__ import annotations

from control.contract import (
    CONTRACT_VERSION,
    KeyRange,
    Column,
    SplitRef,
    SnapshotManifest,
    AgentHealth,
    RegisterRequest,
    RegisterResponse,
    HeartbeatRequest,
    MaterializeTask,
    TaskResult,
    Assignment,
    Drain,
    ReloadConfig,
    PublishSnapshot,
    ControlCommand,
    Ack,
)

__all__ = [
    "CONTRACT_VERSION",
    "KeyRange",
    "Column",
    "SplitRef",
    "SnapshotManifest",
    "AgentHealth",
    "RegisterRequest",
    "RegisterResponse",
    "HeartbeatRequest",
    "MaterializeTask",
    "TaskResult",
    "Assignment",
    "Drain",
    "ReloadConfig",
    "PublishSnapshot",
    "ControlCommand",
    "Ack",
]

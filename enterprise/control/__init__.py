"""
Manager / Controller role package (docs/SCALE_ARCHITECTURE_PLAN.md §4.1).

The **control plane** (the "Primary") owns configuration + secrets, the
authoritative per‑table **published epoch** (Iceberg metadata / Delta
``_delta_log``), split planning, materialization orchestration, and Agent
supervision (heartbeat + restart).

Phase 0 establishes the seam by freezing the Manager↔Agent **contract** in two
equivalent forms:
  - :mod:`enterprise.control.contract` — transport‑neutral Python dataclasses + a
    dict/JSON codec, usable immediately (Phase 1) regardless of whether the
    transport is gRPC or REST.
  - ``control/proto/control.proto`` — the gRPC/protobuf form of the same
    contract shared by the Manager, Python Agents, and C++ serving Agents.
"""
from __future__ import annotations

from enterprise.control.contract import (
    CONTRACT_VERSION,
    MIN_COMPATIBLE_CONTRACT_VERSION,
    TASK_STATES,
    TASK_TERMINAL_STATES,
    TASK_TRANSITIONS,
    RESULT_CODES,
    contract_compatible,
    valid_task_transition,
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
    "MIN_COMPATIBLE_CONTRACT_VERSION",
    "TASK_STATES",
    "TASK_TERMINAL_STATES",
    "TASK_TRANSITIONS",
    "RESULT_CODES",
    "contract_compatible",
    "valid_task_transition",
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

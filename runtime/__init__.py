"""
Agent / Runtime role package (docs/SCALE_ARCHITECTURE_PLAN.md §4.2).

The **runtime** is the stateless worker: it speaks the S3 data plane
(GET/HEAD/List, ranged reads, SigV4), generates Parquet from SQL pushdown, and
serves objects from a shared **artifact store**. It holds no authoritative
state — the Manager (``control`` package) owns configuration and the published
table epochs.

Phase 0 establishes the seam by introducing the artifact-store interface here
(the first concrete runtime-layer component). Existing modules that logically
belong to the runtime role — ``s3`` (router), ``parquet`` (generator),
``cache`` (local read cache), plus the Iceberg/Delta *serving* paths — keep
their current locations for now; they are migrated behind this package
incrementally in later phases to avoid a risky big-bang move (the guardrail is
"single process unchanged; all tests green").
"""
from __future__ import annotations

from runtime.artifact_store import (
    ArtifactStore,
    LocalDirStore,
    MemoryStore,
    ObjectStat,
    ObjectNotFound,
    build_store,
    get_default_store,
    reset_default_store,
)

__all__ = [
    "ArtifactStore",
    "LocalDirStore",
    "MemoryStore",
    "ObjectStat",
    "ObjectNotFound",
    "build_store",
    "get_default_store",
    "reset_default_store",
]

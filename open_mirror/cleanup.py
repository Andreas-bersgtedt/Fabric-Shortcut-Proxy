"""Safe cleanup of Fabric Open Mirroring files ready for deletion."""
from __future__ import annotations

import datetime as dt
import time
from dataclasses import dataclass

from open_mirror.config import OpenMirrorTableTarget, OpenMirrorTarget
from open_mirror.landing_zone import (
    LandingZoneBackend,
    open_landing_zone,
    table_relative_path,
)

READY_DIR = "_FilesReadyToDelete"


@dataclass(frozen=True)
class CleanupCandidate:
    target_id: str
    table: str
    path: str
    file_count: int
    bytes: int
    oldest_modified: str | None
    eligible: bool
    retention_days: int
    reason: str
    file_paths: tuple[str, ...] = ()


def _modified(value) -> dt.datetime | None:
    if isinstance(value, dt.datetime):
        return value if value.tzinfo else value.replace(tzinfo=dt.UTC)
    if value is None:
        return None
    try:
        parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.UTC)
    except ValueError:
        return None


def inspect_cleanup(
    target: OpenMirrorTarget,
    table: OpenMirrorTableTarget,
    *,
    backend: LandingZoneBackend | None = None,
    now: dt.datetime | None = None,
) -> CleanupCandidate:
    retention = (
        table.cleanup_retention_days
        if table.cleanup_retention_days is not None
        else target.cleanup_retention_days
    )
    if retention < 0:
        raise ValueError("cleanup retention days cannot be negative")
    backend = backend or open_landing_zone(target.landing_zone_root)
    path = f"{table_relative_path(table.target_table, table.schema)}/{READY_DIR}"
    files: list[tuple[str, dict]] = []
    pending = [path]
    while pending:
        current_path = pending.pop()
        for entry in backend.list_entries(current_path):
            entry_path = f"{current_path}/{entry['name']}"
            if entry["is_directory"]:
                pending.append(entry_path)
            else:
                files.append((entry_path, entry))
    file_entries = [entry for _, entry in files]
    modified = [_modified(entry.get("last_modified")) for entry in file_entries]
    oldest = min((value for value in modified if value), default=None)
    current = now or dt.datetime.now(dt.UTC)
    cutoff = current - dt.timedelta(days=retention)
    unknown_time = any(value is None for value in modified)
    eligible = bool(files) and not unknown_time and all(value <= cutoff for value in modified if value)
    if not files:
        reason = "no_ready_files"
    elif unknown_time:
        reason = "file_timestamp_unavailable"
    elif eligible:
        reason = "retention_elapsed"
    else:
        reason = "retention_not_elapsed"
    return CleanupCandidate(
        target.id, table.target_table, path, len(files),
        sum(int(entry.get("content_length", 0) or 0) for entry in file_entries),
        oldest.isoformat() if oldest else None, eligible, retention, reason,
        tuple(file_path for file_path, _ in files),
    )


def cleanup_target(
    target: OpenMirrorTarget,
    *,
    table_name: str | None = None,
    execute: bool = False,
) -> dict:
    backend = open_landing_zone(target.landing_zone_root)
    tables = [
        table for table in target.tables
        if table.enabled and (table_name is None or table.target_table == table_name or table.name == table_name)
    ]
    candidates = [inspect_cleanup(target, table, backend=backend) for table in tables]
    deleted = []
    if execute:
        for candidate in candidates:
            if candidate.eligible:
                for file_path in candidate.file_paths:
                    backend.delete(file_path)
                backend.remove_tree(candidate.path)
                if backend.exists(candidate.path):
                    raise RuntimeError(
                        f"cleanup could not verify deletion of {candidate.path}"
                    )
                deleted.append(candidate.path)
    return {
        "target_id": target.id,
        "execute": execute,
        "deleted": deleted,
        "candidates": [candidate.__dict__ for candidate in candidates],
        "completed_at": time.time(),
    }

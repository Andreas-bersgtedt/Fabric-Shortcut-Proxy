"""
Delta Lake emitter — serve the virtual table as a native Delta table.

When ``TABLE_FORMAT=delta`` the proxy exposes a ``_delta_log/`` transaction log
plus the same content-addressed Parquet split files, instead of Iceberg
metadata + Avro manifests. Fabric's S3 shortcut then reads it as a **native
Delta table with no Iceberg->Delta conversion layer** (which is where most of
the lag and conversion bugs live).

Design
------
Each published snapshot *version* becomes one Delta commit:
  - commit 0 (``00000000000000000000.json``): ``protocol`` + ``metaData`` (schema)
    + one ``add`` action per split of version 1.
  - commit V (V>=1): ``add`` the new version's splits + ``remove`` the previous
    version's splits (each snapshot re-materializes the whole table, so a version
    fully replaces its predecessor).

Commits are derived lazily from ``iceberg.state_store`` history and memoized here
(append-only), so the log stays contiguous from commit 0 even after the state
store prunes old snapshot *data* (a Delta reader only physically reads the
net-current files, which are pinned; removed files are never fetched).

Paths in ``add``/``remove`` are RELATIVE to the table root (``data/split-*.parquet``),
matching how Fabric resolves a shortcut whose sub-path is the table folder.
"""
from __future__ import annotations

import hashlib
import json
import threading

import cache.lru_cache as cache
from iceberg.state_store import get_all_snapshots, get_snapshot_history
from observability.logging import get_logger

log = get_logger(__name__)

_lock = threading.Lock()
# table -> [commit_json_text_0, commit_json_text_1, ...]
_commits: dict[str, list[str]] = {}
# table -> last committed [(rel_path, size, records)]
_prev_files: dict[str, list[tuple[str, int, int]]] = {}
# table -> highest snapshot.version already committed
_committed_version: dict[str, int] = {}


# ---------------------------------------------------------------------------
# Schema mapping (Iceberg type string -> Delta type string)
# ---------------------------------------------------------------------------

def _delta_type(iceberg_type: str) -> str:
    t = iceberg_type.strip().lower()
    simple = {
        "boolean": "boolean", "int": "integer", "long": "long",
        "float": "float", "double": "double", "date": "date",
        "string": "string", "binary": "binary", "uuid": "string",
        "time": "string",
        # Iceberg `timestamp` has no zone -> Delta timestamp_ntz;
        # `timestamptz` (UTC) -> Delta `timestamp`.
        "timestamp": "timestamp_ntz", "timestamptz": "timestamp",
    }
    if t in simple:
        return simple[t]
    if t.startswith("decimal("):
        return t.replace(" ", "")  # Delta wants decimal(P,S) with no spaces
    if t.startswith("fixed("):
        return "binary"
    return "string"  # safe fallback


def _schema_string(columns) -> str:
    """Delta stores the schema as a JSON *string* (a Spark StructType)."""
    fields = [
        {"name": c.name, "type": _delta_type(c.iceberg_type),
         "nullable": bool(c.nullable), "metadata": {}}
        for c in columns
    ]
    return json.dumps({"type": "struct", "fields": fields}, separators=(",", ":"))


def _table_uuid(name: str) -> str:
    h = hashlib.md5(f"delta:{name}".encode(), usedforsecurity=False).hexdigest()
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


def _rel_path(object_key: str, table_path: str) -> str:
    prefix = f"{table_path}/"
    return object_key[len(prefix):] if object_key.startswith(prefix) else object_key


# ---------------------------------------------------------------------------
# Commit construction
# ---------------------------------------------------------------------------

def _add_action(path: str, size: int, records: int, ts: int) -> dict:
    return {"add": {
        "path": path,
        "partitionValues": {},
        "size": size,
        "modificationTime": ts,
        "dataChange": True,
        "stats": json.dumps({"numRecords": records}),
    }}


def _remove_action(path: str, size: int, ts: int) -> dict:
    return {"remove": {
        "path": path,
        "deletionTimestamp": ts,
        "dataChange": True,
        "extendedFileMetadata": True,
        "partitionValues": {},
        "size": size,
    }}


def _metadata_action(snap) -> dict:
    return {"metaData": {
        "id": _table_uuid(snap.table.name),
        "name": snap.table.name,
        "format": {"provider": "parquet", "options": {}},
        "schemaString": _schema_string(snap.table.schema),
        "partitionColumns": [],
        "configuration": {},
        "createdTime": snap.watermark_ms,
    }}


def _commit_text(actions: list[dict]) -> str:
    return "".join(json.dumps(a, separators=(",", ":")) + "\n" for a in actions)


def _register(snap) -> None:
    """Append the commit for one snapshot version (must be called in order)."""
    name = snap.table.name
    ver = snap.version
    if _committed_version.get(name, 0) >= ver:
        return
    files = [
        (_rel_path(s.object_key, snap.table_path), s.file_size_in_bytes or 0, s.record_count or 0)
        for s in snap.splits
    ]
    ts = snap.watermark_ms
    commits = _commits.setdefault(name, [])
    actions: list[dict] = []
    if not commits:
        actions.append({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}})
        actions.append(_metadata_action(snap))
        for p, sz, rc in files:
            actions.append(_add_action(p, sz, rc, ts))
    else:
        # Emit a DIFF against the previous version, not a full replace. Splits are
        # content-addressed, so an unchanged split keeps the same path across
        # versions — it must carry forward untouched (no add, no remove). Only
        # genuinely new files are added; only files that disappeared are removed.
        # (Adding+removing the same path in one commit would net it out of the
        # table for a replaying reader — silent data loss.)
        prev = _prev_files.get(name, [])
        cur_paths = {p for p, _, _ in files}
        prev_paths = {p for p, _, _ in prev}
        for p, sz, rc in files:
            if p not in prev_paths:
                actions.append(_add_action(p, sz, rc, ts))
        for p, sz, _rc in prev:
            if p not in cur_paths:
                actions.append(_remove_action(p, sz, ts))
        # Nothing actually changed (identical content-addressed file set): skip
        # the empty commit so the log doesn't advance for a no-op.
        if not actions:
            _committed_version[name] = ver
            return
    commits.append(_commit_text(actions))
    _prev_files[name] = files
    _committed_version[name] = ver


def _sync(name: str) -> None:
    """Register any snapshot versions in the state store not yet committed."""
    for snap in get_snapshot_history(name):
        if snap.version > _committed_version.get(name, 0):
            _register(snap)


def sync_all() -> None:
    """Bring the Delta log in sync with all tables' current snapshot history.

    Call at startup (before pruning could drop version 1) and it is also called
    lazily on every object listing.
    """
    with _lock:
        names = {snap.table.name for snap in get_all_snapshots()}
        for name in names:
            _sync(name)


def reset() -> None:
    with _lock:
        _commits.clear()
        _prev_files.clear()
        _committed_version.clear()


# ---------------------------------------------------------------------------
# Object serving (used by the S3 router when TABLE_FORMAT=delta)
# ---------------------------------------------------------------------------

def _log_key(table_path: str, version: int) -> str:
    return f"{table_path}/_delta_log/{version:020d}.json"


def delta_log_objects() -> dict[str, dict]:
    """Return the virtual-object map for every Delta table: the ``_delta_log``
    commit files plus the current (net) data-file objects."""
    with _lock:
        by_table: dict[str, list] = {}
        for snap in get_all_snapshots():
            by_table.setdefault(snap.table.name, []).append(snap)

        objects: dict[str, dict] = {}
        for name, snaps in by_table.items():
            _sync(name)
            cur = max(snaps, key=lambda s: s.version)
            for i, text in enumerate(_commits.get(name, [])):
                data = text.encode()
                objects[_log_key(cur.table_path, i)] = {
                    "size": len(data),
                    "last_modified_ms": cur.watermark_ms,
                    "data": data,
                    "content_type": "application/json",
                }
            # Advertise data files for EVERY retained version (not just current):
            # after a refresh, Fabric may still read a prior version's files until
            # it re-syncs the _delta_log. Those files stay pinned within
            # SNAPSHOT_HISTORY_LIMIT, so keep them listable/served (no 404).
            seen: set[str] = set()
            for snap in sorted(snaps, key=lambda s: s.version, reverse=True):
                for s in snap.splits:
                    if s.object_key in seen:
                        continue
                    seen.add(s.object_key)
                    cached = cache.peek_parquet(s.object_key)
                    if cached is not None:
                        size = len(cached)
                    elif s.file_size_in_bytes is not None:
                        size = s.file_size_in_bytes
                    else:
                        size = 10 * 1024 * 1024
                    objects[s.object_key] = {
                        "size": size,
                        "last_modified_ms": snap.watermark_ms,
                        "data": None,  # generated / pinned on demand
                        "content_type": "application/octet-stream",
                    }
    return objects


def get_commit_bytes(key: str) -> bytes | None:
    """Return the bytes of a ``_delta_log/NN.json`` commit, or None."""
    if "/_delta_log/" not in key or not key.endswith(".json"):
        return None
    return delta_log_objects().get(key, {}).get("data")

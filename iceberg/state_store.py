"""
State store: single source of truth for the live snapshot.

Holds:
  - The current snapshot descriptor
  - The ordered list of SplitDescriptor objects (one per virtual Parquet file)

This is an in-memory store seeded at startup. Re-calling `build_snapshot()`
advances the snapshot (new snapshot_id, new watermark).

Thread/async safety: all mutations happen only at startup or on an explicit
admin refresh; reads are unsynchronised (acceptable for POC).
"""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field

from sqlalchemy.engine import make_url

import config


@dataclass
class SplitDescriptor:
    """Describes a single virtual Parquet data file."""
    split_index: int          # 0-based index
    num_splits: int           # total number of splits (for modulo logic)
    # The virtual object key inside the bucket, e.g.
    #   warehouse/db/sales/data/split-0-<snapshot_id>.parquet
    object_key: str
    # The watermark timestamp bound this split was generated against.
    watermark_ms: int
    # The table this split belongs to (schema + source table for the planner).
    table: "config.TableDef"
    # Populated after eager materialization at startup so Iceberg manifest
    # stats (record_count, file_size_in_bytes) are accurate. Iceberg/Parquet
    # readers rely on the declared file size to locate the footer.
    record_count: int | None = None
    file_size_in_bytes: int | None = None
    # Per-column Iceberg statistics (F3), keyed by field_id. Populated only when
    # ICEBERG_MANIFEST_STATS is enabled; drives the manifest stat maps.
    stats: dict | None = None
    # Split key chosen by planner (explicit key_column or auto-selected).
    split_key_column: str | None = None
    # Phase 4/2 range planning: the half-open key range [key_lo, key_hi)
    # this split owns when a range strategy is active (integer/date/timestamp).
    # None => modulo planning (or row-number fallback for non-integer keys).
    key_lo: object | None = None
    key_hi: object | None = None


@dataclass
class SnapshotState:
    snapshot_id: int
    sequence_number: int
    watermark_ms: int
    manifest_list_key: str          # object key of the manifest list Avro
    manifest_file_key: str          # object key of the single manifest Avro
    metadata_key: str               # object key of metadata.json
    version_hint_key: str           # object key of version-hint.text
    table: "config.TableDef"        # the table definition backing this snapshot
    table_path: str                 # active object root for this snapshot
    legacy_table_path: str          # legacy alias root (for migration compatibility)
    splits: list[SplitDescriptor] = field(default_factory=list)

    # Metadata version (v1, v2, ...). Advances on each history-enabled refresh.
    version: int = 1

    # Deterministic uuid seed for manifest object keys (content-addressed builds).
    uuid: str = ""

    # Total record count across all splits, filled after materialization.
    total_records: int | None = None

    # Cached bytes – populated lazily by the metadata/manifest builders.
    metadata_bytes: bytes | None = None
    manifest_list_bytes: bytes | None = None
    manifest_file_bytes: bytes | None = None


# ---------------------------------------------------------------------------
# Registry of live snapshots, keyed by Iceberg table name (F1 — multi-table).
# ``_snapshots`` holds the CURRENT snapshot per table; ``_history`` holds every
# version (oldest -> newest) per table for time-travel (F2).
# ---------------------------------------------------------------------------
_snapshots: dict[str, SnapshotState] = {}
_history: dict[str, list[SnapshotState]] = {}


def _safe_segment(s: str | None, fallback: str) -> str:
    v = (s or "").strip()
    if not v:
        v = fallback
    # Keep path segments filesystem/S3-friendly and deterministic.
    v = re.sub(r"[^A-Za-z0-9._-]+", "_", v)
    return v or fallback


def _split_source_table(source_table: str) -> tuple[str, str]:
    if "." in source_table:
        schema, _, name = source_table.rpartition(".")
        return schema or "default", name or "table"
    return "default", source_table or "table"


def _connection_identity(table: "config.TableDef | None" = None) -> tuple[str, str]:
    """Return ``(server, database)`` for a table's source connection.

    Multi-source aware: each table's canonical object path is namespaced by its
    OWN connection's server/database, so tables from different sources never
    collide. Falls back to the default connection when no table is given.
    """
    conn_id = getattr(table, "connection_id", "default") if table is not None else "default"
    try:
        u = make_url(config.effective_db_url(conn_id))
        server = _safe_segment(u.host, "local")
        database = _safe_segment(u.database, "default")
        return server, database
    except Exception:  # noqa: BLE001 - config validation handles malformed URLs
        return "local", "default"


def legacy_table_path(table: "config.TableDef", warehouse_prefix: str) -> str:
    return f"{warehouse_prefix}/{_safe_segment(table.name, 'table')}"


def canonical_table_path(table: "config.TableDef", warehouse_prefix: str) -> str:
    server, database = _connection_identity(table)
    schema, obj = _split_source_table(table.source_table)
    return (
        f"{warehouse_prefix}/"
        f"{_safe_segment(server, 'local')}/"
        f"{_safe_segment(database, 'default')}/"
        f"{_safe_segment(schema, 'default')}/"
        f"{_safe_segment(obj, 'table')}"
    )


def active_table_path(table: "config.TableDef", warehouse_prefix: str) -> str:
    if config.OBJECT_PATH_LAYOUT == "canonical":
        return canonical_table_path(table, warehouse_prefix)
    return legacy_table_path(table, warehouse_prefix)


def alias_to_active_key(key: str) -> str:
    """Map a legacy alias key to the active key when canonical mode is enabled."""
    if not (config.OBJECT_PATH_LAYOUT == "canonical" and config.ENABLE_LEGACY_PATH_ALIASES):
        return key

    for snap in get_all_snapshots():
        lp = snap.legacy_table_path
        if key == lp or key.startswith(lp + "/"):
            return snap.table_path + key[len(lp):]
    return key


def active_to_legacy_key(snap: "SnapshotState", key: str) -> str:
    if key == snap.table_path or key.startswith(snap.table_path + "/"):
        return snap.legacy_table_path + key[len(snap.table_path):]
    return key


def build_table_snapshot(table: "config.TableDef", bucket: str, warehouse_prefix: str) -> SnapshotState:
    """Build and register a deterministic snapshot for a single table.

    Identifiers are derived from a stable seed so every virtual object key is
    constant across restarts (Fabric's async XTable conversion caches paths).
    """
    table_path = active_table_path(table, warehouse_prefix)
    legacy_path = legacy_table_path(table, warehouse_prefix)

    seed = (
        f"{bucket}/{table_path}"
        f"|source={table.source_table}"
        f"|key={table.key_column or ''}"
        f"|splits={table.num_splits}"
        f"|strategy={table.effective_split_strategy}"
        f"|target={table.effective_split_target_rows}"
    )
    digest = hashlib.sha256(seed.encode()).hexdigest()
    snap_id = int(digest[:15], 16)                 # stable positive long (< 2**60)
    snap_uuid = digest[:32]
    # Immutable snapshot => stable timestamp; anchored to a fixed epoch base.
    now_ms = 1_700_000_000_000 + (int(digest[15:25], 16) % 86_400_000)

    snap = SnapshotState(
        snapshot_id=snap_id,
        sequence_number=1,
        watermark_ms=now_ms,
        manifest_list_key=f"{table_path}/metadata/snap-{snap_id}-1-{snap_uuid}.avro",
        manifest_file_key=f"{table_path}/metadata/{snap_uuid}-m0.avro",
        metadata_key=f"{table_path}/metadata/v1.metadata.json",
        version_hint_key=f"{table_path}/metadata/version-hint.text",
        table=table,
        table_path=table_path,
        legacy_table_path=legacy_path,
    )
    snap.splits = [
        SplitDescriptor(
            split_index=i,
            num_splits=table.num_splits,
            object_key=f"{table_path}/data/split-{i}-{snap_id}.parquet",
            watermark_ms=now_ms,
            table=table,
        )
        for i in range(table.num_splits)
    ]
    _snapshots[table.name] = snap
    _history[table.name] = [snap]
    return snap


def advance_table_snapshot(table_name: str, bucket: str, warehouse_prefix: str) -> SnapshotState:
    """Append a new snapshot version for a table (F2 — time-travel).

    The new version reuses the same data-split files (deterministic keys) but
    gets its own snapshot id, sequence number, watermark, versioned manifests
    and ``v{N}.metadata.json``. Prior versions remain addressable so readers can
    time-travel; ``build_metadata_json`` renders each version point-in-time.
    """
    cur = _snapshots[table_name]
    table = cur.table
    table_path = cur.table_path
    version = cur.version + 1

    seed = f"{bucket}/{table_path}/v{version}"
    digest = hashlib.sha256(seed.encode()).hexdigest()
    snap_id = int(digest[:15], 16)
    snap_uuid = digest[:32]
    seq = cur.sequence_number + 1
    watermark = cur.watermark_ms + 1000 * version  # strictly increasing

    new = SnapshotState(
        snapshot_id=snap_id,
        sequence_number=seq,
        watermark_ms=watermark,
        manifest_list_key=f"{table_path}/metadata/snap-{snap_id}-{seq}-{snap_uuid}.avro",
        manifest_file_key=f"{table_path}/metadata/{snap_uuid}-m{version}.avro",
        metadata_key=f"{table_path}/metadata/v{version}.metadata.json",
        version_hint_key=cur.version_hint_key,
        table=table,
        table_path=table_path,
        legacy_table_path=cur.legacy_table_path,
    )
    new.version = version
    new.splits = cur.splits            # share the same materialized data files
    new.total_records = cur.total_records
    _history.setdefault(table.name, [cur]).append(new)
    _snapshots[table.name] = new
    return new


def build_all_snapshots(tables, bucket: str, warehouse_prefix: str) -> list[SnapshotState]:
    """Rebuild the registry for every table in ``tables``."""
    _snapshots.clear()
    _history.clear()
    return [build_table_snapshot(t, bucket, warehouse_prefix) for t in tables]


def build_snapshot(table_name: str, num_splits: int, bucket: str, warehouse_prefix: str) -> SnapshotState:
    """Backward-compatible single-table builder.

    Synthesizes a :class:`config.TableDef` from the global config (schema /
    source table) and resets the registry to just this table. Kept so existing
    callers and tests keep working unchanged.
    """
    table = config.TableDef(
        name=table_name,
        source_table=config.DB_SOURCE_TABLE,
        schema=config.TABLE_SCHEMA,
        num_splits=num_splits,
    )
    _snapshots.clear()
    _history.clear()
    return build_table_snapshot(table, bucket, warehouse_prefix)


def get_snapshot(table_name: str | None = None) -> SnapshotState:
    if table_name is not None:
        try:
            return _snapshots[table_name]
        except KeyError:
            raise RuntimeError(f"No snapshot registered for table {table_name!r}.")
    if not _snapshots:
        raise RuntimeError("Snapshot not yet initialised. Call build_snapshot() first.")
    if len(_snapshots) == 1:
        return next(iter(_snapshots.values()))
    raise RuntimeError("Multiple tables registered; specify a table name.")


def get_all_snapshots() -> list[SnapshotState]:
    """Return every snapshot VERSION across all tables (current + history).

    With history disabled there is exactly one version per table, so this is the
    set of current snapshots. With history enabled it also includes prior
    versions so their metadata/manifests remain listable and servable.
    """
    if _history:
        out: list[SnapshotState] = []
        for versions in _history.values():
            out.extend(versions)
        return out
    return list(_snapshots.values())


def get_snapshot_history(table_name: str) -> list[SnapshotState]:
    """Return all snapshot versions for a table, oldest first."""
    return _history.get(table_name, [])


def register_snapshot(snap: SnapshotState) -> list[SnapshotState]:
    """Install a fully-built snapshot as the current version for its table.

    Appends to history and returns any versions pruned beyond
    ``SNAPSHOT_HISTORY_LIMIT`` (so the caller can evict their data-file caches).
    Used by the freshness poller (content-addressed publish).
    """
    name = snap.table.name
    hist = _history.setdefault(name, [])
    hist.append(snap)
    _snapshots[name] = snap

    pruned: list[SnapshotState] = []
    limit = max(1, config.SNAPSHOT_HISTORY_LIMIT)
    while len(hist) > limit:
        pruned.append(hist.pop(0))
    return pruned


def unregister_snapshot(table_name: str) -> None:
    """Remove a table's snapshot(s) from the served registry (used to quarantine a
    table whose materialization failed after its snapshot was built)."""
    _snapshots.pop(table_name, None)
    _history.pop(table_name, None)


def get_split_by_key(object_key: str) -> SplitDescriptor | None:
    """Return the SplitDescriptor for a data file object key across all tables.

    Searches the current snapshot per table first, then the retained history.
    History matters when AUTO_REFRESH publishes a new version: the *previous*
    version's data files stay pinned/retained (within SNAPSHOT_HISTORY_LIMIT)
    and Fabric may still request them until it re-syncs the metadata/_delta_log.
    Resolving them here (instead of 404) prevents "underlying location does not
    exist" errors on the Fabric SQL endpoint while it catches up.
    """
    object_key = alias_to_active_key(object_key)

    for snap in _snapshots.values():
        for split in snap.splits:
            if split.object_key == object_key:
                return split
    for snaps in _history.values():
        for snap in snaps:
            for split in snap.splits:
                if split.object_key == object_key:
                    return split
    return None

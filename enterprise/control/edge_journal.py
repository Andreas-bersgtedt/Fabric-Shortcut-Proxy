"""Durable journal for edge-produced snapshot manifests."""

from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re
import sqlite3

from enterprise.control.contract import SnapshotManifest
from fabric_shortcut_proxy.runtime.artifact_store import _normalize_key


class EdgeJournalConflict(ValueError):
    """A journal operation conflicts with a recorded generation or replay order."""


@dataclass(frozen=True)
class EdgeMetadata:
    key: str
    size_bytes: int
    sha256: str

    def __post_init__(self) -> None:
        if _normalize_key(self.key) != self.key:
            raise ValueError("metadata key must be canonical")
        if isinstance(self.size_bytes, bool) or not isinstance(self.size_bytes, int) or self.size_bytes < 0:
            raise ValueError("metadata size must be a non-negative integer")
        if not re.fullmatch(r"[0-9a-f]{64}", self.sha256):
            raise ValueError("metadata digest must be a lowercase SHA-256")


@dataclass(frozen=True)
class EdgeJournalEntry:
    sequence: int
    manifest: SnapshotManifest
    manifest_sha256: str
    metadata: tuple[EdgeMetadata, ...]


def encode_edge_record(
    manifest: SnapshotManifest, metadata: tuple[EdgeMetadata, ...]
) -> str:
    if not manifest.dataset_id or not manifest.table or not manifest.generation_id:
        raise ValueError("manifest requires a dataset, table and generation ID")
    if (
        isinstance(manifest.generation_fence, bool)
        or not isinstance(manifest.generation_fence, int)
        or manifest.generation_fence < 0
        or isinstance(manifest.produced_at_ms, bool)
        or not isinstance(manifest.produced_at_ms, int)
        or manifest.produced_at_ms < 1
    ):
        raise ValueError("manifest requires a non-negative fence and production time")
    metadata = tuple(sorted(metadata, key=lambda item: item.key))
    split_keys = [split.object_key for split in manifest.splits]
    for split in manifest.splits:
        EdgeMetadata(split.object_key, split.size_bytes, split.content_hash)
        if (
            isinstance(split.record_count, bool)
            or not isinstance(split.record_count, int)
            or split.record_count < 0
        ):
            raise ValueError("split record count must be a non-negative integer")
    if (
        len({item.key for item in metadata}) != len(metadata)
        or len(set(manifest.metadata_keys)) != len(manifest.metadata_keys)
        or {item.key for item in metadata} != set(manifest.metadata_keys)
        or len(set(split_keys)) != len(split_keys)
        or set(split_keys).intersection(manifest.metadata_keys)
    ):
        raise ValueError("manifest object keys and metadata digests do not match")
    return json.dumps({
        "version": 1,
        "manifest": manifest.to_dict(),
        "metadata": [
            {"key": item.key, "size_bytes": item.size_bytes, "sha256": item.sha256}
            for item in metadata
        ],
    }, sort_keys=True, separators=(",", ":"), allow_nan=False)


class EdgeJournal:
    """Keep immutable generation manifests until the regional Manager acknowledges them."""

    def __init__(self, path: str | Path, *, dataset_id: str) -> None:
        if not dataset_id or not dataset_id.strip():
            raise ValueError("dataset_id must not be empty")
        self.path = Path(path)
        self.dataset_id = dataset_id
        with closing(self._connect()) as db, db:
            mode = db.execute("PRAGMA journal_mode=WAL").fetchone()
            if mode is None or str(mode[0]).lower() != "wal":
                raise RuntimeError("edge journal requires SQLite WAL mode")
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "CREATE TABLE IF NOT EXISTS metadata "
                "(key TEXT PRIMARY KEY, value TEXT NOT NULL)"
            )
            db.execute(
                "CREATE TABLE IF NOT EXISTS generations ("
                "sequence INTEGER PRIMARY KEY AUTOINCREMENT, "
                "generation_id TEXT NOT NULL UNIQUE, "
                "manifest TEXT NOT NULL, "
                "manifest_sha256 TEXT NOT NULL, "
                "acknowledged INTEGER NOT NULL DEFAULT 0 "
                "CHECK (acknowledged IN (0, 1)))"
            )
            existing = db.execute(
                "SELECT value FROM metadata WHERE key = 'dataset_id'"
            ).fetchone()
            if existing is None:
                db.execute(
                    "INSERT INTO metadata (key, value) VALUES ('dataset_id', ?)",
                    (dataset_id,),
                )
            elif existing[0] != dataset_id:
                raise EdgeJournalConflict("journal belongs to a different dataset")

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        try:
            db.execute("PRAGMA synchronous=FULL")
        except sqlite3.Error:
            db.close()
            raise
        return db

    def append(
        self, manifest: SnapshotManifest, *, metadata: tuple[EdgeMetadata, ...] = ()
    ) -> int:
        if manifest.dataset_id != self.dataset_id:
            raise EdgeJournalConflict("manifest belongs to a different dataset")
        encoded = encode_edge_record(manifest, metadata)
        digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
        with closing(self._connect()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT sequence, manifest, manifest_sha256 FROM generations "
                "WHERE generation_id = ?",
                (manifest.generation_id,),
            ).fetchone()
            if row is not None:
                if row[1] != encoded or row[2] != digest:
                    raise EdgeJournalConflict("generation ID has different manifest content")
                return int(row[0])
            cursor = db.execute(
                "INSERT INTO generations (generation_id, manifest, manifest_sha256) "
                "VALUES (?, ?, ?)",
                (manifest.generation_id, encoded, digest),
            )
            if cursor.lastrowid is None:
                raise RuntimeError("journal did not assign a generation sequence")
            return int(cursor.lastrowid)

    def _entry(
        self, sequence: int, generation_id: str, encoded: str, digest: str
    ) -> EdgeJournalEntry:
        if hashlib.sha256(encoded.encode("utf-8")).hexdigest() != digest:
            raise EdgeJournalConflict("journal manifest failed its integrity check")
        envelope = json.loads(encoded)
        if (
            not isinstance(envelope, dict)
            or envelope.get("version") != 1
            or not isinstance(envelope.get("manifest"), dict)
            or not isinstance(envelope.get("metadata"), list)
        ):
            raise EdgeJournalConflict("unsupported journal manifest envelope")
        manifest = SnapshotManifest.from_dict(envelope["manifest"])
        metadata = tuple(EdgeMetadata(**item) for item in envelope["metadata"])
        if encode_edge_record(manifest, metadata) != encoded:
            raise EdgeJournalConflict("journal record is not canonical")
        if manifest.dataset_id != self.dataset_id or manifest.generation_id != generation_id:
            raise EdgeJournalConflict("journal manifest identity does not match its record")
        return EdgeJournalEntry(
            sequence=sequence, manifest=manifest, manifest_sha256=digest,
            metadata=metadata,
        )

    def next_pending(self) -> EdgeJournalEntry | None:
        with closing(self._connect()) as db:
            row = db.execute(
                "SELECT sequence, generation_id, manifest, manifest_sha256 "
                "FROM generations WHERE acknowledged = 0 ORDER BY sequence LIMIT 1"
            ).fetchone()
        if row is None:
            return None
        return self._entry(int(row[0]), str(row[1]), str(row[2]), str(row[3]))

    def pending_count(self) -> int:
        with closing(self._connect()) as db:
            row = db.execute(
                "SELECT COUNT(*) FROM generations WHERE acknowledged = 0"
            ).fetchone()
        if row is None:
            raise RuntimeError("journal did not return its pending count")
        return int(row[0])

    def acknowledge(self, sequence: int, generation_id: str, manifest_sha256: str) -> None:
        with closing(self._connect()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT generation_id, acknowledged, manifest, manifest_sha256 "
                "FROM generations WHERE sequence = ?",
                (sequence,),
            ).fetchone()
            if row is None or row[0] != generation_id or row[3] != manifest_sha256:
                raise EdgeJournalConflict("acknowledgement does not identify a journal entry")
            self._entry(sequence, str(row[0]), str(row[2]), str(row[3]))
            if not row[1]:
                first = db.execute(
                    "SELECT sequence FROM generations WHERE acknowledged = 0 "
                    "ORDER BY sequence LIMIT 1"
                ).fetchone()
                if first is None or int(first[0]) != sequence:
                    raise EdgeJournalConflict("generations must be acknowledged in order")
                db.execute(
                    "UPDATE generations SET acknowledged = 1 WHERE sequence = ?",
                    (sequence,),
                )

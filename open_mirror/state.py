"""Restart-safe local state for Open Mirroring change tracking."""
from __future__ import annotations

import datetime as _dt
import hashlib
import json
import os
from dataclasses import dataclass, field
from typing import Literal

from open_mirror.config import OpenMirrorTableTarget, OpenMirrorTarget

DEFAULT_STATE_DIR = "./.open_mirror_state"
STATE_VERSION = 2


def encode_watermark(value):
    """Serialize a cursor value with a type tag so it survives JSON and restart."""
    if value is None:
        return None
    if isinstance(value, bool):
        return {"t": "int", "v": int(value)}
    if isinstance(value, int):
        return {"t": "int", "v": value}
    if isinstance(value, float):
        return {"t": "float", "v": value}
    if isinstance(value, _dt.datetime):
        return {"t": "datetime", "v": value.isoformat()}
    if isinstance(value, _dt.date):
        return {"t": "date", "v": value.isoformat()}
    if isinstance(value, (bytes, bytearray)):
        return {"t": "bytes", "v": bytes(value).hex()}
    return {"t": "str", "v": str(value)}


def decode_watermark(stored):
    """Rebuild a cursor value for SQL binding from its tagged representation."""
    if not isinstance(stored, dict) or set(stored) < {"t", "v"}:
        return None
    t, v = stored.get("t"), stored.get("v")
    if v is None:
        return None
    try:
        if t == "int":
            return int(v)
        if t == "float":
            return float(v)
        if t == "datetime":
            return _dt.datetime.fromisoformat(str(v))
        if t == "date":
            return _dt.date.fromisoformat(str(v))
        if t == "bytes":
            return bytes.fromhex(str(v))
        if t == "str":
            return str(v)
    except (ValueError, TypeError):
        return None
    return None


def _valid_encoded(value) -> bool:
    return value is None or (
        isinstance(value, dict)
        and value.get("t") in {"int", "float", "datetime", "date", "bytes", "str"}
        and "v" in value
        and decode_watermark(value) is not None
    )


@dataclass
class CommittedCursor:
    watermark: dict | None = None
    keys: list[dict] = field(default_factory=list)
    file: str | None = None
    committed_at: str | None = None

    def to_json(self) -> dict:
        return {
            "watermark": self.watermark,
            "keys": self.keys,
            "file": self.file,
            "committed_at": self.committed_at,
        }

    @classmethod
    def from_json(cls, data: dict | None) -> CommittedCursor | None:
        if data is None:
            return None
        if not isinstance(data, dict):
            raise TypeError("committed cursor must be an object or null")
        watermark = data.get("watermark")
        keys = data.get("keys", [])
        if not _valid_encoded(watermark) or not isinstance(keys, list) or not all(
            _valid_encoded(v) and v is not None for v in keys
        ):
            raise ValueError("committed cursor contains invalid typed values")
        return cls(
            watermark=watermark,
            keys=keys,
            file=str(data["file"]) if data.get("file") else None,
            committed_at=str(data["committed_at"]) if data.get("committed_at") else None,
        )


@dataclass
class PendingBatch:
    prior: CommittedCursor | None
    next: CommittedCursor
    path: str
    row_count: int
    content_hash: str
    initial: bool = False
    snapshot_keys: dict[str, dict] | None = None

    def to_json(self) -> dict:
        return {
            "prior": self.prior.to_json() if self.prior else None,
            "next": self.next.to_json(),
            "path": self.path,
            "row_count": self.row_count,
            "content_hash": self.content_hash,
            "initial": self.initial,
            "snapshot_keys": self.snapshot_keys,
        }

    @classmethod
    def from_json(cls, data: dict | None) -> PendingBatch | None:
        if data is None:
            return None
        if not isinstance(data, dict):
            raise TypeError("pending batch must be an object or null")
        path = data.get("path")
        content_hash = data.get("content_hash")
        row_count = data.get("row_count")
        next_cursor = CommittedCursor.from_json(data.get("next"))
        snapshot_keys = data.get("snapshot_keys")
        if (
            not isinstance(path, str)
            or not path
            or not isinstance(content_hash, str)
            or len(content_hash) != 64
            or not isinstance(row_count, int)
            or row_count < 0
            or next_cursor is None
            or (snapshot_keys is not None and not isinstance(snapshot_keys, dict))
        ):
            raise ValueError("pending batch metadata is invalid")
        return cls(
            prior=CommittedCursor.from_json(data.get("prior")),
            next=next_cursor,
            path=path,
            row_count=row_count,
            content_hash=content_hash,
            initial=bool(data.get("initial", False)),
            snapshot_keys=snapshot_keys,
        )


@dataclass
class PublishState:
    """Version 2 table state; ``keys`` remains the snapshot-diff row map."""

    strategy: str = "snapshot"
    projection_fingerprint: str | None = None
    initialized: bool = False
    committed: CommittedCursor | None = None
    pending: PendingBatch | None = None
    keys: dict[str, dict] = field(default_factory=dict)
    watermark: dict | None = None
    published_rows_total: int = 0
    last_batch_rows: int = 0
    last_published_at: str | None = None

    def __post_init__(self) -> None:
        # ``watermark=`` was the public version 1 constructor API.
        if self.committed is None and self.watermark is not None:
            self.committed = CommittedCursor(watermark=self.watermark)
        elif self.committed is not None:
            self.watermark = self.committed.watermark

    def to_json(self) -> dict:
        return {
            "version": STATE_VERSION,
            "strategy": self.strategy,
            "projection_fingerprint": self.projection_fingerprint,
            "initialized": self.initialized,
            "committed": self.committed.to_json() if self.committed else None,
            "pending": self.pending.to_json() if self.pending else None,
            "keys": self.keys,
            "published_rows_total": self.published_rows_total,
            "last_batch_rows": self.last_batch_rows,
            "last_published_at": self.last_published_at,
        }

    @classmethod
    def from_json(cls, data: dict) -> PublishState:
        if not isinstance(data, dict):
            raise TypeError("state must be a JSON object")
        version = data.get("version", 1)
        if version == 1:
            keys = data.get("keys", {})
            watermark = data.get("watermark")
            if not isinstance(keys, dict) or not _valid_encoded(watermark):
                raise ValueError("version 1 state is invalid")
            strategy = "watermark" if watermark is not None else "snapshot"
            return cls(
                strategy=strategy,
                initialized=True,
                committed=CommittedCursor(watermark=watermark) if watermark is not None else None,
                keys=keys,
            )
        if version != STATE_VERSION:
            raise ValueError(f"unsupported state version {version!r}")
        strategy = data.get("strategy")
        if strategy not in {"watermark", "snapshot", "initial"}:
            raise ValueError(f"unsupported state strategy {strategy!r}")
        keys = data.get("keys", {})
        projection_fingerprint = data.get("projection_fingerprint")
        if projection_fingerprint is not None and (
            not isinstance(projection_fingerprint, str)
            or len(projection_fingerprint) != 64
        ):
            raise ValueError("projection fingerprint is invalid")
        if not isinstance(data.get("initialized"), bool) or not isinstance(keys, dict):
            raise TypeError("state initialized/keys fields are invalid")
        published_rows_total = data.get("published_rows_total", 0)
        last_batch_rows = data.get("last_batch_rows", 0)
        last_published_at = data.get("last_published_at")
        if (
            not isinstance(published_rows_total, int) or published_rows_total < 0
            or not isinstance(last_batch_rows, int) or last_batch_rows < 0
            or (last_published_at is not None and not isinstance(last_published_at, str))
        ):
            raise ValueError("published row metrics are invalid")
        return cls(
            strategy=strategy,
            projection_fingerprint=projection_fingerprint,
            initialized=data["initialized"],
            committed=CommittedCursor.from_json(data.get("committed")),
            pending=PendingBatch.from_json(data.get("pending")),
            keys=keys,
            published_rows_total=published_rows_total,
            last_batch_rows=last_batch_rows,
            last_published_at=last_published_at,
        )


StateStatus = Literal["missing", "valid", "corrupt", "unreadable", "incompatible"]


@dataclass(frozen=True)
class StateLoadResult:
    status: StateStatus
    path: str
    state: PublishState | None = None
    error: str | None = None

    def __getattr__(self, name):
        # Compatibility for callers that previously received PublishState directly.
        if self.state is not None:
            return getattr(self.state, name)
        raise AttributeError(name)


def _canon(value) -> str:
    if value is None:
        return "\x00"
    return str(value)


def row_hash(row: dict, columns) -> str:
    parts = [_canon(row.get(col.name)) for col in columns]
    return hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()


def projection_fingerprint(columns) -> str:
    """Identify a published projection without including token key material."""
    policy = []
    for column in columns:
        transform = column.transform
        policy.append({
            "field_id": column.field_id,
            "name": column.name,
            "source": column.source_name,
            "type": column.iceberg_type,
            "nullable": column.nullable,
            "transform": ({
                "kind": transform.kind,
                "key_ref": transform.key_ref,
                "domain": transform.domain,
                "normalization": transform.normalization,
            } if transform else None),
        })
    encoded = json.dumps(policy, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def key_string(row: dict, key_columns: list[str]) -> str:
    return "|".join(_canon(row.get(k)) for k in key_columns)


def _slug(text: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in (text or "")).strip("_") or "x"


def state_file_path(state_dir: str, target: OpenMirrorTarget, table: OpenMirrorTableTarget) -> str:
    name = f"{_slug(target.id)}__{_slug(table.schema or '')}__{_slug(table.target_table)}.json"
    return os.path.abspath(os.path.join(state_dir or DEFAULT_STATE_DIR, name))


def load_state(
    state_dir: str, target: OpenMirrorTarget, table: OpenMirrorTableTarget
) -> StateLoadResult:
    """Load state with a typed outcome; only ``missing`` permits an implicit initial load."""
    path = state_file_path(state_dir, target, table)
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except FileNotFoundError:
        return StateLoadResult("missing", path)
    except json.JSONDecodeError as exc:
        return StateLoadResult("corrupt", path, error=str(exc))
    except OSError as exc:
        return StateLoadResult("unreadable", path, error=str(exc))
    try:
        return StateLoadResult("valid", path, PublishState.from_json(data))
    except (TypeError, ValueError) as exc:
        status: StateStatus = "incompatible" if "unsupported" in str(exc) else "corrupt"
        return StateLoadResult(status, path, error=str(exc))


def save_state(
    state_dir: str, target: OpenMirrorTarget, table: OpenMirrorTableTarget, state: PublishState
) -> str:
    path = state_file_path(state_dir, target, table)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(state.to_json(), fh, default=str)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)
    return path


def delete_state(state_dir: str, target: OpenMirrorTarget, table: OpenMirrorTableTarget) -> None:
    path = state_file_path(state_dir, target, table)
    try:
        os.remove(path)
    except FileNotFoundError:
        pass


def build_state_from_rows(rows, columns, key_columns: list[str]) -> PublishState:
    keys: dict[str, dict] = {}
    for row in rows:
        ks = key_string(row, key_columns)
        keys[ks] = {"h": row_hash(row, columns), "k": [row.get(k) for k in key_columns]}
    return PublishState(strategy="snapshot", initialized=True, keys=keys)


def target_manifest_path(state_dir: str, target: OpenMirrorTarget) -> str:
    return os.path.abspath(
        os.path.join(state_dir or DEFAULT_STATE_DIR, f"{_slug(target.id)}__tables.json")
    )


def load_published_tables(state_dir: str, target: OpenMirrorTarget) -> list[dict]:
    path = target_manifest_path(state_dir, target)
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except FileNotFoundError:
        return []
    except (OSError, json.JSONDecodeError):
        return []
    tables = data.get("tables") if isinstance(data, dict) else None
    return [t for t in tables if isinstance(t, dict)] if isinstance(tables, list) else []


def save_published_tables(state_dir: str, target: OpenMirrorTarget, tables: list[dict]) -> None:
    path = target_manifest_path(state_dir, target)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump({"tables": tables}, fh, default=str)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)

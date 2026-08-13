"""Local publish state for Open Mirroring incremental change detection.

Fabric Open Mirroring needs ``__rowMarker__`` insert/update/delete rows for
incremental changes, but most sources expose no change feed. This module keeps a
small local snapshot of what was last published per ``(target, table)`` — a map
of key-string -> (row hash, key values) — so the next publish diffs the current
source rows against it to derive the change set.

State lives OUTSIDE the landing zone (Fabric only reads the landing zone), in a
local directory (default ``./.open_mirror_state``), one JSON file per table.
"""
from __future__ import annotations

import datetime as _dt
import hashlib
import json
import os
from dataclasses import dataclass, field

from open_mirror.config import OpenMirrorTableTarget, OpenMirrorTarget

DEFAULT_STATE_DIR = "./.open_mirror_state"


def encode_watermark(value):
    """Serialize a watermark value with a type tag so it survives JSON + restart."""
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
    """Rebuild a watermark value (for SQL binding) from its stored tagged form."""
    if not isinstance(stored, dict):
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
    except (ValueError, TypeError):
        return None
    return str(v)


def _canon(value) -> str:
    """Stable string for one cell (NULL is distinct from the empty string)."""
    if value is None:
        return "\x00"
    return str(value)


def row_hash(row: dict, columns) -> str:
    """Content hash of a full row across ``columns`` (order-stable)."""
    parts = [_canon(row.get(col.name)) for col in columns]
    return hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()


def key_string(row: dict, key_columns: list[str]) -> str:
    """Stable identity string for a row's key column(s)."""
    return "|".join(_canon(row.get(k)) for k in key_columns)


@dataclass
class PublishState:
    """Last-published snapshot: key-string -> {"h": row_hash, "k": [key values]}.

    In watermark mode ``keys`` is empty and ``watermark`` holds the tagged
    last-seen value of the source's monotonic column instead.
    """

    keys: dict[str, dict] = field(default_factory=dict)
    watermark: dict | None = None

    def to_json(self) -> dict:
        out = {"version": 1, "keys": self.keys}
        if self.watermark is not None:
            out["watermark"] = self.watermark
        return out

    @classmethod
    def from_json(cls, data: dict) -> "PublishState":
        keys = data.get("keys") if isinstance(data, dict) else None
        wm = data.get("watermark") if isinstance(data, dict) else None
        return cls(keys=keys if isinstance(keys, dict) else {},
                   watermark=wm if isinstance(wm, dict) else None)


def _slug(text: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in (text or "")).strip("_") or "x"


def state_file_path(state_dir: str, target: OpenMirrorTarget, table: OpenMirrorTableTarget) -> str:
    """Local path of the state file for one ``(target, table)``."""
    name = f"{_slug(target.id)}__{_slug(table.schema or '')}__{_slug(table.target_table)}.json"
    return os.path.join(state_dir or DEFAULT_STATE_DIR, name)


def load_state(state_dir: str, target: OpenMirrorTarget, table: OpenMirrorTableTarget) -> PublishState | None:
    """Load prior publish state, or ``None`` when the table has never been published."""
    path = state_file_path(state_dir, target, table)
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return PublishState.from_json(json.load(fh))
    except FileNotFoundError:
        return None
    except (OSError, json.JSONDecodeError):
        return None


def save_state(state_dir: str, target: OpenMirrorTarget, table: OpenMirrorTableTarget, state: PublishState) -> str:
    """Persist publish state atomically; returns the file path written."""
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
    """Remove a table's state file (used when a table is dropped/reset)."""
    path = state_file_path(state_dir, target, table)
    try:
        os.remove(path)
    except FileNotFoundError:
        pass


def build_state_from_rows(rows, columns, key_columns: list[str]) -> PublishState:
    """Snapshot state from a full set of current source rows."""
    keys: dict[str, dict] = {}
    for row in rows:
        ks = key_string(row, key_columns)
        keys[ks] = {"h": row_hash(row, columns), "k": [row.get(k) for k in key_columns]}
    return PublishState(keys=keys)


# ---------------------------------------------------------------------------
# Per-target published-table manifest (for drop reconciliation).
# ---------------------------------------------------------------------------

def target_manifest_path(state_dir: str, target: OpenMirrorTarget) -> str:
    """Local path of the per-target manifest listing its published tables."""
    return os.path.join(state_dir or DEFAULT_STATE_DIR, f"{_slug(target.id)}__tables.json")


def load_published_tables(state_dir: str, target: OpenMirrorTarget) -> list[dict]:
    """Return the ``[{"schema", "target_table"}]`` last published for this target."""
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
    """Persist the set of tables currently published for this target (atomic)."""
    path = target_manifest_path(state_dir, target)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump({"tables": tables}, fh, default=str)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)

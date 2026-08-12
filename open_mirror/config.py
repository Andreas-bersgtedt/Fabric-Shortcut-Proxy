"""Open Mirroring target configuration model + loader.

Targets are read from a dedicated ``config.open_mirror.json`` document (gitignored,
like the other per-deployment config files) shaped as::

    {
      "open_mirror": {
        "open_mirror_targets": [
          {
            "id": "fabric-sales",
            "connection": "default",
            "landing_zone_root": "https://onelake.dfs.fabric.microsoft.com/<ws>/<db>/Files/LandingZone",
            "workspace_id": "<ws>",
            "mirrored_database_id": "<db>",
            "partner_name": "FabricShortcutProxy",
            "source_type": "SQL",
            "enabled": true,
            "tables": [
              {"name": "sales", "source_table": "dbo.sales", "target_table": "sales",
               "key_column": "id", "schema": "dbo", "mode": "incremental"}
            ]
          }
        ]
      }
    }

A target binds to an EXISTING source connection id (``connection``) so the source
database engine and connector selection remain exactly as configured for the
proxy's read path — the target only describes the Fabric landing-zone sink.
"""
from __future__ import annotations

import json
import sys
from dataclasses import dataclass

_CONFIG_FILE = "config.open_mirror.json"

DEFAULT_PARTNER_NAME = "FabricShortcutProxy"


@dataclass(frozen=True)
class OpenMirrorTableTarget:
    """One source table mapped to a mirrored table in the landing zone."""

    name: str
    source_table: str
    target_table: str
    key_column: str
    schema: str | None = None
    mode: str = "incremental"
    watermark_column: str | None = None
    enabled: bool = True

    @property
    def key_columns(self) -> list[str]:
        """Key column(s) as a list (``key_column`` may be a comma-separated compound key)."""
        return [c.strip() for c in self.key_column.split(",") if c.strip()]


@dataclass(frozen=True)
class OpenMirrorTarget:
    """A Fabric mirrored-database landing-zone target bound to a source connection."""

    id: str
    connection_id: str
    landing_zone_root: str
    workspace_id: str | None = None
    mirrored_database_id: str | None = None
    partner_name: str = DEFAULT_PARTNER_NAME
    source_type: str | None = None
    source_version: str | None = None
    enabled: bool = True
    tables: tuple[OpenMirrorTableTarget, ...] = ()


def _table_from_dict(d: dict) -> OpenMirrorTableTarget:
    name = str(d.get("name") or "").strip()
    source_table = str(d.get("source_table") or "").strip()
    if not name:
        raise ValueError("open mirror table: 'name' must be non-empty")
    if not source_table:
        raise ValueError(f"open mirror table {name!r}: 'source_table' must be non-empty")
    key_column = str(d.get("key_column") or "").strip()
    if not key_column:
        raise ValueError(f"open mirror table {name!r}: 'key_column' must be non-empty")
    target_table = str(d.get("target_table") or name).strip()
    schema = str(d["schema"]).strip() if d.get("schema") else None
    mode = str(d.get("mode") or "incremental").strip().lower()
    watermark_column = str(d["watermark_column"]).strip() if d.get("watermark_column") else None
    return OpenMirrorTableTarget(
        name=name,
        source_table=source_table,
        target_table=target_table,
        key_column=key_column,
        schema=schema,
        mode=mode,
        watermark_column=watermark_column,
        enabled=bool(d.get("enabled", True)),
    )


def target_from_dict(d: dict) -> OpenMirrorTarget:
    """Build an :class:`OpenMirrorTarget` from a JSON target entry."""
    tid = str(d.get("id") or "").strip()
    if not tid:
        raise ValueError("open mirror target: 'id' must be non-empty")
    landing_zone_root = str(d.get("landing_zone_root") or "").strip()
    if not landing_zone_root:
        raise ValueError(f"open mirror target {tid!r}: 'landing_zone_root' must be non-empty")
    connection_id = str(d.get("connection") or d.get("connection_id") or "default").strip() or "default"
    raw_tables = d.get("tables") or []
    if not isinstance(raw_tables, list):
        raise ValueError(f"open mirror target {tid!r}: 'tables' must be a list")
    tables = tuple(_table_from_dict(t) for t in raw_tables if isinstance(t, dict))
    return OpenMirrorTarget(
        id=tid,
        connection_id=connection_id,
        landing_zone_root=landing_zone_root,
        workspace_id=(str(d["workspace_id"]).strip() if d.get("workspace_id") else None),
        mirrored_database_id=(str(d["mirrored_database_id"]).strip() if d.get("mirrored_database_id") else None),
        partner_name=str(d.get("partner_name") or DEFAULT_PARTNER_NAME).strip() or DEFAULT_PARTNER_NAME,
        source_type=(str(d["source_type"]).strip() if d.get("source_type") else None),
        source_version=(str(d["source_version"]).strip() if d.get("source_version") else None),
        enabled=bool(d.get("enabled", True)),
        tables=tables,
    )


def _load_raw(path: str = _CONFIG_FILE) -> dict:
    """Load the raw ``config.open_mirror.json`` document (empty when absent)."""
    try:
        with open(path, "r", encoding="utf-8-sig") as fh:
            data = json.load(fh)
    except FileNotFoundError:
        return {}
    except (OSError, json.JSONDecodeError) as exc:
        print(f"[open_mirror.config] failed to read {path!r}: {exc}", file=sys.stderr)
        return {}
    if not isinstance(data, dict):
        print(f"[open_mirror.config] {path}: top-level JSON must be an object; ignoring.", file=sys.stderr)
        return {}
    return data


def load_targets(path: str = _CONFIG_FILE) -> list[OpenMirrorTarget]:
    """Return every configured Open Mirroring target (empty list when unconfigured)."""
    raw = _load_raw(path)
    section = raw.get("open_mirror", raw) if isinstance(raw.get("open_mirror"), dict) else raw
    entries = section.get("open_mirror_targets") if isinstance(section, dict) else None
    if not isinstance(entries, list):
        return []
    targets: list[OpenMirrorTarget] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        try:
            targets.append(target_from_dict(entry))
        except ValueError as exc:
            print(f"[open_mirror.config] skipping invalid target: {exc}", file=sys.stderr)
    return targets


# Module-level registry loaded at import (mirrors connection_config.CONNECTIONS).
TARGETS: list[OpenMirrorTarget] = load_targets()

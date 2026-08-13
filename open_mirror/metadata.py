"""Landing-zone metadata file builders (Fabric Open Mirroring format).

- ``_metadata.json`` (per table folder): declares ``keyColumns`` and optional
  file-detection / upsert behavior.
- ``_partnerEvents.json`` (per mirrored database): optional partner/source info.

See https://learn.microsoft.com/fabric/mirroring/open-mirroring-landing-zone-format
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping

TABLE_METADATA_FILE = "_metadata.json"
PARTNER_EVENTS_FILE = "_partnerEvents.json"


def build_table_metadata(
    key_columns: Iterable[str],
    *,
    file_detection_strategy: str | None = None,
    upsert_default: bool | None = None,
) -> dict:
    """Build the per-table ``_metadata.json`` record.

    ``keyColumns`` enables update/delete/upsert semantics; once set for a table it
    must not change. ``file_detection_strategy`` (e.g. ``LastUpdateTimeFileDetection``)
    and ``upsert_default`` (``isUpsertDefaultRowMarker``) are optional non-sequential
    / upsert controls.
    """
    keys = [str(c).strip() for c in key_columns if str(c).strip()]
    if not keys:
        raise ValueError("build_table_metadata: at least one key column is required")
    meta: dict = {"keyColumns": keys}
    if file_detection_strategy:
        meta["fileDetectionStrategy"] = str(file_detection_strategy)
    if upsert_default is not None:
        meta["isUpsertDefaultRowMarker"] = bool(upsert_default)
    return meta


def build_partner_events(
    partner_name: str,
    *,
    source_type: str | None = None,
    source_version: str | None = None,
    additional: Mapping[str, str] | None = None,
) -> dict:
    """Build the database-level ``_partnerEvents.json`` record."""
    name = str(partner_name or "").strip()
    if not name:
        raise ValueError("build_partner_events: partner_name is required")
    source_info: dict = {}
    if source_type:
        source_info["sourceType"] = str(source_type)
    if source_version:
        source_info["sourceVersion"] = str(source_version)
    if additional:
        source_info["additionalInformation"] = {str(k): str(v) for k, v in additional.items()}
    event: dict = {"partnerName": name}
    if source_info:
        event["sourceInfo"] = source_info
    return event

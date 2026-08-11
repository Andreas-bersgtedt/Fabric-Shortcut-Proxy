"""Landing-zone publisher: writes metadata + numbered Parquet batches.

Ties together :mod:`open_mirror.landing_zone`, :mod:`open_mirror.metadata`, and
:mod:`open_mirror.manifest` to emit a Fabric-compatible table folder.

Change tracking follows the spec: for incremental changes a trailing
``__rowMarker__`` column (0=insert, 1=update, 2=delete, 4=upsert) is appended as
the FINAL column. For an initial load the marker is omitted and Fabric treats the
whole file as inserts.
"""
from __future__ import annotations

import io
import json
from collections.abc import Sequence
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from iceberg.schema import pyarrow_schema
from open_mirror.config import OpenMirrorTableTarget, OpenMirrorTarget
from open_mirror.landing_zone import LandingZoneBackend, table_relative_path
from open_mirror.manifest import format_file_name, next_file_index
from open_mirror.metadata import (
    PARTNER_EVENTS_FILE,
    TABLE_METADATA_FILE,
    build_partner_events,
    build_table_metadata,
)

# Per spec: the change-marker column name (two underscores on each side), and it
# must be the FINAL column in the physical schema.
ROW_MARKER_COLUMN = "__rowMarker__"

_COMPRESSION = "snappy"


def build_landing_parquet(
    rows: Sequence[dict[str, Any]],
    columns,
    *,
    row_markers: Sequence[int] | None = None,
) -> bytes:
    """Serialize ``rows`` to Parquet for the landing zone.

    ``columns`` is a list of :class:`config.ColumnDef`. When ``row_markers`` is
    given, a trailing ``__rowMarker__`` int32 column is appended (one marker per
    row) so Fabric applies the rows as incremental changes; otherwise the file is
    an initial-load insert batch.
    """
    if row_markers is not None and len(row_markers) != len(rows):
        raise ValueError("row_markers must have one entry per row")

    base_schema = pyarrow_schema(columns)
    fields = list(base_schema)
    if row_markers is not None:
        fields.append(pa.field(ROW_MARKER_COLUMN, pa.int32(), nullable=False))
    schema = pa.schema(fields)

    data: dict[str, pa.Array] = {}
    for col in columns:
        pa_type = base_schema.field(col.name).type
        raw = [r.get(col.name) for r in rows]
        try:
            data[col.name] = pa.array(raw, type=pa_type)
        except (pa.ArrowInvalid, pa.ArrowTypeError):
            str_arr = pa.array([str(v) if v is not None else None for v in raw])
            data[col.name] = str_arr.cast(pa_type, safe=False)
    if row_markers is not None:
        data[ROW_MARKER_COLUMN] = pa.array(list(row_markers), type=pa.int32())

    table = pa.table(data, schema=schema)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression=_COMPRESSION, write_statistics=True, write_batch_size=65536)
    return buf.getvalue()


class LandingZonePublisher:
    """Writes one mirrored database's landing-zone files via a backend."""

    def __init__(self, backend: LandingZoneBackend, target: OpenMirrorTarget) -> None:
        self.backend = backend
        self.target = target

    # -- metadata ---------------------------------------------------------

    def ensure_partner_events(self) -> str | None:
        """Write the database-level ``_partnerEvents.json`` when source info is set.

        Returns the relative path written, or ``None`` when there is nothing to
        declare. The file is optional but recommended by the spec.
        """
        if not self.target.source_type and self.target.partner_name == "":
            return None
        event = build_partner_events(
            self.target.partner_name,
            source_type=self.target.source_type,
            source_version=self.target.source_version,
        )
        self.backend.write_text(PARTNER_EVENTS_FILE, json.dumps(event, indent=2))
        return PARTNER_EVENTS_FILE

    def ensure_table_metadata(
        self,
        table: OpenMirrorTableTarget,
        *,
        file_detection_strategy: str | None = None,
        upsert_default: bool | None = None,
        overwrite: bool = False,
    ) -> str:
        """Write a table's ``_metadata.json`` (idempotent unless ``overwrite``).

        Per the spec ``keyColumns`` must not change once set, so an existing file
        is left untouched unless ``overwrite`` is explicitly requested.
        """
        rel = f"{self._table_dir(table)}/{TABLE_METADATA_FILE}"
        if self.backend.exists(rel) and not overwrite:
            return rel
        meta = build_table_metadata(
            table.key_columns,
            file_detection_strategy=file_detection_strategy,
            upsert_default=upsert_default,
        )
        self.backend.write_text(rel, json.dumps(meta, indent=2))
        return rel

    # -- data -------------------------------------------------------------

    def publish_batch(
        self,
        table: OpenMirrorTableTarget,
        rows: Sequence[dict[str, Any]],
        columns,
        *,
        row_markers: Sequence[int] | None = None,
    ) -> str:
        """Write the next numbered Parquet file for ``table`` and return its path."""
        table_dir = self._table_dir(table)
        index = next_file_index(self.backend.list_dir(table_dir))
        rel = f"{table_dir}/{format_file_name(index)}"
        parquet_bytes = build_landing_parquet(rows, columns, row_markers=row_markers)
        self.backend.write_bytes(rel, parquet_bytes)
        return rel

    def publish_initial_load(
        self,
        table: OpenMirrorTableTarget,
        rows: Sequence[dict[str, Any]],
        columns,
        *,
        file_detection_strategy: str | None = None,
        upsert_default: bool | None = None,
    ) -> str:
        """Emit ``_metadata.json`` (if missing) + an initial-load Parquet file."""
        self.ensure_table_metadata(
            table,
            file_detection_strategy=file_detection_strategy,
            upsert_default=upsert_default,
        )
        return self.publish_batch(table, rows, columns)

    # -- helpers ----------------------------------------------------------

    def _table_dir(self, table: OpenMirrorTableTarget) -> str:
        return table_relative_path(table.target_table, table.schema)

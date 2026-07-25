"""
Iceberg per-column statistics: collection + single-value binary encoding (F3).

Column stats (record/null counts, sizes, and lower/upper bounds) let Iceberg
readers prune data files by comparing query predicates against each file's
bounds. Bounds use Iceberg's single-value binary serialization:
little-endian for numeric primitives, UTF-8 for strings, days-since-epoch for
dates, micros for time/timestamp, two's-complement big-endian for decimals.

Stats are derived from the generated Parquet file's own metadata (row-group
statistics), so they exactly describe the bytes the proxy serves.
"""
from __future__ import annotations

import datetime
import io
import struct
from dataclasses import dataclass

import pyarrow.parquet as pq

from config import ColumnDef

_EPOCH = datetime.date(1970, 1, 1)


@dataclass
class ColumnStats:
    field_id: int
    column_size: int
    value_count: int
    null_count: int
    lower: bytes | None
    upper: bytes | None


def encode_bound(iceberg_type: str, value) -> bytes | None:
    """Serialize a single value to Iceberg's binary bound representation."""
    if value is None:
        return None
    t = iceberg_type.strip().lower()
    try:
        if t == "boolean":
            return b"\x01" if value else b"\x00"
        if t == "int":
            return struct.pack("<i", int(value))
        if t == "long":
            return struct.pack("<q", int(value))
        if t == "float":
            return struct.pack("<f", float(value))
        if t == "double":
            return struct.pack("<d", float(value))
        if t == "date":
            days = (value - _EPOCH).days if isinstance(value, datetime.date) else int(value)
            return struct.pack("<i", days)
        if t in ("time", "timestamp", "timestamptz"):
            if isinstance(value, datetime.datetime):
                micros = int(value.timestamp() * 1_000_000)
            else:
                micros = int(value)
            return struct.pack("<q", micros)
        if t == "string":
            return str(value).encode("utf-8")
        if t == "binary":
            return bytes(value)
        if t == "uuid":
            import uuid as _uuid
            return value.bytes if isinstance(value, _uuid.UUID) else _uuid.UUID(str(value)).bytes
        if t.startswith("decimal("):
            return _encode_decimal(value)
        if t.startswith("fixed("):
            return bytes(value)
    except (ValueError, TypeError, struct.error):
        return None
    return None


def _encode_decimal(value) -> bytes:
    from decimal import Decimal

    d = Decimal(str(value))
    exponent = d.as_tuple().exponent
    unscaled = int(d.scaleb(-exponent)) if isinstance(exponent, int) and exponent < 0 else int(d)
    length = (unscaled.bit_length() + 8) // 8 or 1
    return unscaled.to_bytes(length, byteorder="big", signed=True)


def collect_split_stats(parquet_bytes: bytes, columns: list[ColumnDef]) -> dict[int, ColumnStats]:
    """Aggregate per-column stats from a generated Parquet file's metadata."""
    md = pq.ParquetFile(io.BytesIO(parquet_bytes)).metadata
    name_to_col = {c.name: c for c in columns}
    result: dict[int, ColumnStats] = {}

    for c in range(md.num_columns):
        name = md.row_group(0).column(c).path_in_schema
        coldef = name_to_col.get(name)
        if coldef is None:
            continue
        size = 0
        nulls = 0
        mn = None
        mx = None
        for rg in range(md.num_row_groups):
            cc = md.row_group(rg).column(c)
            size += cc.total_compressed_size
            st = cc.statistics
            if st is None:
                continue
            if st.null_count is not None:
                nulls += st.null_count
            if st.has_min_max:
                # Reading st.min/st.max converts the stat to a Python value. For a
                # tz-aware timestamp (timestamptz) that needs the zoneinfo/tzdata
                # package, which may not be installed. Bounds are OPTIONAL, so on
                # any failure we simply skip them for this column rather than
                # crashing startup / manifest generation.
                try:
                    cmn, cmx = st.min, st.max
                except Exception:  # noqa: BLE001 - pyarrow ArrowInvalid etc.
                    cmn = cmx = None
                if cmn is not None and (mn is None or cmn < mn):
                    mn = cmn
                if cmx is not None and (mx is None or cmx > mx):
                    mx = cmx
        result[coldef.field_id] = ColumnStats(
            field_id=coldef.field_id,
            column_size=size,
            value_count=md.num_rows,
            null_count=nulls,
            lower=encode_bound(coldef.iceberg_type, mn),
            upper=encode_bound(coldef.iceberg_type, mx),
        )
    return result

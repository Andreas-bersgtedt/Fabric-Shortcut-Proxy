"""
Iceberg ↔ PyArrow schema translation.

Keeps the single source of truth (config.TABLE_SCHEMA) and exposes:
  - iceberg_schema_dict()   → dict suitable for metadata.json "schemas" list
  - pyarrow_schema()        → pa.Schema for Parquet serialization
"""
from __future__ import annotations

import pyarrow as pa

import config
from config import ColumnDef


# ---------------------------------------------------------------------------
# Iceberg type → PyArrow type
# ---------------------------------------------------------------------------

def _iceberg_to_pa(iceberg_type: str) -> pa.DataType:
    t = iceberg_type.strip().lower()
    if t == "boolean":
        return pa.bool_()
    if t == "int":
        return pa.int32()
    if t == "long":
        return pa.int64()
    if t == "float":
        return pa.float32()
    if t == "double":
        return pa.float64()
    if t == "date":
        return pa.date32()
    if t == "time":
        return pa.time64("us")
    if t in ("timestamp", "timestamptz"):
        return pa.timestamp("us", tz="UTC" if t == "timestamptz" else None)
    if t == "string":
        return pa.string()      # Parquet BINARY + UTF8 — required by Iceberg spec
    if t == "binary":
        return pa.binary()      # Parquet BINARY — required by Iceberg spec
    if t == "uuid":
        return pa.binary(16)    # Iceberg uuid = fixed_len_byte_array(16)
    if t.startswith("decimal("):
        inner = t[len("decimal("):-1]
        prec, scale = (int(x.strip()) for x in inner.split(","))
        return pa.decimal128(prec, scale)
    if t.startswith("fixed("):
        length = int(t[len("fixed("):-1])
        return pa.binary(length)
    raise ValueError(f"Unsupported Iceberg type: {iceberg_type!r}")


def pyarrow_schema(columns: list[ColumnDef] | None = None) -> pa.Schema:
    """Return the PyArrow schema for Parquet generation.

    Each field carries a ``PARQUET:field_id`` metadata entry so PyArrow writes
    the Iceberg field ID into the Parquet column metadata. Iceberg readers
    (and OneLake's Iceberg->Delta virtualization) require these field IDs to
    map Parquet columns to Iceberg schema fields; without them conversion fails.
    """
    cols = columns or config.TABLE_SCHEMA
    fields = []
    for col in cols:
        pa_type = _iceberg_to_pa(col.iceberg_type)
        field_metadata = {b"PARQUET:field_id": str(col.field_id).encode()}
        fields.append(pa.field(col.name, pa_type, nullable=col.nullable, metadata=field_metadata))
    return pa.schema(fields)


# ---------------------------------------------------------------------------
# Iceberg schema dict (for metadata.json)
# ---------------------------------------------------------------------------

def _iceberg_type_json(col: ColumnDef) -> str:
    """Return the Iceberg JSON type representation for a column.

    Per the Iceberg spec (Appendix C, JSON serialization) EVERY primitive type —
    including ``decimal(P, S)`` — is serialized as a JSON *string*. Only nested
    types (struct/list/map) are objects. Emitting decimal as an object
    (``{"type":"decimal",...}``) makes the Iceberg-Java parser used by Fabric's
    XTable conversion reject the whole schema, so the table never converts.
    """
    t = col.iceberg_type.strip().lower()
    if t.startswith("decimal("):
        inner = t[len("decimal("):-1]
        prec, scale = (int(x.strip()) for x in inner.split(","))
        return f"decimal({prec}, {scale})"
    return col.iceberg_type  # primitives are plain strings


def iceberg_schema_dict(schema_id: int = 0, columns: list[ColumnDef] | None = None) -> dict:
    """Return an Iceberg v2 schema dict for metadata.json."""
    cols = columns or config.TABLE_SCHEMA
    fields = []
    for col in cols:
        fields.append({
            "id": col.field_id,
            "name": col.name,
            "required": not col.nullable,
            "type": _iceberg_type_json(col),
        })
    return {
        "schema-id": schema_id,
        "type": "struct",
        "fields": fields,
    }

"""
Parquet generator.

Converts a list of row dicts (from SQL) into Parquet-format bytes
whose schema exactly matches the declared Iceberg schema.

Uses PyArrow in-memory; the result is buffered entirely before returning,
so `Content-Length` can be set accurately in the S3 response.
"""
from __future__ import annotations

import io
from decimal import Decimal
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from fabric_shortcut_proxy import config
from fabric_shortcut_proxy.iceberg.schema import pyarrow_schema
from fabric_shortcut_proxy.observability.logging import get_logger

log = get_logger(__name__)

# Parquet writer settings
_COMPRESSION = "snappy"
_ROW_GROUP_SIZE = 128 * 1024 * 1024  # 128 MB target row group


def rows_to_parquet(rows: list[dict[str, Any]], split_index: int, columns=None) -> bytes:
    """
    Convert SQL row dicts to Parquet bytes.

    Column order and types follow the supplied ``columns`` (defaulting to
    ``config.TABLE_SCHEMA``) / ``pyarrow_schema()``. Missing values are coerced
    to None (null).
    """
    cols = columns if columns is not None else config.TABLE_SCHEMA
    schema = pyarrow_schema(cols)

    if not rows:
        # Write an empty table with the correct schema
        table = pa.table({col.name: pa.array([], type=schema.field(col.name).type)
                          for col in cols}, schema=schema)
        return _table_to_bytes(table, split_index)

    table = _build_table(rows, cols, schema, split_index)
    log.info(
        "parquet_generated",
        split_index=split_index,
        num_rows=len(table),
        num_columns=len(table.schema),
    )
    return _table_to_bytes(table, split_index)


def _build_table(rows: list[dict[str, Any]], cols, schema: "pa.Schema",
                 split_index: int) -> "pa.Table":
    """Build a PyArrow table from row dicts, coercing types to ``schema`` (with a
    string-cast fallback for stubborn values). Shared by the single-shot and
    streaming generators."""
    columns_out: dict[str, pa.Array] = {}
    for col in cols:
        raw_values = [row.get(col.name) for row in rows]
        pa_type = schema.field(col.name).type
        fallback_action = None
        fallback_count = 0
        if pa.types.is_floating(pa_type):
            fallback_count = sum(isinstance(value, Decimal) for value in raw_values)
            if fallback_count:
                raw_values = [
                    float(value) if isinstance(value, Decimal) else value
                    for value in raw_values
                ]
                fallback_action = "decimal_to_float"
        elif pa.types.is_binary(pa_type) or pa.types.is_large_binary(pa_type):
            fallback_count = sum(isinstance(value, Decimal) for value in raw_values)
            if fallback_count:
                raw_values = [
                    format(value, "f").encode("utf-8")
                    if isinstance(value, Decimal) else value
                    for value in raw_values
                ]
                fallback_action = "decimal_to_utf8_bytes"
        elif pa.types.is_string(pa_type) or pa.types.is_large_string(pa_type):
            fallback_count = sum(isinstance(value, Decimal) for value in raw_values)
            if fallback_count:
                raw_values = [
                    format(value, "f") if isinstance(value, Decimal) else value
                    for value in raw_values
                ]
                fallback_action = "decimal_to_string"
        if fallback_action:
            log.info(
                "type_value_normalized",
                column=col.name,
                split_index=split_index,
                target_type=str(pa_type),
                source_type="decimal.Decimal",
                value_count=fallback_count,
                action=fallback_action,
            )
        try:
            arr = pa.array(raw_values, type=pa_type)
        except (pa.ArrowInvalid, pa.ArrowTypeError) as exc:
            log.warning(
                "type_cast_fallback",
                column=col.name,
                split_index=split_index,
                error=str(exc),
            )
            try:
                str_arr = pa.array([
                    str(value) if value is not None else None
                    for value in raw_values
                ])
                arr = str_arr.cast(pa_type, safe=False)
            except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as fallback_exc:
                raise ValueError(
                    f"Column {col.name!r} cannot be converted to declared Arrow "
                    f"type {pa_type}: {fallback_exc}"
                ) from exc
        columns_out[col.name] = arr

    return pa.table(columns_out, schema=schema)


async def stream_rows_to_parquet(row_batches, split_index: int, columns=None) -> tuple[bytes, int]:
    """Stream row-dict batches into one Parquet file (Phase 4 bounded memory).

    ``row_batches`` is an async iterator of ``list[dict]``. Each batch is written
    as its own row group via a single :class:`pyarrow.parquet.ParquetWriter`, so
    only one batch is resident at a time. Returns ``(parquet_bytes, num_rows)``.
    The bytes are still buffered in full (an S3 object needs a Content-Length and
    a locatable footer) — the win is the peak *input* memory, not the output.
    """
    cols = columns if columns is not None else config.TABLE_SCHEMA
    schema = pyarrow_schema(cols)
    buf = io.BytesIO()
    writer: pq.ParquetWriter | None = None
    total = 0
    try:
        async for batch in row_batches:
            if not batch:
                continue
            table = _build_table(batch, cols, schema, split_index)
            if writer is None:
                writer = pq.ParquetWriter(buf, schema, compression=_COMPRESSION)
            writer.write_table(table)
            total += len(batch)
        if writer is None:
            # No rows — emit an empty file with the correct schema.
            empty = pa.table({c.name: pa.array([], type=schema.field(c.name).type)
                              for c in cols}, schema=schema)
            writer = pq.ParquetWriter(buf, schema, compression=_COMPRESSION)
            writer.write_table(empty)
    finally:
        if writer is not None:
            writer.close()
    data = buf.getvalue()
    log.info("parquet_streamed", split_index=split_index, num_rows=total, bytes=len(data))
    return data, total


def _table_to_bytes(table: pa.Table, split_index: int) -> bytes:
    buf = io.BytesIO()
    pq.write_table(
        table,
        buf,
        compression=_COMPRESSION,
        row_group_size=_ROW_GROUP_SIZE,
        write_statistics=True,
        # Use Iceberg-compatible metadata conventions
        write_batch_size=65536,
    )
    data = buf.getvalue()
    log.info(
        "parquet_bytes",
        split_index=split_index,
        bytes=len(data),
    )
    return data

"""
Unit tests for the Parquet generator.
"""
from __future__ import annotations

import io
import os
from decimal import Decimal

import pytest

os.environ.setdefault("DB_URL", "sqlite+aiosqlite:///:memory:")

import pyarrow.parquet as pq

import fabric_shortcut_proxy.parquet.generator as generator
from fabric_shortcut_proxy.parquet.generator import rows_to_parquet
from fabric_shortcut_proxy import config
from fabric_shortcut_proxy.config import ColumnDef


def _sample_rows(n: int = 10) -> list[dict]:
    return [
        {
            "id": i,
            "order_date": "2024-01-15",
            "customer_id": 100 + i,
            "product": "Widget A",
            "quantity": i * 2,
            "unit_price": 9.99,
            "total": round(i * 2 * 9.99, 2),
            "region": "North",
        }
        for i in range(1, n + 1)
    ]


def test_parquet_has_correct_row_count():
    rows = _sample_rows(100)
    data = rows_to_parquet(rows, split_index=0)
    table = pq.read_table(io.BytesIO(data))
    assert table.num_rows == 100


def test_parquet_schema_matches_iceberg():
    rows = _sample_rows(5)
    data = rows_to_parquet(rows, split_index=0)
    table = pq.read_table(io.BytesIO(data))
    expected_names = [col.name for col in config.TABLE_SCHEMA]
    assert list(table.schema.names) == expected_names


def test_empty_rows_produces_valid_parquet():
    data = rows_to_parquet([], split_index=0)
    table = pq.read_table(io.BytesIO(data))
    assert table.num_rows == 0
    expected_names = [col.name for col in config.TABLE_SCHEMA]
    assert list(table.schema.names) == expected_names


def test_parquet_bytes_are_nonzero():
    rows = _sample_rows(1)
    data = rows_to_parquet(rows, split_index=0)
    assert len(data) > 0
    # Parquet magic bytes
    assert data[:4] == b"PAR1"
    assert data[-4:] == b"PAR1"


def test_decimal_values_fit_declared_float_columns():
    rows = _sample_rows(1)
    rows[0]["unit_price"] = Decimal("282.27")
    rows[0]["total"] = Decimal("26815.65")

    table = pq.read_table(io.BytesIO(rows_to_parquet(rows, split_index=0)))

    assert table.column("unit_price")[0].as_py() == 282.27
    assert table.column("total")[0].as_py() == 26815.65


def test_decimal_value_uses_deterministic_bytes_for_stale_binary_schema():
    columns = [
        ColumnDef(field_id=1, name="weight", iceberg_type="binary", nullable=True)
    ]

    data = rows_to_parquet(
        [{"weight": Decimal("189.5000")}, {"weight": None}],
        split_index=3,
        columns=columns,
    )
    table = pq.read_table(io.BytesIO(data))

    assert table.column("weight").to_pylist() == [b"189.5000", None]


def test_decimal_value_converts_directly_for_stale_string_schema(monkeypatch):
    warnings = []
    monkeypatch.setattr(
        generator.log, "warning", lambda event, **values: warnings.append(event)
    )
    columns = [
        ColumnDef(field_id=1, name="weight", iceberg_type="string", nullable=True)
    ]

    data = rows_to_parquet(
        [{"weight": Decimal("189.5000")}, {"weight": None}],
        split_index=0,
        columns=columns,
    )
    table = pq.read_table(io.BytesIO(data))

    assert table.column("weight").to_pylist() == ["189.5000", None]
    assert "type_cast_fallback" not in warnings


@pytest.mark.asyncio
async def test_streaming_decimal_value_uses_binary_compatibility_path():
    from fabric_shortcut_proxy.parquet.generator import stream_rows_to_parquet

    columns = [
        ColumnDef(field_id=1, name="weight", iceberg_type="binary", nullable=True)
    ]

    async def batches():
        yield [{"weight": Decimal("2.50")}]
        yield [{"weight": Decimal("3.75")}]

    data, row_count = await stream_rows_to_parquet(
        batches(), split_index=1, columns=columns
    )
    table = pq.read_table(io.BytesIO(data))

    assert row_count == 2
    assert table.column("weight").to_pylist() == [b"2.50", b"3.75"]

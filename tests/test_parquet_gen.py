"""
Unit tests for the Parquet generator.
"""
from __future__ import annotations

import io
import os

os.environ.setdefault("DB_URL", "sqlite+aiosqlite:///:memory:")

import pyarrow.parquet as pq

from parquet.generator import rows_to_parquet
import config


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

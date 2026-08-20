"""Phase 3 — source database -> landing-zone orchestration tests.

Seeds a real SQLite source, reflects its schema through the same executor the read
path uses, reads its rows, and publishes an initial load into a local landing zone.
No mocks: the query, reflection, and Parquet write all run for real.
"""
from __future__ import annotations

import os
import pathlib

os.environ.setdefault("DB_URL", "sqlite+aiosqlite:///:memory:")

import pyarrow.parquet as pq
import pytest
from sqlalchemy import create_engine, text

import config
import db.executor as executor
from config import ColumnDef, ColumnTransform
from open_mirror import target_from_dict
from open_mirror.source import (
    _select_all_sql,
    _configured_columns,
    _render_projection,
    _validate_projection_strategy,
    publish_initial_load,
    publish_target_initial_load,
)
from planner.dialects import _MSSQL, _ORACLE, _SQLITE, _TERADATA


def _seed_sqlite(path: pathlib.Path) -> None:
    eng = create_engine(f"sqlite:///{path.as_posix()}")
    with eng.begin() as c:
        c.execute(text("CREATE TABLE sales (id INTEGER PRIMARY KEY, name TEXT, status TEXT)"))
        for i in range(1, 6):
            c.execute(
                text("INSERT INTO sales (id, name, status) VALUES (:i, :n, :s)"),
                {"i": i, "n": f"n{i}", "s": "active"},
            )
    eng.dispose()


@pytest.fixture
async def sqlite_source(tmp_path, monkeypatch):
    db = tmp_path / "src.db"
    _seed_sqlite(db)
    monkeypatch.setattr(config, "DB_URL", f"sqlite+aiosqlite:///{db.as_posix()}", raising=False)
    monkeypatch.setattr(
        config, "OPEN_MIRROR_STATE_DIR", str(tmp_path / "state"), raising=False
    )
    executor._engine = None
    executor._sync_engine = None
    yield tmp_path
    await executor.dispose_engines()


def _target(landing_root: pathlib.Path):
    return target_from_dict({
        "id": "fabric-sales",
        "connection": "default",
        "landing_zone_root": str(landing_root),
        "source_type": "SQL",
        "tables": [{
            "name": "sales", "source_table": "sales", "target_table": "sales",
            "key_column": "id", "schema": "dbo",
        }],
    })


# --- select-all SQL per dialect --------------------------------------------

def test_select_all_sql_per_dialect():
    assert _select_all_sql(_SQLITE, '"id"', '"sales"') == 'SELECT "id" FROM "sales" LIMIT :max_rows'
    assert _select_all_sql(_MSSQL, "[id]", "[sales]") == "SELECT TOP (:max_rows) [id] FROM [sales]"
    assert "FETCH FIRST :max_rows ROWS ONLY" in _select_all_sql(_ORACLE, '"id"', '"sales"')
    assert "QUALIFY ROW_NUMBER()" in _select_all_sql(_TERADATA, '"id"', '"sales"')


def test_open_mirror_columns_reuse_tokenization_projection(monkeypatch):
    monkeypatch.setenv("FSP_TOKENIZATION_KEY_CUSTOMER_PII_V1", "secret-value")
    columns = [
        ColumnDef(field_id=1, name="id", iceberg_type="long", nullable=False),
        ColumnDef(
            field_id=2,
            name="email_token",
            source="email",
            iceberg_type="string",
            transform=ColumnTransform(
                kind="deterministic_hash",
                key_ref="customer-pii-v1",
                domain="customer-email",
                normalization="trim_lower",
            ),
        ),
    ]

    projected, params = _render_projection(_MSSQL, columns)

    assert "HASHBYTES('SHA2_256'" in projected
    assert "[email_token]" in projected
    assert "secret-value" not in projected
    assert params["fsp_token_key_om_2"] == "secret-value"
    assert params["fsp_token_domain_om_2"] == "customer-email"


def test_open_mirror_columns_require_pass_through_control_columns(tmp_path):
    target = target_from_dict({
        "id": "fabric-sales",
        "connection": "default",
        "landing_zone_root": str(tmp_path),
        "tables": [{
            "name": "sales",
            "source_table": "sales",
            "target_table": "sales",
            "key_column": "id",
            "columns": [{
                "field_id": 1,
                "name": "id_token",
                "source": "id",
                "type": "string",
                "transform": {
                    "kind": "deterministic_hash",
                    "key_ref": "customer-pii-v1",
                },
            }],
        }],
    })

    with pytest.raises(ValueError, match="control column 'id' must be pass-through"):
        _configured_columns(
            target.tables[0],
            [ColumnDef(field_id=1, name="id", iceberg_type="long", nullable=False)],
        )


def test_open_mirror_random_tokens_are_allowed_with_prepared_recovery():
    columns = [
        ColumnDef(field_id=1, name="id", iceberg_type="long", nullable=False),
        ColumnDef(
            field_id=2,
            name="note_token",
            source="note",
            iceberg_type="string",
            transform=ColumnTransform(kind="random_token"),
        ),
    ]

    _validate_projection_strategy(columns, "snapshot")


# --- end-to-end publish from a live source ---------------------------------

async def test_publish_initial_load_from_source(sqlite_source):
    landing = sqlite_source / "lz"
    target = _target(landing)

    result = await publish_initial_load(target, target.tables[0])

    assert result.rows == 5
    assert result.path == "dbo.schema/sales/00000000000000000001.parquet"
    assert [c.name for c in result.columns] == ["id", "name", "status"]

    data_path = landing / "dbo.schema" / "sales" / "00000000000000000001.parquet"
    table = pq.read_table(data_path)
    assert table.num_rows == 5
    assert table.column("id").to_pylist() == [1, 2, 3, 4, 5]
    assert "__rowMarker__" not in table.schema.names

    meta_path = landing / "dbo.schema" / "sales" / "_metadata.json"
    assert meta_path.exists()
    partner_path = landing / "_partnerEvents.json"
    assert partner_path.exists()


async def test_publish_respects_max_rows(sqlite_source):
    landing = sqlite_source / "lz"
    target = _target(landing)

    result = await publish_initial_load(target, target.tables[0], max_rows=2)
    assert result.rows == 2

    table = pq.read_table(landing / "dbo.schema" / "sales" / "00000000000000000001.parquet")
    assert table.num_rows == 2


async def test_publish_target_initial_load_iterates_tables(sqlite_source):
    landing = sqlite_source / "lz"
    target = _target(landing)

    results = await publish_target_initial_load(target)
    assert len(results) == 1
    assert results[0].rows == 5

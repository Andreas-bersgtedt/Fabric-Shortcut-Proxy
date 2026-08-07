"""
F6 — SQL dialect adapter tests.

Verifies build_split_query() emits correct SQL per dialect:
  - SQLite / Postgres: LIMIT suffix, double-quoted identifiers, CAST type
  - SQL Server (T-SQL): TOP prefix (no LIMIT), bracket identifiers, BIGINT CAST
"""
from __future__ import annotations

import config
import pytest
from config import ColumnDef, ColumnTransform, TableDef
from iceberg.state_store import SplitDescriptor
from planner.dialects import (
    get_dialect,
    SQLiteDialect,
    PostgresDialect,
    MSSQLDialect,
    OracleDialect,
    DatabricksDialect,
    RedshiftDialect,
    TeradataDialect,
    ImpalaDialect,
)
from planner.split_planner import build_split_query


_SCHEMA = [
    ColumnDef(field_id=1, name="id", iceberg_type="long", nullable=False),
    ColumnDef(field_id=2, name="name", iceberg_type="string", nullable=True),
]
_TABLE = TableDef(name="widgets", source_table="widgets", schema=_SCHEMA, num_splits=4)


def _split() -> SplitDescriptor:
    return SplitDescriptor(
        split_index=1,
        num_splits=4,
        object_key="warehouse/db/widgets/data/split-1-x.parquet",
        watermark_ms=0,
        table=_TABLE,
    )


# ---------------------------------------------------------------------------
# get_dialect resolution
# ---------------------------------------------------------------------------

def test_get_dialect_by_scheme():
    assert isinstance(get_dialect("sqlite+aiosqlite:///x.db"), SQLiteDialect)
    assert isinstance(get_dialect("postgresql+asyncpg://h/db"), PostgresDialect)
    assert isinstance(get_dialect("mssql+aioodbc://h/db"), MSSQLDialect)
    assert isinstance(get_dialect("oracle+oracledb://h/db"), OracleDialect)
    assert isinstance(get_dialect("databricks://token:pat@dbc.cloud"), DatabricksDialect)
    assert isinstance(get_dialect("redshift+redshift_connector://h:5439/db"), RedshiftDialect)
    assert isinstance(get_dialect("teradatasql://h/?database=dbc"), TeradataDialect)
    assert isinstance(get_dialect("impala://h:21050/db"), ImpalaDialect)


def test_dialect_quoting():
    assert SQLiteDialect().quote("id") == '"id"'
    assert PostgresDialect().quote("id") == '"id"'
    assert MSSQLDialect().quote("id") == "[id]"
    assert DatabricksDialect().quote("id") == "`id`"
    assert RedshiftDialect().quote("id") == '"id"'
    assert TeradataDialect().quote("id") == '"id"'
    assert ImpalaDialect().quote("id") == "`id`"


def test_dialect_cast_type():
    assert SQLiteDialect().cast_int("id") == "CAST(id AS INTEGER)"
    assert PostgresDialect().cast_int("id") == "CAST(id AS BIGINT)"
    assert MSSQLDialect().cast_int("id") == "CAST(id AS BIGINT)"
    assert OracleDialect().cast_int("id") == "CAST(id AS NUMBER(19))"
    assert RedshiftDialect().cast_int("id") == "CAST(id AS BIGINT)"
    assert TeradataDialect().cast_int("id") == "CAST(id AS BIGINT)"
    assert ImpalaDialect().cast_int("id") == "CAST(id AS BIGINT)"


def test_quote_qualified_dotted():
    assert MSSQLDialect().quote_qualified("dbo.sales") == "[dbo].[sales]"
    assert SQLiteDialect().quote_qualified("main.sales") == '"main"."sales"'


# ---------------------------------------------------------------------------
# build_split_query per dialect
# ---------------------------------------------------------------------------

def test_split_query_sqlite(monkeypatch):
    monkeypatch.setattr(config, "DB_URL", "sqlite+aiosqlite:///x.db")
    sql, params = build_split_query(_split())
    assert 'SELECT "id", "name"' in sql
    assert 'FROM "widgets"' in sql
    assert "CAST(\"id\" AS INTEGER)" in sql
    assert "LIMIT :max_rows" in sql
    assert "TOP" not in sql
    assert params == {"num_splits": 4, "split_index": 1, "max_rows": config.QUERY_MAX_ROWS}


def test_split_query_postgres(monkeypatch):
    monkeypatch.setattr(config, "DB_URL", "postgresql+asyncpg://h/db")
    sql, _ = build_split_query(_split())
    assert 'CAST("id" AS BIGINT)' in sql
    assert "LIMIT :max_rows" in sql


def test_split_query_mssql(monkeypatch):
    monkeypatch.setattr(config, "DB_URL", "mssql+aioodbc://h/db")
    sql, _ = build_split_query(_split())
    assert sql.startswith("SELECT TOP (:max_rows) [id], [name]")
    assert "FROM [widgets]" in sql
    assert "CAST([id] AS BIGINT)" in sql
    assert "LIMIT" not in sql
    assert sql.rstrip().endswith("ORDER BY [id]")


def test_split_query_mssql_range_uses_top_not_limit(monkeypatch):
    # Range planning (key_lo/key_hi) must emit TOP too; T-SQL has no LIMIT.
    monkeypatch.setattr(config, "DB_URL", "mssql+aioodbc://h/db")
    split = SplitDescriptor(
        split_index=1, num_splits=4,
        object_key="warehouse/db/widgets/data/split-1-x.parquet",
        watermark_ms=0, table=_TABLE, key_lo=25, key_hi=50,
    )
    sql, params = build_split_query(split)
    assert sql.startswith("SELECT TOP (:max_rows) [id], [name]")
    assert "[id] >= :key_lo AND [id] < :key_hi" in sql
    assert "LIMIT" not in sql
    assert sql.rstrip().endswith("ORDER BY [id]")
    assert params == {"key_lo": 25, "key_hi": 50, "max_rows": config.QUERY_MAX_ROWS}


def test_split_query_oracle(monkeypatch):
    monkeypatch.setattr(config, "DB_URL", "oracle+oracledb://h/db")
    sql, _ = build_split_query(_split())
    assert 'FROM "widgets"' in sql
    assert "MOD(CAST(\"id\" AS NUMBER(19)), :num_splits)" in sql
    assert "FETCH FIRST :max_rows ROWS ONLY" in sql


def test_split_query_databricks(monkeypatch):
    monkeypatch.setattr(config, "DB_URL", "databricks://token:pat@dbc.cloud")
    sql, _ = build_split_query(_split())
    assert "SELECT `id`, `name`" in sql
    assert "CAST(`id` AS BIGINT)" in sql
    assert "LIMIT :max_rows" in sql


def test_split_query_databricks_range_uses_limit(monkeypatch):
    monkeypatch.setattr(config, "DB_URL", "databricks://token:pat@dbc.cloud")
    split = SplitDescriptor(
        split_index=1, num_splits=4,
        object_key="warehouse/db/widgets/data/split-1-x.parquet",
        watermark_ms=0, table=_TABLE, key_lo=25, key_hi=50,
    )
    sql, params = build_split_query(split)
    assert sql.startswith("SELECT `id`, `name`")
    assert "`id` >= :key_lo AND `id` < :key_hi" in sql
    assert "TOP" not in sql
    assert sql.rstrip().endswith("LIMIT :max_rows")
    assert params == {"key_lo": 25, "key_hi": 50, "max_rows": config.QUERY_MAX_ROWS}


def test_split_query_redshift(monkeypatch):
    monkeypatch.setattr(config, "DB_URL", "redshift+redshift_connector://h:5439/db")
    sql, _ = build_split_query(_split())
    assert 'SELECT "id", "name"' in sql
    assert 'FROM "widgets"' in sql
    assert 'CAST("id" AS BIGINT)' in sql
    assert "% :num_splits) = :split_index" in sql
    assert sql.rstrip().endswith("LIMIT :max_rows")


def test_split_query_teradata_uses_mod_and_qualify(monkeypatch):
    monkeypatch.setattr(config, "DB_URL", "teradatasql://h/?database=dbc")
    sql, _ = build_split_query(_split())
    assert 'FROM "widgets"' in sql
    assert '(CAST("id" AS BIGINT) MOD :num_splits) = :split_index' in sql
    assert 'QUALIFY ROW_NUMBER() OVER (ORDER BY "id") <= :max_rows' in sql
    assert "LIMIT" not in sql
    assert sql.rstrip().endswith('ORDER BY "id"')


def test_split_query_teradata_range_uses_qualify(monkeypatch):
    monkeypatch.setattr(config, "DB_URL", "teradatasql://h/?database=dbc")
    split = SplitDescriptor(
        split_index=1, num_splits=4,
        object_key="warehouse/db/widgets/data/split-1-x.parquet",
        watermark_ms=0, table=_TABLE, key_lo=25, key_hi=50,
    )
    sql, params = build_split_query(split)
    assert '"id" >= :key_lo AND "id" < :key_hi' in sql
    assert 'QUALIFY ROW_NUMBER() OVER (ORDER BY "id") <= :max_rows' in sql
    assert "LIMIT" not in sql
    assert params == {"key_lo": 25, "key_hi": 50, "max_rows": config.QUERY_MAX_ROWS}


def test_split_query_impala(monkeypatch):
    monkeypatch.setattr(config, "DB_URL", "impala://h:21050/db")
    sql, _ = build_split_query(_split())
    assert "SELECT `id`, `name`" in sql
    assert "FROM `widgets`" in sql
    assert "CAST(`id` AS BIGINT)" in sql
    assert "% :num_splits) = :split_index" in sql
    assert sql.rstrip().endswith("LIMIT :max_rows")


def _tokenized_table(*, random: bool = False, string_key: bool = False) -> TableDef:
    transform = (
        ColumnTransform(kind="random_token")
        if random
        else ColumnTransform(
            kind="deterministic_hash",
            key_ref="customer-pii-v1",
            domain="customer-email",
            normalization="trim_lower",
        )
    )
    return TableDef(
        name="customers_safe",
        source_table="dbo.customers",
        key_column="customer_id",
        num_splits=4,
        schema=[
            ColumnDef(
                field_id=1,
                name="customer_id",
                iceberg_type="string" if string_key else "long",
                nullable=False,
            ),
            ColumnDef(
                field_id=2,
                name="email_token",
                source="email",
                iceberg_type="string",
                transform=transform,
            ),
        ],
    )


def _table_split(table: TableDef) -> SplitDescriptor:
    return SplitDescriptor(
        split_index=1,
        num_splits=4,
        object_key="warehouse/db/customers_safe/data/split-1.parquet",
        watermark_ms=0,
        table=table,
    )


def test_mssql_deterministic_hash_projection(monkeypatch):
    monkeypatch.setattr(config, "DB_URL", "mssql+aioodbc://h/db")
    monkeypatch.setenv("FSP_TOKENIZATION_KEY_CUSTOMER_PII_V1", "uat-secret")

    sql, params = build_split_query(_table_split(_tokenized_table()))

    assert "HASHBYTES('SHA2_256'" in sql
    assert "LOWER(LTRIM(RTRIM(CONVERT(nvarchar(max), [email]))))" in sql
    assert "END AS [email_token]" in sql
    assert "uat-secret" not in sql
    assert params["fsp_token_key_1"] == "uat-secret"
    assert params["fsp_token_domain_1"] == "customer-email"


def test_mssql_random_token_projection(monkeypatch):
    monkeypatch.setattr(config, "DB_URL", "mssql+aioodbc://h/db")
    sql, params = build_split_query(_table_split(_tokenized_table(random=True)))
    assert "CONVERT(varchar(36), NEWID())" in sql
    assert "END AS [email_token]" in sql
    assert not any(key.startswith("fsp_token_") for key in params)


def test_postgres_deterministic_hash_projection(monkeypatch):
    monkeypatch.setattr(config, "DB_URL", "postgresql+asyncpg://h/db")
    monkeypatch.setenv("FSP_TOKENIZATION_KEY_CUSTOMER_PII_V1", "uat-secret")

    sql, params = build_split_query(_table_split(_tokenized_table()))

    assert "UPPER(ENCODE(DIGEST(" in sql
    assert "LOWER(BTRIM(CAST(\"email\" AS text)))" in sql
    assert "'sha256'), 'hex')" in sql
    assert "END AS \"email_token\"" in sql
    assert "uat-secret" not in sql
    assert params["fsp_token_key_1"] == "uat-secret"
    assert params["fsp_token_domain_1"] == "customer-email"


def test_postgres_random_token_projection(monkeypatch):
    monkeypatch.setattr(config, "DB_URL", "postgresql+asyncpg://h/db")
    sql, params = build_split_query(_table_split(_tokenized_table(random=True)))
    assert "CAST(gen_random_uuid() AS text)" in sql
    assert "END AS \"email_token\"" in sql
    assert not any(key.startswith("fsp_token_") for key in params)


def test_oracle_deterministic_hash_projection(monkeypatch):
    monkeypatch.setattr(config, "DB_URL", "oracle+oracledb://h/db")
    monkeypatch.setenv("FSP_TOKENIZATION_KEY_CUSTOMER_PII_V1", "uat-secret")

    sql, params = build_split_query(_table_split(_tokenized_table()))

    assert "RAWTOHEX(STANDARD_HASH(" in sql
    assert "LOWER(TRIM(CAST(\"email\" AS VARCHAR2(4000))))" in sql
    assert "'SHA256')) END AS \"email_token\"" in sql
    assert "uat-secret" not in sql
    assert params["fsp_token_key_1"] == "uat-secret"
    assert params["fsp_token_domain_1"] == "customer-email"


def test_oracle_random_token_projection(monkeypatch):
    monkeypatch.setattr(config, "DB_URL", "oracle+oracledb://h/db")
    sql, params = build_split_query(_table_split(_tokenized_table(random=True)))
    assert "RAWTOHEX(SYS_GUID())" in sql
    assert "END AS \"email_token\"" in sql
    assert not any(key.startswith("fsp_token_") for key in params)


def test_databricks_deterministic_hash_projection(monkeypatch):
    monkeypatch.setattr(config, "DB_URL", "databricks://token:pat@dbc.cloud")
    monkeypatch.setenv("FSP_TOKENIZATION_KEY_CUSTOMER_PII_V1", "uat-secret")

    sql, params = build_split_query(_table_split(_tokenized_table()))

    assert "upper(sha2(concat(" in sql
    assert "lower(trim(CAST(`email` AS STRING)))" in sql
    assert "), 256)) END AS `email_token`" in sql
    assert "uat-secret" not in sql
    assert params["fsp_token_key_1"] == "uat-secret"
    assert params["fsp_token_domain_1"] == "customer-email"


def test_databricks_random_token_projection(monkeypatch):
    monkeypatch.setattr(config, "DB_URL", "databricks://token:pat@dbc.cloud")
    sql, params = build_split_query(_table_split(_tokenized_table(random=True)))
    assert "ELSE uuid() END AS `email_token`" in sql
    assert not any(key.startswith("fsp_token_") for key in params)


def test_tokenization_fails_closed_on_unsupported_dialect(monkeypatch):
    monkeypatch.setattr(config, "DB_URL", "sqlite+aiosqlite:///x.db")
    with pytest.raises(ValueError, match="does not support column transform"):
        build_split_query(_table_split(_tokenized_table()))


def test_split_key_transform_is_rejected(monkeypatch):
    monkeypatch.setattr(config, "DB_URL", "mssql+aioodbc://h/db")
    table = _tokenized_table()
    table.schema[0].transform = ColumnTransform(kind="random_token")
    with pytest.raises(ValueError, match="Split key 'customer_id'"):
        build_split_query(_table_split(table))


def test_token_projection_uses_alias_in_row_number_outer_query(monkeypatch):
    monkeypatch.setattr(config, "DB_URL", "mssql+aioodbc://h/db")
    monkeypatch.setenv("FSP_TOKENIZATION_KEY_CUSTOMER_PII_V1", "uat-secret")
    sql, _ = build_split_query(_table_split(_tokenized_table(string_key=True)))
    assert sql.startswith(
        "SELECT TOP (:max_rows) [customer_id], [email_token] FROM (SELECT "
    )
    assert sql.count("HASHBYTES('SHA2_256'") == 1


@pytest.mark.parametrize(
    ("db_url", "hash_expression", "outer_columns"),
    [
        ("postgresql+asyncpg://h/db", "DIGEST(", '"customer_id", "email_token"'),
        ("oracle+oracledb://h/db", "STANDARD_HASH(", '"customer_id", "email_token"'),
        ("databricks://token:pat@dbc.cloud", "sha2(", "`customer_id`, `email_token`"),
    ],
)
def test_multi_dialect_token_projection_uses_row_number_alias(
    monkeypatch, db_url, hash_expression, outer_columns
):
    monkeypatch.setattr(config, "DB_URL", db_url)
    monkeypatch.setenv("FSP_TOKENIZATION_KEY_CUSTOMER_PII_V1", "uat-secret")
    sql, _ = build_split_query(_table_split(_tokenized_table(string_key=True)))
    assert sql.startswith(f"SELECT {outer_columns} FROM (SELECT ")
    assert sql.count(hash_expression) == 1


@pytest.mark.parametrize(
    "db_url",
    [
        "redshift+redshift_connector://h:5439/db",
        "teradatasql://h/?database=dbc",
        "impala://h:21050/db",
    ],
)
def test_expanded_sources_reject_tokenization(monkeypatch, db_url):
    monkeypatch.setattr(config, "DB_URL", db_url)
    with pytest.raises(ValueError, match="does not support column transform"):
        build_split_query(_table_split(_tokenized_table(random=True)))

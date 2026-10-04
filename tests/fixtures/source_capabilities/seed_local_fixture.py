"""Create the dedicated Oracle Free or Apache Impala issue-94 test fixture.

This script drops and recreates only the named ``ISSUE94_*`` Oracle objects or
the ``issue94_gate`` Impala database. Run only against an isolated test service.
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import DBAPIError

from db.reflect import build_url

_FIXTURE_ROWS = [
    {"id": 1, "event_ts": datetime(2024, 1, 1, 0, 0), "email": None},
    {"id": 2, "event_ts": datetime(2024, 1, 2, 0, 0), "email": "Jöhn@example.test"},
    {"id": 3, "event_ts": datetime(2024, 1, 3, 0, 0), "email": "second@example.test"},
    {"id": 4, "event_ts": datetime(2024, 1, 4, 0, 0), "email": "third@example.test"},
]


def _source_url(source: str) -> URL:
    if source == "oracle":
        connection = {
            "dialect": "oracle",
            "host": os.environ["ORACLE_HOST"],
            "port": int(os.environ.get("ORACLE_PORT", "1521")),
            "database": os.environ.get("ORACLE_DATABASE", "FREEPDB1"),
            "username": os.environ["ORACLE_USERNAME"],
            "password": os.environ["ORACLE_PASSWORD"],
        }
    else:
        connection = {
            "dialect": "impala",
            "host": os.environ.get("IMPALA_HOST", "127.0.0.1"),
            "port": int(os.environ.get("IMPALA_PORT", "21050")),
            "database": os.environ.get("IMPALA_DATABASE", "default"),
        }
    return build_url(**connection)


def _seed_oracle(connection) -> tuple[str, str]:
    schema = os.environ["ORACLE_USERNAME"].upper()
    table_name = "ISSUE94_ROWS"
    view_name = "ISSUE94_ROWS_VIEW"
    qualified_table = (
        f"{connection.dialect.identifier_preparer.quote_schema(schema)}."
        f"{connection.dialect.identifier_preparer.quote(table_name)}"
    )
    qualified_view = (
        f"{connection.dialect.identifier_preparer.quote_schema(schema)}."
        f"{connection.dialect.identifier_preparer.quote(view_name)}"
    )
    for object_type, object_name in (
        ("VIEW", qualified_view),
        ("TABLE", qualified_table),
    ):
        try:
            connection.execute(text(f"DROP {object_type} {object_name}"))
        except DBAPIError as exc:
            diagnostic = exc.orig.args[0] if exc.orig.args else None
            if getattr(diagnostic, "code", None) != 942:
                raise

    connection.execute(
        text(
            f"CREATE TABLE {qualified_table} ("
            "ID NUMBER(19) NOT NULL PRIMARY KEY, "
            "EVENT_TS TIMESTAMP NOT NULL, "
            "EMAIL VARCHAR2(320))"
        )
    )
    connection.execute(
        text(
            f"INSERT INTO {qualified_table} (ID, EVENT_TS, EMAIL) "
            "VALUES (:id, :event_ts, :email)"
        ),
        _FIXTURE_ROWS,
    )
    connection.execute(
        text(f"CREATE VIEW {qualified_view} AS SELECT * FROM {qualified_table}")
    )
    return f"{schema}.{table_name}", f"{schema}.{view_name}"


def _seed_impala(connection) -> tuple[str, str]:
    schema = "issue94_gate"
    table_name = "issue94_rows"
    view_name = "issue94_rows_view"
    connection.execute(text(f"CREATE DATABASE IF NOT EXISTS {schema}"))
    connection.execute(text(f"DROP VIEW IF EXISTS {schema}.{view_name}"))
    connection.execute(text(f"DROP TABLE IF EXISTS {schema}.{table_name}"))
    connection.execute(
        text(
            f"CREATE TABLE {schema}.{table_name} ("
            "id BIGINT, event_ts TIMESTAMP, email STRING) "
            "STORED AS PARQUET"
        )
    )
    connection.execute(
        text(
            f"INSERT INTO {schema}.{table_name} (id, event_ts, email) "
            "VALUES (:id, :event_ts, :email)"
        ),
        _FIXTURE_ROWS,
    )
    connection.execute(
        text(
            f"CREATE VIEW {schema}.{view_name} AS "
            f"SELECT id, event_ts, email FROM {schema}.{table_name}"
        )
    )
    return f"{schema}.{table_name}", f"{schema}.{view_name}"


def seed_fixture(source: str) -> tuple[str, str]:
    """Recreate the source-specific fixture and return its table and view names."""
    url = _source_url(source)
    engine = create_engine(url, pool_pre_ping=True)
    try:
        with engine.begin() as connection:
            if source == "oracle":
                return _seed_oracle(connection)
            return _seed_impala(connection)
    finally:
        engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", choices=("oracle", "impala"))
    args = parser.parse_args()
    table, view = seed_fixture(args.source)
    prefix = args.source.upper()
    key = "ID" if args.source == "oracle" else "id"
    timestamp = "EVENT_TS" if args.source == "oracle" else "event_ts"
    token_column = "EMAIL" if args.source == "oracle" else "email"
    print('$env:FSP_RUN_SOURCE_CAPABILITY_GATES = "1"')
    print(f'$env:INTEGRATION_{prefix}_TABLE = "{table}"')
    print(f'$env:INTEGRATION_{prefix}_VIEW = "{view}"')
    print(f'$env:INTEGRATION_{prefix}_INTEGER_KEY = "{key}"')
    print(f'$env:INTEGRATION_{prefix}_DATE_COLUMN = "{timestamp}"')
    if args.source == "oracle":
        print('$env:INTEGRATION_ORACLE_PK_COLUMN = "ID"')
    print(f'$env:INTEGRATION_{prefix}_TOKEN_COLUMN = "{token_column}"')
    print(f'$env:INTEGRATION_{prefix}_NULL_ROW_KEY = "1"')
    print(f'$env:INTEGRATION_{prefix}_UNICODE_ROW_KEY = "2"')
    print(
        "Fixture has four rows (one NULL and one Unicode email) and is safe for the small live gate."
    )


if __name__ == "__main__":
    main()

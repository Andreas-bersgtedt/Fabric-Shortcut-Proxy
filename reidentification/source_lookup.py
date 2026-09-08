"""Bounded source-side lookup queries for approved re-identification mappings."""
from __future__ import annotations

import re

import config
from db.executor import execute_split_query
from planner.dialects import get_dialect
from reidentification.mappings import LookupMapping, ReidentificationMappingError

_TOKEN = re.compile(r"^[0-9A-F]{64}$")
_SUPPORTED_DIALECTS = frozenset({"mssql", "postgresql", "oracle", "databricks"})
_TOKEN_PARAMETER = "__token_reidentification"
_LIMIT_PARAMETER = "__reidentification_limit"


def _table_for(mapping: LookupMapping):
    table = next((item for item in config.TABLES if item.name == mapping.table_id), None)
    if table is None or not table.enabled:
        raise ReidentificationMappingError("re-identification mapping table is unavailable")
    return table


def build_lookup_query(mapping: LookupMapping, token: str) -> tuple[str, dict, str]:
    """Build a two-row equality lookup without interpolating the submitted token."""
    if not _TOKEN.fullmatch(token):
        raise ReidentificationMappingError("token must be a 64-character uppercase sha256 value")
    table = _table_for(mapping)
    db_url = config.effective_db_url(table.connection_id)
    dialect = get_dialect(db_url)
    if dialect.name not in _SUPPORTED_DIALECTS:
        raise ReidentificationMappingError(
            f"re-identification is not supported for dialect {dialect.name!r}"
        )
    source = dialect.quote_qualified(table.source_table)
    primary_key = dialect.quote(mapping.primary_key_column)
    clear_text = dialect.quote(mapping.clear_text_column)
    lookup = dialect.quote(mapping.lookup_column)
    projected = (
        f"{primary_key} AS {dialect.quote('__reidentify_primary_key')}, "
        f"{clear_text} AS {dialect.quote('__reidentify_value')}"
    )
    predicate = f"{lookup} = :{_TOKEN_PARAMETER}"
    if dialect.name == "mssql":
        sql = f"SELECT TOP (:{_LIMIT_PARAMETER}) {projected} FROM {source} WHERE {predicate}"
    elif dialect.name == "oracle":
        sql = f"SELECT {projected} FROM {source} WHERE {predicate} FETCH FIRST :{_LIMIT_PARAMETER} ROWS ONLY"
    else:
        sql = f"SELECT {projected} FROM {source} WHERE {predicate} LIMIT :{_LIMIT_PARAMETER}"
    return sql, {_TOKEN_PARAMETER: token, _LIMIT_PARAMETER: 2}, table.connection_id


async def lookup_rows(mapping: LookupMapping, token: str) -> list[dict]:
    """Run the bounded lookup through the existing source backpressure and retry path."""
    sql, params, connection = build_lookup_query(mapping, token)
    return await execute_split_query(sql, params, split_index=-1, connection=connection)
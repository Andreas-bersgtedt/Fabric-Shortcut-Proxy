"""
SQL dialect adapters for split-query generation (F6).

Each dialect knows how to:
  - quote identifiers (table / column names)
  - render an integer CAST for the modulo split predicate
  - apply a row limit (LIMIT suffix vs. SQL Server's TOP prefix)

The right dialect is selected from the SQLAlchemy DB URL scheme, so the same
planner code produces correct SQL for SQLite, PostgreSQL and SQL Server.
"""
from __future__ import annotations


class Dialect:
    """Generic ANSI-ish dialect (double-quoted identifiers, LIMIT suffix)."""

    name = "generic"
    int_cast_type = "INTEGER"
    quote_open = '"'
    quote_close = '"'

    def quote(self, ident: str) -> str:
        """Quote a single identifier, escaping any embedded quote char."""
        escaped = ident.replace(self.quote_close, self.quote_close * 2)
        return f"{self.quote_open}{escaped}{self.quote_close}"

    def quote_qualified(self, name: str) -> str:
        """Quote a possibly dotted identifier (e.g. ``schema.table``)."""
        return ".".join(self.quote(part) for part in name.split("."))

    def cast_int(self, expr: str) -> str:
        return f"CAST({expr} AS {self.int_cast_type})"

    def render_projection(self, column, param_prefix: str) -> tuple[str, str, dict]:
        """Render one source expression, its output reference, and bind values."""
        source = self.quote(column.source_name)
        output = self.quote(column.name)
        if column.transform:
            raise ValueError(
                f"Dialect {self.name!r} does not support column transform "
                f"{column.transform.kind!r}"
            )
        projected = source if column.source_name == column.name else f"{source} AS {output}"
        return projected, output, {}

    def build_select(
        self,
        *,
        projected: str,
        source: str,
        pk: str,
        num_splits_param: str,
        split_index_param: str,
        max_rows_param: str,
    ) -> str:
        predicate = (
            f"({self.cast_int(pk)} % :{num_splits_param}) = :{split_index_param}"
        )
        return (
            f"SELECT {projected} "
            f"FROM {source} "
            f"WHERE {predicate} "
            f"ORDER BY {pk} "
            f"LIMIT :{max_rows_param}"
        )

    def build_select_range(
        self,
        *,
        projected: str,
        source: str,
        pk: str,
        key_lo_param: str,
        key_hi_param: str,
        max_rows_param: str,
    ) -> str:
        """Range predicate (Phase 4): only this split's contiguous key slice,
        served straight off the PK index instead of a full-table modulo scan."""
        predicate = f"{pk} >= :{key_lo_param} AND {pk} < :{key_hi_param}"
        return (
            f"SELECT {projected} "
            f"FROM {source} "
            f"WHERE {predicate} "
            f"ORDER BY {pk} "
            f"LIMIT :{max_rows_param}"
        )

    def build_select_row_number(
        self,
        *,
        projected: str,
        outer_projected: str | None = None,
        source: str,
        order_by: str,
        num_splits_param: str,
        split_index_param: str,
        max_rows_param: str,
    ) -> str:
        """Fallback split strategy for non-integer keys.

        Uses ROW_NUMBER() over a stable sort key, then shards rows by
        ``(row_number - 1) % num_splits``. This keeps behavior deterministic for
        sortable non-PK keys (e.g. string/date) without integer casting.
        """
        rownum = self.quote("__row_num")
        inner = (
            f"SELECT {projected}, "
            f"ROW_NUMBER() OVER (ORDER BY {order_by}) AS {rownum} "
            f"FROM {source}"
        )
        predicate = f"(({rownum} - 1) % :{num_splits_param}) = :{split_index_param}"
        outer = outer_projected or projected
        return (
            f"SELECT {outer} "
            f"FROM ({inner}) AS q "
            f"WHERE {predicate} "
            f"ORDER BY {rownum} "
            f"LIMIT :{max_rows_param}"
        )


class SQLiteDialect(Dialect):
    name = "sqlite"


class PostgresDialect(Dialect):
    name = "postgresql"
    int_cast_type = "BIGINT"


class MSSQLDialect(Dialect):
    """SQL Server / T-SQL: bracket-quoted identifiers, TOP prefix, no LIMIT."""

    name = "mssql"
    int_cast_type = "BIGINT"
    quote_open = "["
    quote_close = "]"

    def quote(self, ident: str) -> str:
        escaped = ident.replace("]", "]]")
        return f"[{escaped}]"

    def render_projection(self, column, param_prefix: str) -> tuple[str, str, dict]:
        if not column.transform:
            return super().render_projection(column, param_prefix)

        source = self.quote(column.source_name)
        output = self.quote(column.name)
        transform = column.transform
        if transform.kind == "random_token":
            expression = (
                f"CASE WHEN {source} IS NULL THEN NULL "
                f"ELSE CONVERT(varchar(36), NEWID()) END AS {output}"
            )
            return expression, output, {}

        if transform.kind == "deterministic_hash":
            import config

            key_param = f"__token_key_{param_prefix}"
            domain_param = f"__token_domain_{param_prefix}"
            value = f"CONVERT(nvarchar(max), {source})"
            if transform.normalization in {"trim", "trim_lower"}:
                value = f"LTRIM(RTRIM({value}))"
            if transform.normalization == "trim_lower":
                value = f"LOWER({value})"
            expression = (
                f"CASE WHEN {source} IS NULL THEN NULL "
                "ELSE CONVERT(varchar(64), HASHBYTES('SHA2_256', "
                f"CONCAT(CONVERT(nvarchar(max), :{key_param}), N'|', "
                f"CONVERT(nvarchar(max), :{domain_param}), N'|', {value})), 2) "
                f"END AS {output}"
            )
            params = {
                key_param: config.resolve_tokenization_key(transform.key_ref),
                domain_param: transform.domain or column.name,
            }
            return expression, output, params

        return super().render_projection(column, param_prefix)

    def build_select(
        self,
        *,
        projected: str,
        source: str,
        pk: str,
        num_splits_param: str,
        split_index_param: str,
        max_rows_param: str,
    ) -> str:
        predicate = (
            f"({self.cast_int(pk)} % :{num_splits_param}) = :{split_index_param}"
        )
        # TOP must precede the column list; parameterised TOP needs parentheses.
        return (
            f"SELECT TOP (:{max_rows_param}) {projected} "
            f"FROM {source} "
            f"WHERE {predicate} "
            f"ORDER BY {pk}"
        )

    def build_select_range(
        self,
        *,
        projected: str,
        source: str,
        pk: str,
        key_lo_param: str,
        key_hi_param: str,
        max_rows_param: str,
    ) -> str:
        # T-SQL has no LIMIT: the range slice must use the TOP prefix too.
        predicate = f"{pk} >= :{key_lo_param} AND {pk} < :{key_hi_param}"
        return (
            f"SELECT TOP (:{max_rows_param}) {projected} "
            f"FROM {source} "
            f"WHERE {predicate} "
            f"ORDER BY {pk}"
        )

    def build_select_row_number(
        self,
        *,
        projected: str,
        outer_projected: str | None = None,
        source: str,
        order_by: str,
        num_splits_param: str,
        split_index_param: str,
        max_rows_param: str,
    ) -> str:
        rownum = self.quote("__row_num")
        inner = (
            f"SELECT {projected}, "
            f"ROW_NUMBER() OVER (ORDER BY {order_by}) AS {rownum} "
            f"FROM {source}"
        )
        predicate = f"((q.{rownum} - 1) % :{num_splits_param}) = :{split_index_param}"
        outer = outer_projected or projected
        return (
            f"SELECT TOP (:{max_rows_param}) {outer} "
            f"FROM ({inner}) AS q "
            f"WHERE {predicate} "
            f"ORDER BY q.{rownum}"
        )


class OracleDialect(Dialect):
    """Oracle SQL: quoted identifiers and FETCH FIRST row-limit syntax."""

    name = "oracle"
    int_cast_type = "NUMBER(19)"

    def build_select(
        self,
        *,
        projected: str,
        source: str,
        pk: str,
        num_splits_param: str,
        split_index_param: str,
        max_rows_param: str,
    ) -> str:
        predicate = (
            f"(MOD({self.cast_int(pk)}, :{num_splits_param})) = :{split_index_param}"
        )
        return (
            f"SELECT {projected} "
            f"FROM {source} "
            f"WHERE {predicate} "
            f"ORDER BY {pk} "
            f"FETCH FIRST :{max_rows_param} ROWS ONLY"
        )

    def build_select_range(
        self,
        *,
        projected: str,
        source: str,
        pk: str,
        key_lo_param: str,
        key_hi_param: str,
        max_rows_param: str,
    ) -> str:
        predicate = f"{pk} >= :{key_lo_param} AND {pk} < :{key_hi_param}"
        return (
            f"SELECT {projected} "
            f"FROM {source} "
            f"WHERE {predicate} "
            f"ORDER BY {pk} "
            f"FETCH FIRST :{max_rows_param} ROWS ONLY"
        )

    def build_select_row_number(
        self,
        *,
        projected: str,
        outer_projected: str | None = None,
        source: str,
        order_by: str,
        num_splits_param: str,
        split_index_param: str,
        max_rows_param: str,
    ) -> str:
        rownum = self.quote("__row_num")
        inner = (
            f"SELECT {projected}, "
            f"ROW_NUMBER() OVER (ORDER BY {order_by}) AS {rownum} "
            f"FROM {source}"
        )
        predicate = f"(MOD((q.{rownum} - 1), :{num_splits_param})) = :{split_index_param}"
        outer = outer_projected or projected
        return (
            f"SELECT {outer} "
            f"FROM ({inner}) q "
            f"WHERE {predicate} "
            f"ORDER BY q.{rownum} "
            f"FETCH FIRST :{max_rows_param} ROWS ONLY"
        )


class DatabricksDialect(Dialect):
    """Databricks SQL: Spark-style quoting and BIGINT casts."""

    name = "databricks"
    int_cast_type = "BIGINT"
    quote_open = "`"
    quote_close = "`"

    def build_select_range(
        self,
        *,
        projected: str,
        source: str,
        pk: str,
        key_lo_param: str,
        key_hi_param: str,
        max_rows_param: str,
    ) -> str:
        predicate = f"{pk} >= :{key_lo_param} AND {pk} < :{key_hi_param}"
        return (
            f"SELECT TOP (:{max_rows_param}) {projected} "
            f"FROM {source} "
            f"WHERE {predicate} "
            f"ORDER BY {pk}"
        )


_SQLITE = SQLiteDialect()
_POSTGRES = PostgresDialect()
_MSSQL = MSSQLDialect()
_ORACLE = OracleDialect()
_DATABRICKS = DatabricksDialect()
_GENERIC = Dialect()


def get_dialect(db_url: str) -> Dialect:
    """Return the dialect adapter for a SQLAlchemy DB URL."""
    scheme = db_url.lower().split("://", 1)[0]
    if "mssql" in scheme:
        return _MSSQL
    if "postgres" in scheme:
        return _POSTGRES
    if "oracle" in scheme:
        return _ORACLE
    if "databricks" in scheme:
        return _DATABRICKS
    if "sqlite" in scheme:
        return _SQLITE
    return _GENERIC

"""Source read-point contracts for per-table snapshot consistency."""
from __future__ import annotations

import math
import re
import time
import asyncio
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any, AsyncIterator, Protocol, runtime_checkable

from sqlalchemy import text

from db.capabilities import normalize_dialect


class ReadPointError(RuntimeError):
    """Base error for read-point acquisition, use, and release."""


class ReadPointUnavailable(ReadPointError):
    """The selected source cannot provide the requested read point."""


class ReadPointExpired(ReadPointError):
    """The read point is no longer valid for source reads."""


class ReadPointFenced(ReadPointError):
    """The owning generation or worker no longer owns the read point."""


def encode_boundary(value: object) -> dict[str, Any]:
    """Encode a split boundary with an explicit type tag."""
    if value is None:
        return {"type": "none", "value": None}
    if isinstance(value, bool):
        return {"type": "bool", "value": value}
    if isinstance(value, int):
        return {"type": "int", "value": value}
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("split boundary float must be finite")
        return {"type": "float", "value": value}
    if isinstance(value, Decimal):
        return {"type": "decimal", "value": format(value, "f")}
    if isinstance(value, datetime):
        return {"type": "datetime", "value": value.isoformat()}
    if isinstance(value, date):
        return {"type": "date", "value": value.isoformat()}
    if isinstance(value, str):
        return {"type": "string", "value": value}
    raise TypeError(f"unsupported split boundary type: {type(value).__name__}")


def decode_boundary(encoded: dict[str, Any]) -> object:
    """Decode a boundary produced by :func:`encode_boundary`."""
    if not isinstance(encoded, dict):
        raise TypeError("encoded split boundary must be an object")
    kind = str(encoded.get("type") or "")
    value = encoded.get("value")
    if kind == "none":
        if value is not None:
            raise ValueError("none boundary must have a null value")
        return None
    if kind == "bool":
        if not isinstance(value, bool):
            raise TypeError("bool boundary must contain a boolean")
        return value
    if kind == "int":
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError("int boundary must contain an integer")
        return value
    if kind == "float":
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise TypeError("float boundary must contain a number")
        result = float(value)
        if not math.isfinite(result):
            raise ValueError("split boundary float must be finite")
        return result
    if kind == "decimal":
        return Decimal(str(value))
    if kind == "datetime":
        return datetime.fromisoformat(str(value))
    if kind == "date":
        return date.fromisoformat(str(value))
    if kind == "string":
        if not isinstance(value, str):
            raise TypeError("string boundary must contain a string")
        return value
    raise ValueError(f"unsupported split boundary tag: {kind!r}")


@dataclass(frozen=True)
class ReadPointDescriptor:
    """Serializable identity for one table's source read point."""

    version: int
    provider: str
    connection_id: str
    table_id: str
    owner_shard: int
    generation_id: str
    generation_fence: int
    acquired_at_ms: int
    expires_at_ms: int | None
    token: str | None = field(default=None, repr=False)
    reopenable: bool = False

    def __post_init__(self) -> None:
        if self.version != 1:
            raise ValueError(
                f"unsupported read-point descriptor version: {self.version}"
            )
        for name in ("provider", "connection_id", "table_id", "generation_id"):
            if not str(getattr(self, name) or "").strip():
                raise ValueError(f"read-point descriptor {name} is required")
        if self.owner_shard < 0:
            raise ValueError("read-point owner_shard must be >= 0")
        if self.generation_fence < 0:
            raise ValueError("read-point generation_fence must be >= 0")
        if self.acquired_at_ms < 0:
            raise ValueError("read-point acquired_at_ms must be >= 0")
        if self.expires_at_ms is not None and self.expires_at_ms <= self.acquired_at_ms:
            raise ValueError("read-point expiry must be after acquisition")

    def to_dict(self) -> dict[str, Any]:
        """Return metadata safe for logs, readiness, and monitor payloads."""
        return {
            "version": self.version,
            "provider": self.provider,
            "connection_id": self.connection_id,
            "table_id": self.table_id,
            "owner_shard": self.owner_shard,
            "generation_id": self.generation_id,
            "generation_fence": self.generation_fence,
            "acquired_at_ms": self.acquired_at_ms,
            "expires_at_ms": self.expires_at_ms,
            "token_present": self.token is not None,
            "reopenable": self.reopenable,
        }

    def to_state_dict(self) -> dict[str, Any]:
        """Return generation-state data, including the provider token."""
        result = self.to_dict()
        result.pop("token_present")
        result["token"] = self.token
        return result

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "ReadPointDescriptor":
        if not isinstance(raw, dict):
            raise TypeError("read-point descriptor must be an object")
        return cls(
            version=int(raw["version"]),
            provider=str(raw["provider"]),
            connection_id=str(raw["connection_id"]),
            table_id=str(raw["table_id"]),
            owner_shard=int(raw["owner_shard"]),
            generation_id=str(raw["generation_id"]),
            generation_fence=int(raw["generation_fence"]),
            acquired_at_ms=int(raw["acquired_at_ms"]),
            expires_at_ms=(
                int(raw["expires_at_ms"])
                if raw.get("expires_at_ms") is not None
                else None
            ),
            token=(str(raw["token"]) if raw.get("token") is not None else None),
            reopenable=bool(raw.get("reopenable", False)),
        )


@dataclass(frozen=True)
class TableReadPlan:
    """One generation's immutable plan for reading a source table."""

    version: int
    table_id: str
    descriptor: ReadPointDescriptor
    split_count: int
    split_strategy: str
    split_key: str
    split_key_type: str | None
    ranges: tuple[tuple[object, object], ...] = ()

    def __post_init__(self) -> None:
        if self.version != 1:
            raise ValueError(f"unsupported table read-plan version: {self.version}")
        if not self.table_id.strip():
            raise ValueError("table read-plan table_id is required")
        if self.descriptor.table_id != self.table_id:
            raise ValueError("table read-plan and descriptor table IDs differ")
        if self.split_count < 1:
            raise ValueError("table read-plan split_count must be >= 1")
        if self.ranges and len(self.ranges) != self.split_count:
            raise ValueError("table read-plan ranges must match split_count")

    def _serialized_ranges(self) -> list[dict[str, dict[str, Any]]]:
        return [
            {"lo": encode_boundary(lo), "hi": encode_boundary(hi)}
            for lo, hi in self.ranges
        ]

    def to_dict(self) -> dict[str, Any]:
        """Return plan metadata without the provider token."""
        return {
            "version": self.version,
            "table_id": self.table_id,
            "descriptor": self.descriptor.to_dict(),
            "split_count": self.split_count,
            "split_strategy": self.split_strategy,
            "split_key": self.split_key,
            "split_key_type": self.split_key_type,
            "ranges": self._serialized_ranges(),
        }

    def to_state_dict(self) -> dict[str, Any]:
        result = self.to_dict()
        result["descriptor"] = self.descriptor.to_state_dict()
        return result

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "TableReadPlan":
        if not isinstance(raw, dict):
            raise TypeError("table read-plan must be an object")
        ranges = tuple(
            (decode_boundary(item["lo"]), decode_boundary(item["hi"]))
            for item in raw.get("ranges", [])
        )
        return cls(
            version=int(raw["version"]),
            table_id=str(raw["table_id"]),
            descriptor=ReadPointDescriptor.from_dict(raw["descriptor"]),
            split_count=int(raw["split_count"]),
            split_strategy=str(raw["split_strategy"]),
            split_key=str(raw["split_key"]),
            split_key_type=(
                str(raw["split_key_type"])
                if raw.get("split_key_type") is not None
                else None
            ),
            ranges=ranges,
        )


@runtime_checkable
class ReadSession(Protocol):
    """Query surface shared by best-effort and snapshot-aware sessions."""

    connection_id: str

    async def execute_split_query(
        self, sql: str, params: dict[str, Any], split_index: int
    ) -> list[dict[str, Any]]: ...

    def stream_split_query(
        self, sql: str, params: dict[str, Any], split_index: int, *, batch_rows: int
    ) -> AsyncIterator[list[dict[str, Any]]]: ...

    async def execute_scalar(
        self, sql: str, params: dict[str, Any] | None = None
    ) -> Any: ...

    async def fetch_column_bounds(
        self, source_table: str, key_column: str
    ) -> tuple[object, object] | None: ...

    async def fetch_table_row_count(self, source_table: str) -> int | None: ...

    async def fetch_key_bounds(
        self, source_table: str, key_column: str
    ) -> tuple[int, int] | None: ...

    async def fetch_key_quantile_bounds(
        self,
        source_table: str,
        key_column: str,
        n: int,
        *,
        sample_rows: int = 0,
        key_is_integer: bool = False,
    ) -> tuple[list[object], object] | None: ...

    async def fetch_key_histogram_bounds(
        self, source_table: str, key_column: str, n: int
    ) -> tuple[list[int], int] | None: ...

    async def close(self) -> None: ...


@runtime_checkable
class OwnedReadPoint(Protocol):
    """Owner-side resources that keep a read point alive."""

    descriptor: ReadPointDescriptor
    session: ReadSession

    async def healthy(self) -> bool: ...
    async def release(self) -> None: ...
    async def abort(self, reason: str) -> None: ...


@runtime_checkable
class ReadPointProvider(Protocol):
    """Source-specific read-point acquisition and join behavior."""

    flavor: str
    distributed: bool
    reopenable: bool

    async def validate(self, connection_id: str, table_id: str) -> None: ...

    async def acquire_owner(
        self,
        *,
        connection_id: str,
        table_id: str,
        generation_id: str,
        generation_fence: int,
        owner_shard: int,
    ) -> OwnedReadPoint: ...

    async def join(self, descriptor: ReadPointDescriptor) -> ReadSession: ...


class BestEffortReadSession:
    """Adapter that preserves the existing independent-query behavior."""

    def __init__(self, connection_id: str = "default") -> None:
        self.connection_id = connection_id

    async def execute_split_query(
        self, sql: str, params: dict[str, Any], split_index: int
    ) -> list[dict[str, Any]]:
        from db.executor import execute_split_query

        return await execute_split_query(
            sql, params, split_index, connection=self.connection_id
        )

    def stream_split_query(
        self, sql: str, params: dict[str, Any], split_index: int, *, batch_rows: int
    ) -> AsyncIterator[list[dict[str, Any]]]:
        from db.executor import stream_split_query

        return stream_split_query(
            sql,
            params,
            split_index,
            batch_rows=batch_rows,
            connection=self.connection_id,
        )

    async def execute_scalar(
        self, sql: str, params: dict[str, Any] | None = None
    ) -> Any:
        from db.executor import execute_scalar

        return await execute_scalar(sql, params, connection=self.connection_id)

    async def fetch_column_bounds(
        self, source_table: str, key_column: str
    ) -> tuple[object, object] | None:
        from db.executor import fetch_column_bounds

        return await fetch_column_bounds(
            source_table, key_column, connection=self.connection_id
        )

    async def fetch_table_row_count(self, source_table: str) -> int | None:
        from db.executor import fetch_table_row_count

        return await fetch_table_row_count(source_table, connection=self.connection_id)

    async def fetch_key_bounds(
        self, source_table: str, key_column: str
    ) -> tuple[int, int] | None:
        from db.executor import fetch_key_bounds

        return await fetch_key_bounds(
            source_table, key_column, connection=self.connection_id
        )

    async def fetch_key_quantile_bounds(
        self,
        source_table: str,
        key_column: str,
        n: int,
        *,
        sample_rows: int = 0,
        key_is_integer: bool = False,
    ) -> tuple[list[object], object] | None:
        from db.executor import fetch_key_quantile_bounds

        return await fetch_key_quantile_bounds(
            source_table,
            key_column,
            n,
            connection=self.connection_id,
            sample_rows=sample_rows,
            key_is_integer=key_is_integer,
        )

    async def fetch_key_histogram_bounds(
        self, source_table: str, key_column: str, n: int
    ) -> tuple[list[int], int] | None:
        from db.executor import fetch_key_histogram_bounds

        return await fetch_key_histogram_bounds(
            source_table, key_column, n, connection=self.connection_id
        )

    async def close(self) -> None:
        return None


class TransactionalReadSession:
    """Run planning and split reads through one source transaction."""

    def __init__(
        self,
        connection_id: str,
        connection,
        transaction,
        *,
        flavor: str,
        expires_at_ms: int | None = None,
    ) -> None:
        self.connection_id = connection_id
        self._connection = connection
        self._transaction = transaction
        self._flavor = normalize_dialect(flavor)
        self._expires_at_ms = expires_at_ms
        self._closed = False

    def _ensure_open(self) -> None:
        if self._closed:
            raise ReadPointExpired("source read session is closed")
        if (
            self._expires_at_ms is not None
            and int(time.time() * 1000) >= self._expires_at_ms
        ):
            raise ReadPointExpired("source read point expired")

    def _dialect(self):
        from db.executor import _dialect_for

        return _dialect_for(self.connection_id)

    async def execute_split_query(
        self, sql: str, params: dict[str, Any], split_index: int
    ) -> list[dict[str, Any]]:
        self._ensure_open()
        result = await self._connection.execute(text(sql), params)
        columns = list(result.keys())
        return [dict(zip(columns, row)) for row in result.fetchall()]

    async def _stream(
        self, sql: str, params: dict[str, Any], batch_rows: int
    ) -> AsyncIterator[list[dict[str, Any]]]:
        self._ensure_open()
        result = await self._connection.stream(text(sql), params)
        columns = list(result.keys())
        async for partition in result.partitions(batch_rows):
            yield [dict(zip(columns, row)) for row in partition]

    def stream_split_query(
        self, sql: str, params: dict[str, Any], split_index: int, *, batch_rows: int
    ) -> AsyncIterator[list[dict[str, Any]]]:
        return self._stream(sql, params, batch_rows)

    async def execute_scalar(
        self, sql: str, params: dict[str, Any] | None = None
    ) -> Any:
        self._ensure_open()
        return (await self._connection.execute(text(sql), params or {})).scalar()

    async def fetch_column_bounds(
        self, source_table: str, key_column: str
    ) -> tuple[object, object] | None:
        self._ensure_open()
        dialect = self._dialect()
        source = dialect.quote_qualified(source_table)
        key = dialect.quote(key_column)
        row = (
            await self._connection.execute(
                text(f"SELECT MIN({key}) AS lo, MAX({key}) AS hi FROM {source}")
            )
        ).first()
        if row is None or row[0] is None or row[1] is None:
            return None
        return row[0], row[1]

    async def fetch_table_row_count(self, source_table: str) -> int | None:
        self._ensure_open()
        source = self._dialect().quote_qualified(source_table)
        row = (
            await self._connection.execute(
                text(f"SELECT COUNT(*) AS n FROM {source}")
            )
        ).first()
        if row is None or row[0] is None:
            return None
        try:
            return int(row[0])
        except (TypeError, ValueError):
            return None

    async def fetch_key_bounds(
        self, source_table: str, key_column: str
    ) -> tuple[int, int] | None:
        bounds = await self.fetch_column_bounds(source_table, key_column)
        if bounds is None:
            return None
        try:
            return int(bounds[0]), int(bounds[1])
        except (TypeError, ValueError):
            return None

    async def fetch_key_quantile_bounds(
        self,
        source_table: str,
        key_column: str,
        n: int,
        *,
        sample_rows: int = 0,
        key_is_integer: bool = False,
    ) -> tuple[list[object], object] | None:
        if n < 1:
            return None
        dialect = self._dialect()
        source = dialect.quote_qualified(source_table)
        key = dialect.quote(key_column)
        ntile_source = source
        if sample_rows > 0 and key_is_integer:
            total = await self.fetch_table_row_count(source_table)
            if total and total > sample_rows:
                stride = (total + sample_rows - 1) // sample_rows
                if stride >= 2:
                    ntile_source = (
                        f"(SELECT {key} FROM {source} WHERE {key} IS NOT NULL "
                        f"AND (ABS({key}) % {int(stride)}) = 0) "
                        "fsp_ntile_sample"
                    )
        sql = (
            f"SELECT MIN({key}) AS lo, MAX({key}) AS hi "
            f"FROM (SELECT {key}, NTILE({int(n)}) OVER (ORDER BY {key}) "
            "AS fsp_ntile_bucket "
            f"FROM {ntile_source} WHERE {key} IS NOT NULL) q "
            "GROUP BY fsp_ntile_bucket ORDER BY fsp_ntile_bucket"
        )
        rows = (await self._connection.execute(text(sql))).all()
        if not rows:
            return None
        mins = [row[0] for row in rows]
        overall_max = rows[-1][1]
        if sample_rows > 0 and key_is_integer and ntile_source != source:
            full = await self.fetch_column_bounds(source_table, key_column)
            if full is not None:
                mins[0], overall_max = full
        if overall_max is None or any(value is None for value in mins):
            return None
        return mins, overall_max

    async def fetch_key_histogram_bounds(
        self, source_table: str, key_column: str, n: int
    ) -> tuple[list[int], int] | None:
        if n < 1:
            return None
        from planner.split_planner import (
            mins_from_equidepth,
            mins_from_histogram_steps,
        )

        if self._flavor == "mssql":
            sql = (
                "WITH pick AS ("
                " SELECT TOP 1 s.object_id AS oid, s.stats_id AS sid"
                " FROM sys.stats s"
                " JOIN sys.stats_columns sc ON sc.object_id=s.object_id"
                " AND sc.stats_id=s.stats_id AND sc.stats_column_id=1"
                " JOIN sys.columns c ON c.object_id=s.object_id"
                " AND c.column_id=sc.column_id"
                " WHERE s.object_id = OBJECT_ID(:tbl) AND c.name = :col"
                " ORDER BY s.stats_id)"
                " SELECT CAST(h.range_high_key AS BIGINT) AS hi,"
                " (h.range_rows + h.equal_rows) AS rows_"
                " FROM pick CROSS APPLY"
                " sys.dm_db_stats_histogram(pick.oid, pick.sid) h"
                " ORDER BY h.step_number"
            )
            rows = (
                await self._connection.execute(
                    text(sql), {"tbl": source_table, "col": key_column}
                )
            ).all()
            steps = [
                (int(row[0]), float(row[1] or 0))
                for row in rows
                if row[0] is not None
            ]
            mins = mins_from_histogram_steps(steps, n)
            if not mins:
                return None
            return [int(value) for value in mins], int(steps[-1][0])
        if self._flavor == "postgresql":
            schema, separator, table_name = source_table.rpartition(".")
            if separator:
                sql = (
                    "SELECT histogram_bounds::text FROM pg_stats "
                    "WHERE schemaname=:schema AND tablename=:table AND attname=:column"
                )
                params = {
                    "schema": schema,
                    "table": table_name,
                    "column": key_column,
                }
            else:
                sql = (
                    "SELECT histogram_bounds::text FROM pg_stats "
                    "WHERE tablename=:table AND attname=:column "
                    "ORDER BY schemaname LIMIT 1"
                )
                params = {"table": source_table, "column": key_column}
            rows = (await self._connection.execute(text(sql), params)).all()
            if not rows or rows[0][0] is None:
                return None
            inner = str(rows[0][0]).strip().lstrip("{").rstrip("}")
            bounds = [
                int(float(value))
                for value in inner.split(",")
                if value.strip()
            ]
            mins = mins_from_equidepth(bounds, n)
            if not mins:
                return None
            return [int(value) for value in mins], int(bounds[-1])
        return None

    async def close(self, *, commit: bool = True) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            if self._transaction.is_active:
                if commit:
                    await self._transaction.commit()
                else:
                    await self._transaction.rollback()
        finally:
            await self._connection.close()


class _TransactionOwnedReadPoint:
    def __init__(self, descriptor: ReadPointDescriptor, session: TransactionalReadSession):
        self.descriptor = descriptor
        self.session = session
        self._finished = False

    async def healthy(self) -> bool:
        if self._finished:
            return False
        try:
            return (await self.session.execute_scalar("SELECT 1")) == 1
        except Exception:
            return False

    async def release(self) -> None:
        if self._finished:
            return
        self._finished = True
        await self.session.close(commit=True)

    async def abort(self, reason: str) -> None:
        if self._finished:
            return
        self._finished = True
        await self.session.close(commit=False)


class SqlServerSnapshotProvider:
    flavor = "mssql"
    distributed = False
    reopenable = False

    async def validate(self, connection_id: str, table_id: str) -> None:
        session = BestEffortReadSession(connection_id)
        state = await session.execute_scalar(
            "SELECT snapshot_isolation_state FROM sys.databases WHERE name = DB_NAME()"
        )
        if int(state or 0) != 1:
            raise ReadPointUnavailable(
                "SQL Server snapshot isolation is disabled. Run "
                "ALTER DATABASE [your_database] SET ALLOW_SNAPSHOT_ISOLATION ON."
            )

    async def acquire_owner(
        self,
        *,
        connection_id: str,
        table_id: str,
        generation_id: str,
        generation_fence: int,
        owner_shard: int,
    ) -> OwnedReadPoint:
        await self.validate(connection_id, table_id)
        from db.executor import _engine_for

        connection = await _engine_for(connection_id).connect()
        try:
            connection = await connection.execution_options(
                isolation_level="SNAPSHOT"
            )
            transaction = await connection.begin()
        except Exception:
            await connection.close()
            raise
        import config

        acquired_at_ms = int(time.time() * 1000)
        expires_at_ms = (
            acquired_at_ms + config.SNAPSHOT_MAX_LIFETIME_SECONDS * 1000
        )
        session = TransactionalReadSession(
            connection_id,
            connection,
            transaction,
            flavor=self.flavor,
            expires_at_ms=expires_at_ms,
        )
        descriptor = ReadPointDescriptor(
            version=1,
            provider="mssql_transaction",
            connection_id=connection_id,
            table_id=table_id,
            owner_shard=owner_shard,
            generation_id=generation_id,
            generation_fence=generation_fence,
            acquired_at_ms=acquired_at_ms,
            expires_at_ms=expires_at_ms,
            token=None,
            reopenable=False,
        )
        return _TransactionOwnedReadPoint(descriptor, session)

    async def join(self, descriptor: ReadPointDescriptor) -> ReadSession:
        raise ReadPointUnavailable(
            "SQL Server snapshot transactions are owner-only and cannot be joined"
        )


_POSTGRES_SNAPSHOT_ID = re.compile(r"^[0-9A-Fa-f-]+$")


class PostgresSnapshotProvider:
    flavor = "postgresql"
    distributed = True
    reopenable = False

    async def validate(self, connection_id: str, table_id: str) -> None:
        from db.executor import _async_mode_for

        if not _async_mode_for(connection_id):
            raise ReadPointUnavailable(
                "PostgreSQL snapshot mode requires an async PostgreSQL connection"
            )

    async def _open_session(
        self, connection_id: str, *, expires_at_ms: int | None = None
    ) -> tuple[object, object, TransactionalReadSession]:
        from db.executor import _engine_for

        connection = await _engine_for(connection_id).connect()
        try:
            connection = await connection.execution_options(
                isolation_level="REPEATABLE READ"
            )
            transaction = await connection.begin()
            await connection.exec_driver_sql("SET TRANSACTION READ ONLY")
        except Exception:
            await connection.close()
            raise
        return connection, transaction, TransactionalReadSession(
            connection_id,
            connection,
            transaction,
            flavor=self.flavor,
            expires_at_ms=expires_at_ms,
        )

    async def acquire_owner(
        self,
        *,
        connection_id: str,
        table_id: str,
        generation_id: str,
        generation_fence: int,
        owner_shard: int,
    ) -> OwnedReadPoint:
        await self.validate(connection_id, table_id)
        import config

        acquired_at_ms = int(time.time() * 1000)
        expires_at_ms = (
            acquired_at_ms + config.SNAPSHOT_MAX_LIFETIME_SECONDS * 1000
        )
        _, _, session = await self._open_session(
            connection_id, expires_at_ms=expires_at_ms
        )
        try:
            token = str(await session.execute_scalar("SELECT pg_export_snapshot()"))
            if not _POSTGRES_SNAPSHOT_ID.fullmatch(token):
                raise ReadPointError("PostgreSQL returned an invalid snapshot identifier")
        except Exception:
            await session.close(commit=False)
            raise
        descriptor = ReadPointDescriptor(
            version=1,
            provider="postgresql_exported",
            connection_id=connection_id,
            table_id=table_id,
            owner_shard=owner_shard,
            generation_id=generation_id,
            generation_fence=generation_fence,
            acquired_at_ms=acquired_at_ms,
            expires_at_ms=expires_at_ms,
            token=token,
            reopenable=False,
        )
        return _TransactionOwnedReadPoint(descriptor, session)

    async def _join_transaction(
        self, descriptor: ReadPointDescriptor
    ) -> TransactionalReadSession:
        if descriptor.provider != "postgresql_exported":
            raise ReadPointUnavailable(
                f"cannot join PostgreSQL provider {descriptor.provider!r}"
            )
        token = str(descriptor.token or "")
        if not _POSTGRES_SNAPSHOT_ID.fullmatch(token):
            raise ReadPointUnavailable("PostgreSQL snapshot identifier is invalid")
        _, _, session = await self._open_session(
            descriptor.connection_id,
            expires_at_ms=descriptor.expires_at_ms,
        )
        try:
            await session._connection.exec_driver_sql(
                f"SET TRANSACTION SNAPSHOT '{token}'"
            )
        except Exception:
            await session.close(commit=False)
            raise
        return session

    async def join(self, descriptor: ReadPointDescriptor) -> ReadSession:
        session = await self._join_transaction(descriptor)
        return _PostgresJoinedReadSession(self, descriptor, session)


class _PostgresJoinedReadSession:
    """Rejoin one exported snapshot after a pre-yield worker connection failure."""

    def __init__(
        self,
        provider: PostgresSnapshotProvider,
        descriptor: ReadPointDescriptor,
        session: TransactionalReadSession,
    ) -> None:
        self.connection_id = descriptor.connection_id
        self._provider = provider
        self._descriptor = descriptor
        self._session = session

    async def _replace(self) -> None:
        await self._session.close(commit=False)
        self._session = await self._provider._join_transaction(self._descriptor)

    async def execute_split_query(
        self, sql: str, params: dict[str, Any], split_index: int
    ) -> list[dict[str, Any]]:
        from db.executor import _max_retries_for, _retry_backoff_for

        retries = _max_retries_for(self.connection_id)
        backoff = _retry_backoff_for(self.connection_id)
        for attempt in range(retries + 1):
            try:
                return await self._session.execute_split_query(
                    sql, params, split_index
                )
            except ReadPointExpired:
                raise
            except Exception:
                if attempt >= retries:
                    raise
                await asyncio.sleep(backoff * (attempt + 1))
                await self._replace()
        raise AssertionError("unreachable")

    async def _stream(
        self,
        sql: str,
        params: dict[str, Any],
        split_index: int,
        batch_rows: int,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        from db.executor import _max_retries_for, _retry_backoff_for

        retries = _max_retries_for(self.connection_id)
        backoff = _retry_backoff_for(self.connection_id)
        for attempt in range(retries + 1):
            yielded = False
            try:
                async for batch in self._session.stream_split_query(
                    sql, params, split_index, batch_rows=batch_rows
                ):
                    yielded = True
                    yield batch
                return
            except ReadPointExpired:
                raise
            except Exception:
                if yielded or attempt >= retries:
                    raise
                await asyncio.sleep(backoff * (attempt + 1))
                await self._replace()

    def stream_split_query(
        self, sql: str, params: dict[str, Any], split_index: int, *, batch_rows: int
    ) -> AsyncIterator[list[dict[str, Any]]]:
        return self._stream(sql, params, split_index, batch_rows)

    async def execute_scalar(
        self, sql: str, params: dict[str, Any] | None = None
    ) -> Any:
        return await self._session.execute_scalar(sql, params)

    async def fetch_column_bounds(self, source_table: str, key_column: str):
        return await self._session.fetch_column_bounds(source_table, key_column)

    async def fetch_table_row_count(self, source_table: str):
        return await self._session.fetch_table_row_count(source_table)

    async def fetch_key_bounds(self, source_table: str, key_column: str):
        return await self._session.fetch_key_bounds(source_table, key_column)

    async def fetch_key_quantile_bounds(
        self,
        source_table: str,
        key_column: str,
        n: int,
        *,
        sample_rows: int = 0,
        key_is_integer: bool = False,
    ):
        return await self._session.fetch_key_quantile_bounds(
            source_table,
            key_column,
            n,
            sample_rows=sample_rows,
            key_is_integer=key_is_integer,
        )

    async def fetch_key_histogram_bounds(
        self, source_table: str, key_column: str, n: int
    ):
        return await self._session.fetch_key_histogram_bounds(
            source_table, key_column, n
        )

    async def close(self) -> None:
        await self._session.close()


_PROVIDERS: dict[str, ReadPointProvider] = {}


def register_provider(provider: ReadPointProvider, *, replace: bool = False) -> None:
    """Register one source provider by normalized flavor."""
    flavor = normalize_dialect(str(provider.flavor or ""))
    if not flavor:
        raise ValueError("read-point provider flavor is required")
    if flavor in _PROVIDERS and not replace:
        raise ValueError(f"read-point provider already registered: {flavor}")
    _PROVIDERS[flavor] = provider


def get_provider(flavor: str | None) -> ReadPointProvider | None:
    return _PROVIDERS.get(normalize_dialect(flavor))


def require_provider(flavor: str | None) -> ReadPointProvider:
    provider = get_provider(flavor)
    if provider is None:
        raise ReadPointUnavailable(
            f"source flavor {normalize_dialect(flavor)!r} has no snapshot provider"
        )
    return provider


def registered_providers() -> tuple[str, ...]:
    return tuple(sorted(_PROVIDERS))


def clear_provider_registry() -> None:
    """Clear provider registrations. Intended for isolated tests and reloads."""
    _PROVIDERS.clear()


def register_builtin_providers() -> None:
    """Register implemented source providers without replacing test overrides."""
    for provider in (SqlServerSnapshotProvider(), PostgresSnapshotProvider()):
        flavor = normalize_dialect(provider.flavor)
        if flavor not in _PROVIDERS:
            _PROVIDERS[flavor] = provider


register_builtin_providers()

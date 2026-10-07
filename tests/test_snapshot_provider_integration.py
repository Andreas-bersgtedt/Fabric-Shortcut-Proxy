from __future__ import annotations

import os
import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from fabric_shortcut_proxy import config
import fabric_shortcut_proxy.db.executor as executor
from fabric_shortcut_proxy.db.read_points import PostgresSnapshotProvider, SqlServerSnapshotProvider


async def _configure_default(monkeypatch, url):
    await executor.dispose_engines()
    monkeypatch.setattr(config, "DB_URL", url)
    executor._engine = None
    executor._sync_engine = None


async def test_postgres_exported_snapshot_is_stable_during_mutation(monkeypatch):
    url = os.environ.get("SNAPSHOT_POSTGRES_URL")
    if not url:
        pytest.skip("SNAPSHOT_POSTGRES_URL is not configured")
    await _configure_default(monkeypatch, url)
    table = f"fsp_snapshot_{uuid.uuid4().hex[:12]}"
    engine = create_async_engine(url)
    try:
        async with engine.begin() as connection:
            await connection.execute(text(f'CREATE TABLE "{table}" (id BIGINT PRIMARY KEY)'))
            await connection.execute(text(f'INSERT INTO "{table}" (id) VALUES (1)'))

        provider = PostgresSnapshotProvider()
        owned = await provider.acquire_owner(
            connection_id="default",
            table_id=f"default::{table}",
            generation_id="integration-generation",
            generation_fence=1,
            owner_shard=0,
        )
        assert await owned.session.execute_scalar(
            f'SELECT COUNT(*) FROM "{table}"'
        ) == 1
        joined = await provider.join(owned.descriptor)

        async with engine.begin() as connection:
            await connection.execute(text(f'INSERT INTO "{table}" (id) VALUES (2)'))

        assert await owned.session.execute_scalar(
            f'SELECT COUNT(*) FROM "{table}"'
        ) == 1
        assert await joined.execute_scalar(f'SELECT COUNT(*) FROM "{table}"') == 1
        await joined.close()
        await owned.release()
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP TABLE IF EXISTS "{table}"'))
        await engine.dispose()
        await executor.dispose_engines()


async def test_sql_server_snapshot_transaction_is_stable_during_mutation(monkeypatch):
    url = os.environ.get("SNAPSHOT_MSSQL_URL")
    if not url:
        pytest.skip("SNAPSHOT_MSSQL_URL is not configured")
    await _configure_default(monkeypatch, url)
    table = f"fsp_snapshot_{uuid.uuid4().hex[:12]}"
    engine = create_async_engine(url)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(f"CREATE TABLE [dbo].[{table}] ([id] BIGINT NOT NULL PRIMARY KEY)")
            )
            await connection.execute(
                text(f"INSERT INTO [dbo].[{table}] ([id]) VALUES (1)")
            )

        provider = SqlServerSnapshotProvider()
        owned = await provider.acquire_owner(
            connection_id="default",
            table_id=f"default::dbo.{table}",
            generation_id="integration-generation",
            generation_fence=1,
            owner_shard=0,
        )
        assert await owned.session.execute_scalar(
            f"SELECT COUNT(*) FROM [dbo].[{table}]"
        ) == 1

        async with engine.begin() as connection:
            await connection.execute(
                text(f"INSERT INTO [dbo].[{table}] ([id]) VALUES (2)")
            )

        assert await owned.session.execute_scalar(
            f"SELECT COUNT(*) FROM [dbo].[{table}]"
        ) == 1
        await owned.release()
    finally:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "IF OBJECT_ID(:name, 'U') IS NOT NULL "
                    f"DROP TABLE [dbo].[{table}]"
                ),
                {"name": f"dbo.{table}"},
            )
        await engine.dispose()
        await executor.dispose_engines()

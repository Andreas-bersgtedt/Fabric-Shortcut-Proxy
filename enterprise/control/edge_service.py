"""Single-process experimental edge refresh and replay service."""

from __future__ import annotations

import argparse
import asyncio
from contextlib import ExitStack, contextmanager
import math
import os
from pathlib import Path
import sqlite3
from typing import BinaryIO, Iterable, Iterator

import httpx
from sqlalchemy.exc import SQLAlchemyError

from enterprise.control.edge_client import EdgeReplayClient
from enterprise.control.edge_journal import EdgeJournal
from enterprise.control.edge_manager import EdgeManager
from fabric_shortcut_proxy import config
from fabric_shortcut_proxy.observability.logging import configure_logging, get_logger
from fabric_shortcut_proxy.runtime.artifact_store import LocalDirStore, ObjectStat

log = get_logger(__name__)


class EdgeStorageLimitExceeded(RuntimeError):
    pass


class BoundedEdgeStore(LocalDirStore):
    """Bound immutable snapshot objects written by the exclusive edge service."""

    def __init__(self, root: str, max_bytes: int):
        if isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or max_bytes < 1:
            raise ValueError("edge store limit must be a positive integer")
        super().__init__(root)
        self.max_bytes = max_bytes

    def put_stream_if_absent(
        self, key: str, data: BinaryIO | Iterable[bytes], *, length: int | None = None
    ) -> ObjectStat:
        if length is None or length < 0:
            raise ValueError("edge writes require a known non-negative length")
        if self.head(key) is None:
            size = sum(item.size for item in self.list())
            if size + length > self.max_bytes:
                raise EdgeStorageLimitExceeded("edge artifact store limit reached")
        return super().put_stream_if_absent(key, data, length=length)


@contextmanager
def _process_lock(path: Path) -> Iterator[None]:
    db = sqlite3.connect(path, timeout=0, isolation_level=None)
    try:
        try:
            db.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError as exc:
            raise RuntimeError(f"edge service lock unavailable: {path}") from exc
        yield
    finally:
        db.close()


async def _wait(stop: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        pass


async def run_edge_service(
    manager: EdgeManager,
    replay: EdgeReplayClient,
    *,
    refresh_seconds: float,
    retry_seconds: float,
    max_pending: int,
    stop: asyncio.Event,
) -> None:
    if any(not math.isfinite(value) or value <= 0 for value in (refresh_seconds, retry_seconds)):
        raise ValueError("edge intervals must be finite and positive")
    if isinstance(max_pending, bool) or not isinstance(max_pending, int) or max_pending < 1:
        raise ValueError("edge pending limit must be a positive integer")

    async def refresh_loop() -> None:
        while not stop.is_set():
            if await asyncio.to_thread(manager.journal.pending_count) >= max_pending:
                log.warning("edge_pending_limit_reached", limit=max_pending)
            else:
                try:
                    sequence = await manager.refresh_once()
                    log.info("edge_refresh_journaled", sequence=sequence)
                except (EdgeStorageLimitExceeded, OSError, SQLAlchemyError) as exc:
                    log.warning("edge_refresh_failed", error=str(exc))
            await _wait(stop, refresh_seconds)

    async def replay_loop() -> None:
        while not stop.is_set():
            try:
                sent = await replay.replay_once(manager.journal, manager.store)
                if sent:
                    log.info("edge_generation_acknowledged")
                    continue
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code not in {409, 429, 500, 502, 503, 504}:
                    raise
                log.warning(
                    "edge_replay_rejected", status=exc.response.status_code,
                    detail=exc.response.text[:256],
                )
            except httpx.RequestError as exc:
                log.warning("edge_replay_unavailable", error=type(exc).__name__)
            await _wait(stop, retry_seconds)

    async with asyncio.TaskGroup() as group:
        group.create_task(refresh_loop())
        group.create_task(replay_loop())


def main() -> None:
    parser = argparse.ArgumentParser(description="Run an experimental single-dataset edge Manager")
    parser.add_argument("--table", required=True)
    parser.add_argument("--journal", required=True)
    parser.add_argument("--store-dir", required=True)
    parser.add_argument("--manager-url", required=True)
    parser.add_argument("--identity", required=True)
    parser.add_argument("--refresh-seconds", type=float, default=30)
    parser.add_argument("--retry-seconds", type=float, default=5)
    parser.add_argument("--max-pending", type=int, default=16)
    parser.add_argument("--max-store-bytes", type=int, required=True)
    args = parser.parse_args()
    tables = [table for table in config.TABLES if table.enabled and table.name == args.table]
    if len(tables) != 1:
        raise ValueError("edge table must identify one enabled configured table")
    journal_path = Path(args.journal).resolve()
    store_path = Path(args.store_dir).resolve()
    if journal_path.is_relative_to(store_path):
        raise ValueError("edge journal must be outside its artifact store")
    if not journal_path.parent.is_dir():
        raise ValueError("edge journal parent directory must exist")
    os.makedirs(store_path, exist_ok=True)
    configure_logging()
    with ExitStack() as locks:
        locks.enter_context(_process_lock(journal_path.with_name(journal_path.name + ".worker-lock")))
        locks.enter_context(_process_lock(store_path / ".fsp-edge-worker-lock.sqlite"))
        table = tables[0]
        journal = EdgeJournal(journal_path, dataset_id=f"{table.connection_id}::{table.source_table}")
        store = BoundedEdgeStore(str(store_path), args.max_store_bytes)
        manager = EdgeManager(table, journal, store)
        replay = EdgeReplayClient(args.manager_url, args.identity)

        async def run() -> None:
            try:
                await run_edge_service(
                    manager, replay, refresh_seconds=args.refresh_seconds,
                    retry_seconds=args.retry_seconds, max_pending=args.max_pending,
                    stop=asyncio.Event(),
                )
            finally:
                await replay.aclose()
                from fabric_shortcut_proxy.db import executor

                await executor.dispose_engines()

        asyncio.run(run())


if __name__ == "__main__":
    main()

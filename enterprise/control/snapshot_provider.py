"""Durable published snapshot lookup for the Manager control service."""
from __future__ import annotations

from enterprise.control.contract import SnapshotManifest
from enterprise.control.work_queue import DurableWorkQueue


class DurableSnapshotProvider:
    def __init__(self, queue: DurableWorkQueue) -> None:
        self.queue = queue

    def __call__(self, table: str, epoch: int) -> SnapshotManifest | None:
        return self.queue.get_snapshot(table, epoch)

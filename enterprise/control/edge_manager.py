"""Local refresh and journaling for a single edge dataset."""

from __future__ import annotations

import asyncio
from contextlib import closing
import hashlib
import math
import time
import uuid

from enterprise.control.contract import SnapshotManifest, SplitRef
from enterprise.control.edge_journal import EdgeJournal, EdgeMetadata
from enterprise.control.materialize_service import _publish_snapshot_objects
from fabric_shortcut_proxy import config
from fabric_shortcut_proxy.cache import lru_cache as cache
from fabric_shortcut_proxy.iceberg import freshness
from fabric_shortcut_proxy.runtime.artifact_store import LocalDirStore


class EdgeManager:
    """Materialize one dataset locally; regional publication is separate."""

    def __init__(
        self, table: config.TableDef, journal: EdgeJournal, store: LocalDirStore
    ) -> None:
        if journal.dataset_id != f"{table.connection_id}::{table.source_table}":
            raise ValueError("table does not match the edge journal dataset")
        if not table.schema or table.num_splits != 1:
            raise ValueError("edge refresh currently requires a schema and one split")
        if table.effective_split_strategy != "modulo":
            raise ValueError("edge refresh currently requires modulo splitting")
        if not isinstance(store, LocalDirStore):
            raise ValueError("edge refresh requires a local artifact store")
        if config.TABLE_FORMAT != "iceberg":
            raise ValueError("edge refresh currently supports only Iceberg")
        if config.ARTIFACT_STORE_SERVING or config.PARQUET_DISK_CACHE:
            raise ValueError("edge refresh requires external cache write-through to be disabled")
        if config.PIN_MATERIALIZED_SPLITS:
            raise ValueError("edge refresh requires PIN_MATERIALIZED_SPLITS to be disabled")
        self.table = table
        self.journal = journal
        self.store = store
        self._lock = asyncio.Lock()

    def _metadata_digests(self, keys: list[str]) -> tuple[EdgeMetadata, ...]:
        result = []
        for key in keys:
            stat = self.store.head(key)
            if stat is None:
                raise RuntimeError(f"local metadata is missing: {key}")
            digest = hashlib.sha256()
            size = 0
            with closing(self.store.get_stream(key)) as stream:
                for chunk in stream:
                    digest.update(chunk)
                    size += len(chunk)
            if size != stat.size:
                raise RuntimeError(f"local metadata size changed: {key}")
            result.append(EdgeMetadata(key, size, digest.hexdigest()))
        return tuple(result)

    async def refresh_once(self) -> int:
        """Write one complete local snapshot before adding it to the journal."""
        async with self._lock:
            snap = await freshness.materialize_table(
                self.table, config.BUCKET_NAME, config.WAREHOUSE_PREFIX
            )
            generation_id = uuid.uuid4().hex
            split_data: dict[str, bytes] = {}
            refs = []
            for split in snap.splits:
                data = cache.peek_parquet(split.object_key)
                if data is None or split.record_count is None:
                    raise RuntimeError(f"materialized split is unavailable: {split.object_key}")
                split.object_key = (
                    f"{snap.table_path}/.fsp/edge-staging/{generation_id}/"
                    f"part-{split.split_index}.parquet"
                )
                split.file_size_in_bytes = len(data)
                split.content_hash = hashlib.sha256(data).hexdigest()
                split_data[split.object_key] = data
                refs.append(
                    SplitRef(
                        object_key=split.object_key,
                        size_bytes=len(data),
                        record_count=split.record_count,
                        content_hash=split.content_hash,
                    )
                )
            written, metadata_keys = await asyncio.to_thread(
                _publish_snapshot_objects,
                snap,
                generation_id,
                target_store=self.store,
                split_data=split_data,
            )
            if written != len(refs) + 4 or len(metadata_keys) != 4:
                raise RuntimeError("local snapshot is incomplete")
            for ref in refs:
                if not await asyncio.to_thread(
                    self.store.verify, ref.object_key,
                    size=ref.size_bytes, content_hash=ref.content_hash,
                ):
                    raise RuntimeError(f"local split verification failed: {ref.object_key}")
            metadata = await asyncio.to_thread(self._metadata_digests, metadata_keys)
            manifest = SnapshotManifest(
                table=self.table.name,
                epoch=snap.snapshot_id,
                table_format="iceberg",
                splits=refs,
                metadata_keys=metadata_keys,
                generation_id=generation_id,
                dataset_id=self.journal.dataset_id,
                produced_at_ms=time.time_ns() // 1_000_000,
                request_id=generation_id,
            )
            return await asyncio.to_thread(
                self.journal.append, manifest, metadata=metadata
            )

    async def run(self, interval_seconds: float, stop: asyncio.Event) -> None:
        if (
            isinstance(interval_seconds, bool)
            or not math.isfinite(interval_seconds)
            or interval_seconds <= 0
        ):
            raise ValueError("refresh interval must be finite and positive")
        while not stop.is_set():
            await self.refresh_once()
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval_seconds)
            except asyncio.TimeoutError:
                continue

"""
Data freshness — content-addressed snapshots + background poller (P1/P2).

Uniform, source-agnostic, read-only. When ``AUTO_REFRESH`` is enabled the proxy
periodically re-reads each table, hashes the *logical row content* of every chunk
(NOT the parquet bytes — those are nondeterministic), names each chunk file by
its content hash, and publishes a new Iceberg snapshot **only when content
actually changed**. Fabric then sees a new ``current-snapshot-id`` + new
data-file paths and re-reads.

Change detection is a probe-first cascade (``REFRESH_STRATEGY=auto``):
``dialect_probe`` → manual → (optional) full content read. Everything is wrapped
so a probe/query failure can never crash the poller.
"""
from __future__ import annotations

import asyncio
import hashlib
import time

import config
import cache.lru_cache as cache
import iceberg.state_store as state_store
from iceberg.state_store import SnapshotState, SplitDescriptor
from planner.split_planner import build_split_query
from db.executor import execute_scalar, execute_split_query
from parquet.generator import rows_to_parquet
from iceberg.stats import collect_split_stats
from observability.logging import get_logger

log = get_logger(__name__)

_FIELD_SEP = b"\x1f"
_RECORD_SEP = b"\x1e"

# Per-table change-detection state.
_probe_tokens: dict[str, str | None] = {}
_ttl_gen: dict[str, int] = {}
_poller_task: asyncio.Task | None = None


# ---------------------------------------------------------------------------
# Content hashing (deterministic over row values, not parquet bytes)
# ---------------------------------------------------------------------------

def _rows_hash(rows: list[dict], columns) -> str:
    """A stable 12-hex digest of a chunk's rows, in query order.

    Rows arrive ordered by the split key, so the digest is deterministic for a
    given content and changes iff any value changes.
    """
    h = hashlib.sha256()
    names = [c.name for c in columns]
    for row in rows:
        for n in names:
            v = row.get(n)
            if v is None:
                h.update(b"\x00")
            elif isinstance(v, (bytes, bytearray)):
                h.update(bytes(v))
            else:
                h.update(str(v).encode("utf-8", "replace"))
            h.update(_FIELD_SEP)
        h.update(_RECORD_SEP)
    return h.hexdigest()[:12]


# ---------------------------------------------------------------------------
# Materialize + publish
# ---------------------------------------------------------------------------

async def materialize_table(table, bucket: str, warehouse_prefix: str) -> SnapshotState:
    """Read every chunk, content-address it, and build a candidate snapshot.

    Caches the generated parquet under each content-addressed key. The snapshot
    id is derived from the chunk hashes, so identical content yields identical
    ids (restart-stable) and any change yields a new id.
    """
    table_path = f"{warehouse_prefix}/{table.name}"
    splits: list[SplitDescriptor] = []
    chunk_hashes: list[str] = []
    total_records = 0

    for i in range(table.num_splits):
        probe_split = SplitDescriptor(
            split_index=i, num_splits=table.num_splits,
            object_key="", watermark_ms=0, table=table,
        )
        sql, params = build_split_query(probe_split)
        rows = await execute_split_query(sql, params, split_index=i)
        chash = _rows_hash(rows, table.schema)
        object_key = f"{table_path}/data/split-{i}-{chash}.parquet"
        # Content-addressed => IMMUTABLE. If this exact content was already
        # materialized, REUSE the pinned bytes verbatim. Parquet output is
        # nondeterministic, so regenerating would change the file size while
        # publish() dedupes the (unchanged) snapshot — leaving the manifest
        # declaring the OLD size and Fabric reading a differently-sized file
        # (SQL msg 19778 "file size defined in metadata is smaller than actual" /
        # READ_EXCEPTION). First materialization of a given content wins.
        existing = cache.peek_parquet(object_key)
        if existing is not None:
            pq_bytes = existing
        else:
            pq_bytes = rows_to_parquet(rows, split_index=i, columns=table.schema)
            if config.PIN_MATERIALIZED_SPLITS:
                cache.pin_parquet(object_key, pq_bytes)
            else:
                cache.put_parquet(object_key, pq_bytes)
        stats = collect_split_stats(pq_bytes, table.schema) if config.ICEBERG_MANIFEST_STATS else None
        splits.append(SplitDescriptor(
            split_index=i, num_splits=table.num_splits, object_key=object_key,
            watermark_ms=0, table=table, record_count=len(rows),
            file_size_in_bytes=len(pq_bytes), stats=stats,
        ))
        chunk_hashes.append(chash)
        total_records += len(rows)

    seed = f"{bucket}/{table_path}:" + "|".join(f"{i}:{h}" for i, h in enumerate(chunk_hashes))
    digest = hashlib.sha256(seed.encode()).hexdigest()
    snap_id = int(digest[:15], 16)
    snap_uuid = digest[:32]
    watermark = 1_700_000_000_000 + (int(digest[15:25], 16) % 86_400_000)

    snap = SnapshotState(
        snapshot_id=snap_id,
        sequence_number=1,
        watermark_ms=watermark,
        manifest_list_key=f"{table_path}/metadata/snap-{snap_id}-1-{snap_uuid}.avro",
        manifest_file_key=f"{table_path}/metadata/{snap_uuid}-m0.avro",
        metadata_key=f"{table_path}/metadata/v1.metadata.json",
        version_hint_key=f"{table_path}/metadata/version-hint.text",
        table=table,
        uuid=snap_uuid,
    )
    for s in splits:
        s.watermark_ms = watermark
    snap.splits = splits
    snap.total_records = total_records
    return snap


def _finalize_versioned_keys(snap: SnapshotState, version: int) -> None:
    table_path = f"{config.WAREHOUSE_PREFIX}/{snap.table.name}"
    snap.version = version
    snap.sequence_number = version
    snap.metadata_key = f"{table_path}/metadata/v{version}.metadata.json"
    snap.manifest_list_key = f"{table_path}/metadata/snap-{snap.snapshot_id}-{version}-{snap.uuid}.avro"
    snap.manifest_file_key = f"{table_path}/metadata/{snap.uuid}-m{version}.avro"
    snap.metadata_bytes = None
    snap.manifest_list_bytes = None
    snap.manifest_file_bytes = None


def _evict_unreferenced(pruned: list[SnapshotState]) -> None:
    if not pruned:
        return
    retained: set[str] = set()
    for snaps in state_store._history.values():
        for s in snaps:
            for sp in s.splits:
                retained.add(sp.object_key)
    for old in pruned:
        for sp in old.splits:
            if sp.object_key not in retained:
                cache.evict_parquet(sp.object_key)


async def publish(candidate: SnapshotState) -> bool:
    """Install ``candidate`` as a new version iff its content differs.

    Returns True when a new snapshot was published, False when the content was
    identical to the current version (no Fabric churn).
    """
    name = candidate.table.name
    prior = state_store._snapshots.get(name)
    if prior is not None and candidate.snapshot_id == prior.snapshot_id:
        return False
    version = (prior.version + 1) if prior is not None else 1
    _finalize_versioned_keys(candidate, version)
    pruned = state_store.register_snapshot(candidate)
    _evict_unreferenced(pruned)
    return True


# ---------------------------------------------------------------------------
# Change detection — dialect probe (best-effort, always validated)
# ---------------------------------------------------------------------------

async def probe_change_token(table) -> str | None:
    """Cheap read-only change token from source catalog stats, or None.

    Never raises: any error / unsupported dialect returns None so the cascade
    falls through.
    """
    src = table.source_table
    url = config.DB_URL.lower()
    try:
        if "sqlite" in url:
            v = await execute_scalar("PRAGMA data_version")
            return f"dv:{v}"
        if "postgres" in url:
            v = await execute_scalar(
                "SELECT (n_tup_ins + n_tup_upd + n_tup_del) "
                "FROM pg_stat_user_tables WHERE relid = to_regclass(:t)",
                {"t": src},
            )
            return None if v is None else f"pg:{v}"
        if "mssql" in url:
            v = await execute_scalar(
                "SELECT MAX(last_user_update) FROM sys.dm_db_index_usage_stats "
                "WHERE database_id = DB_ID() AND object_id = OBJECT_ID(:t)",
                {"t": src},
            )
            if v is None:
                return None
            return f"ms:{v.isoformat() if hasattr(v, 'isoformat') else v}"
    except Exception as exc:  # noqa: BLE001 - best-effort probe
        log.warning("refresh_probe_failed", table=table.name, error=str(exc))
        return None
    return None


async def prime_probe(table) -> None:
    """Record the current probe token so the first poll doesn't re-materialize."""
    _probe_tokens[table.name] = await probe_change_token(table)


# ---------------------------------------------------------------------------
# Poll (the cascade)
# ---------------------------------------------------------------------------

async def poll_once(table, bucket: str, warehouse_prefix: str) -> bool:
    """Run one detection+publish cycle for a table. Never raises."""
    strat = config.REFRESH_STRATEGY
    try:
        if strat == "manual":
            return False

        if strat == "content_hash":
            return await publish(await materialize_table(table, bucket, warehouse_prefix))

        if strat == "ttl":
            gen = int(time.time() // max(1, config.REFRESH_TTL_SECONDS))
            if _ttl_gen.get(table.name) == gen:
                return False
            _ttl_gen[table.name] = gen
            return await publish(await materialize_table(table, bucket, warehouse_prefix))

        # auto | dialect_probe -> probe first
        token = await probe_change_token(table)
        if token is not None:
            if token == _probe_tokens.get(table.name):
                return False
            _probe_tokens[table.name] = token
            return await publish(await materialize_table(table, bucket, warehouse_prefix))

        # probe unavailable
        if strat == "dialect_probe":
            return False
        if config.REFRESH_ALLOW_FULL_PULL:
            return await publish(await materialize_table(table, bucket, warehouse_prefix))
        log.warning("refresh_probe_unavailable", table=table.name)
        return False
    except Exception as exc:  # noqa: BLE001 - a poll must never crash the server
        log.warning("refresh_poll_failed", table=table.name, error=str(exc))
        return False


async def refresh_all(tables, bucket: str, warehouse_prefix: str) -> list[str]:
    """Force a materialize+publish for every table (manual override). Returns the
    names of tables whose content changed."""
    changed = []
    for t in tables:
        if await publish(await materialize_table(t, bucket, warehouse_prefix)):
            changed.append(t.name)
    return changed


def start_poller(tables, bucket: str, warehouse_prefix: str) -> asyncio.Task:
    """Start the background freshness poller (crash-proof)."""
    global _poller_task

    async def _loop():
        while True:
            try:
                await asyncio.sleep(config.REFRESH_POLL_SECONDS)
                for t in tables:
                    if await poll_once(t, bucket, warehouse_prefix):
                        cur = state_store._snapshots.get(t.name)
                        log.info("refresh_published", table=t.name,
                                 version=getattr(cur, "version", None))
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - loop must survive anything
                log.warning("refresh_loop_error", error=str(exc))

    _poller_task = asyncio.create_task(_loop())
    return _poller_task


async def stop_poller() -> None:
    global _poller_task
    if _poller_task is not None:
        _poller_task.cancel()
        try:
            await _poller_task
        except asyncio.CancelledError:
            pass
        _poller_task = None

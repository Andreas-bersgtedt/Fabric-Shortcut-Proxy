"""
Resilient table onboarding + background retry (agent startup resilience).

Instead of the all-or-nothing resolve that exits EX_CONFIG (78) when any source
is unreachable, the agent resolves each table independently: failures are
quarantined (``runtime.quarantine``) and the agent still serves the healthy
tables and every storage-proxy mount. A background loop retries quarantined
tables on an interval so they self-heal when the source comes back.

This module isolates the per-table fault handling; the main lifespan keeps its
existing (unchanged) materialization for the healthy set.
"""
from __future__ import annotations

import asyncio

import config
from runtime import quarantine
from observability.logging import get_logger

log = get_logger(__name__)


def _short(exc: Exception) -> str:
    text = str(exc).strip().splitlines()[0] if str(exc).strip() else exc.__class__.__name__
    return text[:300]


async def _resolve_one(table) -> None:
    """Resolve one table's schema/key and (optionally) validate its columns."""
    from db.executor import resolve_tables, validate_source_schema
    await resolve_tables([table])
    if config.VALIDATE_SOURCE_SCHEMA:
        await validate_source_schema(table)


async def resolve_resiliently(tables) -> list:
    """Resolve each table independently; quarantine failures. Return healthy tables.

    A table that resolves is released from any prior quarantine. A table that
    raises (unreachable source, bad credential, missing column) is quarantined
    with the error as its reason and left out of the served set.
    """
    healthy: list = []
    for table in tables:
        try:
            await _resolve_one(table)
        except Exception as exc:  # noqa: BLE001 - a bad table must not sink the agent
            reason = _short(exc)
            quarantine.quarantine(table.name, reason)
            log.error("table_quarantined", table=table.name, reason=reason)
            continue
        quarantine.release(table.name)
        healthy.append(table)
    return healthy


async def _bring_online(table, bucket: str, warehouse_prefix: str) -> None:
    """Resolve, materialize, and publish one table so it starts serving."""
    from iceberg import freshness
    await _resolve_one(table)
    candidate = await freshness.materialize_table(table, bucket, warehouse_prefix)
    await freshness.publish(candidate)


async def _retry_pass(by_name: dict, bucket: str, warehouse_prefix: str) -> None:
    """One retry sweep over the quarantined tables; release those that recover."""
    for name in quarantine.names():
        table = by_name.get(name)
        if table is None:
            quarantine.release(name)  # table no longer configured
            continue
        quarantine.record_attempt(name)
        try:
            await _bring_online(table, bucket, warehouse_prefix)
        except Exception as exc:  # noqa: BLE001 - stay quarantined, try again next tick
            log.info("table_retry_failed", table=name, reason=_short(exc))
            continue
        quarantine.release(name)
        log.info("table_recovered", table=name)


async def retry_loop(tables, bucket: str, warehouse_prefix: str, interval_seconds: float):
    """Periodically re-onboard quarantined tables; release them on success.

    ``tables`` is the full configured (enabled) set; only names currently in the
    quarantine registry are retried. Recovered tables are materialized + published
    (registered for serving) additively, without disturbing the healthy set.
    """
    by_name = {t.name: t for t in tables}
    interval = max(1.0, float(interval_seconds))
    try:
        while True:
            await asyncio.sleep(interval)
            await _retry_pass(by_name, bucket, warehouse_prefix)
    except asyncio.CancelledError:
        raise

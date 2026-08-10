"""
Resilient-startup table quarantine tests.

Covers the quarantine registry, resilient per-table resolution (a bad source is
quarantined, not fatal), the background retry pass (recovers or stays quarantined),
and the per-table ``enabled`` flag.
"""
from __future__ import annotations

import pytest

import config
from runtime import quarantine, table_health


@pytest.fixture(autouse=True)
def _clean_quarantine():
    quarantine.clear()
    yield
    quarantine.clear()


def _table(name: str) -> config.TableDef:
    return config.TableDef(name=name, source_table=name, schema=config.TABLE_SCHEMA,
                           num_splits=2, key_column="id")


# --- registry ----------------------------------------------------------------

def test_quarantine_registry_lifecycle():
    assert not quarantine.is_quarantined("t")
    quarantine.quarantine("t", "boom")
    assert quarantine.is_quarantined("t")
    assert quarantine.names() == ["t"]
    quarantine.record_attempt("t")
    snap = quarantine.snapshot()
    assert snap["t"]["reason"] == "boom" and snap["t"]["attempts"] == 1
    assert quarantine.release("t") is True
    assert not quarantine.is_quarantined("t")
    assert quarantine.release("t") is False


def test_quarantine_preserves_since_on_reason_update():
    quarantine.quarantine("t", "first")
    since = quarantine.snapshot()["t"]["since"]
    quarantine.quarantine("t", "second")
    entry = quarantine.snapshot()["t"]
    assert entry["reason"] == "second" and entry["since"] == since


# --- resilient resolution ----------------------------------------------------

async def test_resolve_resiliently_quarantines_only_failures(monkeypatch):
    async def fake_resolve(tables):
        for t in tables:
            if t.name == "bad":
                raise RuntimeError("could not connect to source db")

    monkeypatch.setattr("db.executor.resolve_tables", fake_resolve)
    monkeypatch.setattr(config, "VALIDATE_SOURCE_SCHEMA", False)

    healthy = await table_health.resolve_resiliently([_table("good"), _table("bad")])

    assert [t.name for t in healthy] == ["good"]
    assert quarantine.is_quarantined("bad")
    assert not quarantine.is_quarantined("good")
    assert "could not connect" in quarantine.snapshot()["bad"]["reason"]


async def test_resolve_resiliently_releases_recovered_table(monkeypatch):
    quarantine.quarantine("good", "was down")

    async def ok(tables):
        return None

    monkeypatch.setattr("db.executor.resolve_tables", ok)
    monkeypatch.setattr(config, "VALIDATE_SOURCE_SCHEMA", False)

    healthy = await table_health.resolve_resiliently([_table("good")])

    assert [t.name for t in healthy] == ["good"]
    assert not quarantine.is_quarantined("good")


# --- background retry pass ----------------------------------------------------

async def test_retry_pass_recovers_table(monkeypatch):
    quarantine.quarantine("flaky", "down")

    async def bring_online(table, bucket, prefix):
        return None

    monkeypatch.setattr(table_health, "_bring_online", bring_online)
    await table_health._retry_pass({"flaky": _table("flaky")}, "bucket", "wh")
    assert not quarantine.is_quarantined("flaky")


async def test_retry_pass_keeps_quarantined_on_failure(monkeypatch):
    quarantine.quarantine("flaky", "down")

    async def boom(table, bucket, prefix):
        raise RuntimeError("still down")

    monkeypatch.setattr(table_health, "_bring_online", boom)
    await table_health._retry_pass({"flaky": _table("flaky")}, "bucket", "wh")
    assert quarantine.is_quarantined("flaky")
    assert quarantine.snapshot()["flaky"]["attempts"] == 1


async def test_retry_pass_drops_unconfigured_table(monkeypatch):
    quarantine.quarantine("gone", "down")
    await table_health._retry_pass({}, "bucket", "wh")   # no matching table def
    assert not quarantine.is_quarantined("gone")


# --- enabled flag ------------------------------------------------------------

def test_tabledef_enabled_defaults_true_and_parses():
    from config import _tabledef_from_json

    assert config.TableDef(name="x", source_table="x").enabled is True
    off = _tabledef_from_json({"name": "d", "source_table": "d", "key_column": "id", "enabled": False})
    assert off.enabled is False
    on = _tabledef_from_json({"name": "e", "source_table": "e", "key_column": "id"})
    assert on.enabled is True

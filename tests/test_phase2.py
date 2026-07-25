"""
Phase 2 (robustness) tests:
  - H8  416 for unsatisfiable ranges; unique RequestId/HostId in error XML
  - H5  execute_split_query raises SourceUnavailable after retries
  - H6  validate_source_schema passes on a good schema, fails on a missing column
  - H4  config guards for concurrency
"""
from __future__ import annotations

import os
import pathlib
import xml.etree.ElementTree as ET

_DB = pathlib.Path(__file__).parent / "test_phase2.db"
os.environ["DB_URL"] = f"sqlite+aiosqlite:///{_DB.as_posix()}"
os.environ["NUM_SPLITS"] = "4"
os.environ["S3_BUCKET"] = "p2-bucket"

import pytest

import config
config.DB_URL = f"sqlite+aiosqlite:///{_DB.as_posix()}"
config.NUM_SPLITS = 4
config.BUCKET_NAME = "p2-bucket"

from s3.router import _make_object_response, _range_is_unsatisfiable
from s3.xml_responses import error_response


# ---------------------------------------------------------------------------
# H8 — range 416 + error-body fidelity
# ---------------------------------------------------------------------------

def test_range_unsatisfiable_detection():
    assert _range_is_unsatisfiable("bytes=100-200", 50) is True
    assert _range_is_unsatisfiable("bytes=49-", 50) is False
    assert _range_is_unsatisfiable("bytes=50-", 50) is True
    assert _range_is_unsatisfiable("bytes=-8", 50) is False   # suffix: satisfiable
    assert _range_is_unsatisfiable("bytes=0-3", 50) is False
    assert _range_is_unsatisfiable(None, 50) is False


def test_make_object_response_416():
    resp = _make_object_response(b"abcdef", "application/octet-stream", "bytes=100-200")
    assert resp.status_code == 416
    assert resp.headers["Content-Range"] == "bytes */6"


def test_make_object_response_suffix_still_206():
    resp = _make_object_response(b"abcdef", "application/octet-stream", "bytes=-2")
    assert resp.status_code == 206
    assert resp.body == b"ef"


def test_error_response_has_unique_request_and_host_id():
    a = ET.fromstring(error_response("NoSuchKey", "nope", "/x").decode())
    b = ET.fromstring(error_response("NoSuchKey", "nope", "/x").decode())
    a_req = a.findtext("RequestId")
    b_req = b.findtext("RequestId")
    assert a_req and b_req and a_req != b_req          # unique per response
    assert a.findtext("HostId")                        # present
    assert a.findtext("Code") == "NoSuchKey"


# ---------------------------------------------------------------------------
# H4 — resource-guard config
# ---------------------------------------------------------------------------

def test_validate_config_rejects_zero_concurrency(monkeypatch):
    monkeypatch.setattr(config, "MAX_CONCURRENT_GENERATIONS", 0)
    with pytest.raises(ValueError, match="MAX_CONCURRENT_GENERATIONS"):
        config.validate_config()


# ---------------------------------------------------------------------------
# H5 — retry / SourceUnavailable
# ---------------------------------------------------------------------------

async def test_execute_split_query_raises_source_unavailable(monkeypatch):
    import db.executor as ex

    monkeypatch.setattr(config, "DB_RETRY_BACKOFF_SECONDS", 0.0)

    attempts = {"n": 0}

    async def always_fail(sql, params, split_index):
        attempts["n"] += 1
        raise RuntimeError("db down")

    monkeypatch.setattr(ex, "_execute_once", always_fail)

    with pytest.raises(ex.SourceUnavailable):
        await ex.execute_split_query("SELECT 1", {}, split_index=0, max_retries=2)

    assert attempts["n"] == 3  # initial + 2 retries


# ---------------------------------------------------------------------------
# H6 — source schema validation
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
async def seeded():
    from demo.seed_db import seed_demo_database
    import db.executor as ex

    ex._engine = None
    await seed_demo_database()
    yield
    if ex._engine is not None:
        await ex._engine.dispose()
        ex._engine = None


async def test_validate_source_schema_ok(seeded):
    from db.executor import validate_source_schema
    await validate_source_schema()  # declared columns all present -> no raise


async def test_validate_source_schema_missing_column(seeded, monkeypatch):
    from db.executor import validate_source_schema
    from config import ColumnDef

    bad = list(config.TABLE_SCHEMA) + [
        ColumnDef(field_id=99, name="nonexistent_col", iceberg_type="string")
    ]
    monkeypatch.setattr(config, "TABLE_SCHEMA", bad)

    with pytest.raises(RuntimeError, match="missing declared"):
        await validate_source_schema()

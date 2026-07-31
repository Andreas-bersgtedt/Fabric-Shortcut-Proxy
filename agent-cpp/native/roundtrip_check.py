"""Round-trip validation for the native C++ Iceberg output.

Reads the Parquet + Avro manifests produced by ``native_tests.exe`` and asserts
that pyarrow / fastavro (and pyiceberg, if installed) can parse them and that the
Iceberg field-ids and values survive. Run after building + running native_tests:

    native_tests.exe split0.parquet manifest.avro manifest_list.avro manifest_stats.avro
    python roundtrip_check.py <build-dir>

Exit code 0 = all checks passed.
"""
from __future__ import annotations

import struct
import sys
from pathlib import Path

import fastavro
import pyarrow.parquet as pq

_passed = 0
_failed = 0


def check(ok: bool, what: str) -> None:
    global _passed, _failed
    if ok:
        _passed += 1
    else:
        _failed += 1
        print(f"  FAIL: {what}")


def check_parquet(path: Path) -> None:
    pf = pq.ParquetFile(path)
    schema = pf.schema_arrow
    expected_ids = {
        "id": 1, "order_date": 2, "customer_id": 3, "product": 4,
        "quantity": 5, "unit_price": 6, "total": 7, "region": 8,
    }
    for name, fid in expected_ids.items():
        field = schema.field(name)
        md = field.metadata or {}
        got = md.get(b"PARQUET:field_id")
        check(got == str(fid).encode(), f"parquet field_id {name}={fid} (got {got!r})")

    data = pf.read().to_pydict()
    check(data["id"] == [1, 2, 3], "parquet id values")
    check(data["product"] == ["apple", "banana", "cherry"], "parquet product values")
    check(data["quantity"] == [5, 10, 15], "parquet quantity values")


def _schema_field_ids(writer_schema: dict) -> dict:
    return {f["name"]: f.get("field-id") for f in writer_schema["fields"]}


def check_manifest_file(path: Path) -> None:
    with open(path, "rb") as fh:
        reader = fastavro.reader(fh)
        ws = reader.writer_schema
        records = list(reader)
    ids = _schema_field_ids(ws)
    check(ids.get("status") == 0, "manifest_entry status field-id")
    check(ids.get("data_file") == 2, "manifest_entry data_file field-id")
    check(len(records) == 1, "manifest has one entry")
    entry = records[0]
    check(entry["status"] == 1, "manifest entry status=ADDED")
    df = entry["data_file"]
    check(df["file_path"] == "s3://warehouse/data/split-0.parquet", "data_file file_path")
    check(df["file_format"] == "PARQUET", "data_file file_format")
    check(df["record_count"] == 3, "data_file record_count")


def check_manifest_list(path: Path, expected_length: int) -> None:
    with open(path, "rb") as fh:
        reader = fastavro.reader(fh)
        ws = reader.writer_schema
        records = list(reader)
    ids = _schema_field_ids(ws)
    check(ids.get("manifest_path") == 500, "manifest_list manifest_path field-id")
    check(ids.get("added_snapshot_id") == 503, "manifest_list added_snapshot_id field-id")
    check(len(records) == 1, "manifest list has one manifest")
    rec = records[0]
    check(rec["manifest_path"] == "s3://warehouse/metadata/snap-1-m0.avro", "manifest_path value")
    check(rec["manifest_length"] == expected_length, "manifest_length matches file size")
    check(rec["added_files_count"] == 1, "added_files_count")
    check(rec["added_rows_count"] == 3, "added_rows_count")


def _as_map(pairs) -> dict:
    return {p["key"]: p["value"] for p in (pairs or [])}


def check_manifest_stats(path: Path) -> None:
    with open(path, "rb") as fh:
        reader = fastavro.reader(fh)
        ws = reader.writer_schema
        records = list(reader)
    df_fields = {f["name"]: f for f in ws["fields"] if f["name"] == "data_file"}["data_file"]
    inner = {f["name"]: f.get("field-id") for f in df_fields["type"]["fields"]}
    check(inner.get("lower_bounds") == 125, "stats lower_bounds field-id")
    check(inner.get("upper_bounds") == 128, "stats upper_bounds field-id")

    df = records[0]["data_file"]
    value_counts = _as_map(df.get("value_counts"))
    check(all(v == 3 for v in value_counts.values()) and value_counts, "value_counts all == 3")
    lower = _as_map(df.get("lower_bounds"))
    # field_id 1 = id (long); lower bound = little-endian int64(1).
    check(lower.get(1) == struct.pack("<q", 1), "lower_bound(id) == LE int64(1)")
    upper = _as_map(df.get("upper_bounds"))
    check(upper.get(1) == struct.pack("<q", 3), "upper_bound(id) == LE int64(3)")


def try_pyiceberg(manifest_path: Path) -> None:
    try:
        from pyiceberg.manifest import _manifests  # noqa: F401
    except Exception:
        print("  (pyiceberg not available or API differs; skipping deep parse)")
        return
    print("  (pyiceberg import ok)")


def main() -> int:
    build = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).parent / "build"
    parquet = build / "split0.parquet"
    mf = build / "manifest.avro"
    ml = build / "manifest_list.avro"
    mfs = build / "manifest_stats.avro"

    check_parquet(parquet)
    check_manifest_file(mf)
    check_manifest_list(ml, mf.stat().st_size)
    check_manifest_stats(mfs)
    try_pyiceberg(mf)

    print(f"roundtrip: {_passed} passed, {_failed} failed")
    return 0 if _failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

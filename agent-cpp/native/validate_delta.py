"""Validate a native Delta serving image with the deltalake (delta-rs) reader.

Reads the _delta_log + Parquet splits produced by native_publish --format delta
(the same bytes the C++ agent serves) and asserts the row count and schema.

    python validate_delta.py <table-root> [expected-rows]
"""
from __future__ import annotations

import os
import sys

from deltalake import DeltaTable

_EXPECTED_COLUMNS = [
    "id", "order_date", "customer_id", "product",
    "quantity", "unit_price", "total", "region",
]


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: validate_delta.py <table-root> [expected-rows]")
        return 2
    root = sys.argv[1]
    expected = int(sys.argv[2]) if len(sys.argv) > 2 else 50000

    dt = DeltaTable(root)
    table = dt.to_pyarrow_table()
    print(f"Loaded Delta table at {root} (version {dt.version()})")
    print("COLUMNS =", table.column_names)
    print("ROWS SCANNED =", table.num_rows)

    failed = 0
    if table.column_names != _EXPECTED_COLUMNS:
        print(f"  FAIL: columns {table.column_names} != {_EXPECTED_COLUMNS}")
        failed += 1
    if table.num_rows != expected:
        print(f"  FAIL: rows {table.num_rows} != {expected}")
        failed += 1

    print("SUCCESS: deltalake read the table" if failed == 0 else "\nUNEXPECTED result")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    _rc = main()
    # delta-rs' tokio runtime can abort (SIGABRT) during normal interpreter
    # teardown; the check is already done, so hard-exit past that teardown.
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(_rc)

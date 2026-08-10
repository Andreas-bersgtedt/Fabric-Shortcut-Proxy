"""
Generate a small sample Delta table with PII-like columns for testing the object
store tokenizer (issue #12).

Point a local tokenizing mount at the output and the proxy serves a masked copy.
Example ``config.mounts.json`` entry (drops phone/ssn plaintext, tokenizes email,
random-tokens the SSN, keeps name/city):

    {
      "bucket": "customers-safe", "backend": "local",
      "root": "demo/sample_delta/customers", "format": "delta",
      "key_column": "customer_id",
      "columns": [
        {"field_id": 1, "name": "customer_id", "type": "long", "nullable": false},
        {"field_id": 2, "name": "full_name",   "type": "string"},
        {"field_id": 3, "name": "email_token",  "source": "email", "type": "string",
         "transform": {"kind": "deterministic_hash", "key_ref": "customer-pii-v1",
                       "domain": "customer-email", "normalization": "trim_lower"}},
        {"field_id": 4, "name": "ssn_token",     "source": "ssn", "type": "string",
         "transform": {"kind": "random_token"}},
        {"field_id": 5, "name": "city",          "type": "string"}
      ]
    }

Rows 1 and 2 share an email that only differs by case/whitespace, so a
``trim_lower`` deterministic token makes them equal — handy for verifying token
stability. Row 5 has a null email (null token). Set the key before serving:
``FSP_TOKENIZATION_KEY_CUSTOMER_PII_V1=<secret>``.

Usage:   python demo/seed_delta.py [output_dir]
Requires the 'objectstore' extra:  pip install 'fabric-shortcut-proxy[objectstore]'
"""
from __future__ import annotations

import sys
from datetime import date

_DEFAULT_DIR = "demo/sample_delta/customers"

# customer_id, full_name, email, phone, ssn, city, signup_date
_ROWS = [
    (1, "Alice Anderson", "Alice@Example.com",   "+1-206-555-0101", "111-11-1111", "Seattle",       date(2023, 1, 15)),
    (2, "Alice Anderson", " alice@example.com ", "+1-206-555-0101", "111-11-1111", "Seattle",       date(2023, 1, 16)),
    (3, "Bob Brown",      "bob@example.com",     "+1-415-555-0102", "222-22-2222", "San Francisco", date(2023, 2, 3)),
    (4, "Carol Clark",    "carol@example.com",   "+1-312-555-0103", "333-33-3333", "Chicago",       date(2023, 3, 20)),
    (5, "Dan Davis",      None,                  "+1-212-555-0104", "444-44-4444", "New York",      date(2023, 4, 1)),
    (6, "Erin Evans",     "erin@example.com",    "+1-617-555-0105", "555-55-5555", "Boston",        date(2023, 5, 12)),
    (7, "Frank Foster",   "frank@example.com",   "+1-303-555-0106", "666-66-6666", "Denver",        date(2023, 6, 30)),
    (8, "Grace Green",    "grace@example.com",   "+1-503-555-0107", "777-77-7777", "Portland",      date(2023, 7, 22)),
]


def build_table():
    import pyarrow as pa
    return pa.table({
        "customer_id": pa.array([r[0] for r in _ROWS], type=pa.int64()),
        "full_name":   pa.array([r[1] for r in _ROWS], type=pa.string()),
        "email":       pa.array([r[2] for r in _ROWS], type=pa.string()),
        "phone":       pa.array([r[3] for r in _ROWS], type=pa.string()),
        "ssn":         pa.array([r[4] for r in _ROWS], type=pa.string()),
        "city":        pa.array([r[5] for r in _ROWS], type=pa.string()),
        "signup_date": pa.array([r[6] for r in _ROWS], type=pa.date32()),
    })


def main(out_dir: str) -> None:
    try:
        from deltalake import write_deltalake
    except ImportError:
        sys.exit("This needs the 'objectstore' extra: pip install 'fabric-shortcut-proxy[objectstore]'")
    table = build_table()
    write_deltalake(out_dir, table, mode="overwrite")
    print(f"Wrote {table.num_rows}-row sample Delta table to {out_dir!r}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else _DEFAULT_DIR)

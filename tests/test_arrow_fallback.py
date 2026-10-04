from __future__ import annotations

import re

import config
from config import ColumnDef, ColumnTransform, TableDef
from iceberg.state_store import SplitDescriptor
from observability import tokenization as tokenization_metrics
from runtime.materializer import _apply_arrow_fallback


def test_arrow_fallback_preserves_nulls_and_records_safe_operational_event(
    monkeypatch, capsys
):
    monkeypatch.setattr(config, "DB_URL", "impala://host:21050/analytics")
    monkeypatch.setattr(config, "TOKENIZATION_FALLBACK", "arrow")
    tokenization_metrics.reset()
    table = TableDef(
        name="events",
        source_table="events",
        key_column="id",
        num_splits=2,
        schema=[
            ColumnDef(field_id=1, name="id", iceberg_type="long", nullable=False),
            ColumnDef(
                field_id=2,
                name="email_token",
                iceberg_type="string",
                source="email",
                transform=ColumnTransform(kind="random_token"),
            ),
        ],
    )
    split = SplitDescriptor(
        split_index=1,
        num_splits=2,
        object_key="warehouse/events/split-1.parquet",
        watermark_ms=0,
        table=table,
    )
    input_rows = [
        {"id": 1, "email_token": "Jöhn@example.test"},
        {"id": 2, "email_token": None},
    ]
    announced: set[tuple[str, str]] = set()

    transformed = _apply_arrow_fallback(input_rows, split, announced)
    _apply_arrow_fallback(input_rows, split, announced)

    assert transformed[0]["email_token"] != "Jöhn@example.test"
    assert transformed[1]["email_token"] is None
    assert len(announced) == 1
    event = tokenization_metrics.snapshot()["events"]
    assert event == {
        "count": 1,
        "columns": ["email_token"],
        "flavors": ["impala"],
        "kinds": ["random_token"],
    }
    logs = re.sub(r"\x1b\[[0-9;]*m", "", capsys.readouterr().out)
    assert "arrow_tokenization_fallback" in logs
    assert "plaintext_values_cross_proxy=True" in logs
    assert "Jöhn@example.test" not in logs

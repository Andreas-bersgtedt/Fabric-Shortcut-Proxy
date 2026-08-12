"""Compute an Open Mirroring change set by diffing source rows against state.

Given the last-published snapshot (:class:`open_mirror.state.PublishState`) and
the current source rows, derive the ``__rowMarker__`` change rows Fabric needs:

- a key present now but not before  -> INSERT (0)
- a key present in both, hash differs -> UPDATE (1)
- a key present before but not now   -> DELETE (2)  (only the key columns are sent)

``default_upsert`` collapses INSERT/UPDATE into UPSERT (4) for sources where the
insert-vs-update distinction is not meaningful.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from open_mirror.state import PublishState, build_state_from_rows, key_string, row_hash


class RowMarker:
    INSERT = 0
    UPDATE = 1
    DELETE = 2
    UPSERT = 4


@dataclass
class ChangeBatch:
    """The rows + parallel ``__rowMarker__`` values to publish, and the new state."""

    rows: list[dict] = field(default_factory=list)
    markers: list[int] = field(default_factory=list)
    new_state: PublishState = field(default_factory=PublishState)
    inserts: int = 0
    updates: int = 0
    deletes: int = 0

    @property
    def total(self) -> int:
        return len(self.rows)

    @property
    def has_changes(self) -> bool:
        return bool(self.rows)

    @property
    def counts(self) -> dict:
        return {"inserts": self.inserts, "updates": self.updates, "deletes": self.deletes}


def compute_changes(
    prev: PublishState,
    rows,
    columns,
    key_columns: list[str],
    *,
    default_upsert: bool = False,
) -> ChangeBatch:
    """Diff current ``rows`` against the prior ``prev`` snapshot.

    Deletes carry only the key columns (all other fields null), per the spec —
    a delete needs only the key. The returned ``new_state`` reflects the current
    rows and should be persisted only AFTER the change file is durably written.
    """
    prev_keys = dict(prev.keys) if prev else {}
    batch = ChangeBatch(new_state=build_state_from_rows(rows, columns, key_columns))

    seen: set[str] = set()
    insert_marker = RowMarker.UPSERT if default_upsert else RowMarker.INSERT
    update_marker = RowMarker.UPSERT if default_upsert else RowMarker.UPDATE

    for row in rows:
        ks = key_string(row, key_columns)
        seen.add(ks)
        old = prev_keys.get(ks)
        if old is None:
            batch.rows.append(row)
            batch.markers.append(insert_marker)
            batch.inserts += 1
        elif old.get("h") != row_hash(row, columns):
            batch.rows.append(row)
            batch.markers.append(update_marker)
            batch.updates += 1

    for ks, meta in prev_keys.items():
        if ks in seen:
            continue
        key_values = (meta or {}).get("k") or []
        del_row = {kc: (key_values[i] if i < len(key_values) else None) for i, kc in enumerate(key_columns)}
        batch.rows.append(del_row)
        batch.markers.append(RowMarker.DELETE)
        batch.deletes += 1

    return batch

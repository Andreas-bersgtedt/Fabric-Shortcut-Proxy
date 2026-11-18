from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
import hashlib
import json
import sqlite3
import subprocess
import sys

import pytest

from enterprise.control.contract import SnapshotManifest, SplitRef
from enterprise.control.edge_journal import EdgeJournal, EdgeJournalConflict, EdgeMetadata


def _manifest(number: int) -> SnapshotManifest:
    return SnapshotManifest(
        table="sales",
        epoch=number,
        table_format="iceberg",
        generation_id=f"generation-{number}",
        generation_fence=number,
        dataset_id="source::sales",
        produced_at_ms=1000 + number,
        plan_sha256="a" * 64,
    )


def test_journal_survives_restart_and_requires_ordered_acknowledgements(tmp_path):
    path = tmp_path / "edge.db"
    journal = EdgeJournal(path, dataset_id="source::sales")
    for generation in range(1, 4):
        assert journal.append(_manifest(generation)) == generation

    restarted = EdgeJournal(path, dataset_id="source::sales")
    pending = restarted.next_pending()
    assert pending is not None
    assert pending.manifest == _manifest(1)
    with closing(sqlite3.connect(path)) as db:
        second_digest = db.execute(
            "SELECT manifest_sha256 FROM generations WHERE sequence = 2"
        ).fetchone()[0]
    with pytest.raises(EdgeJournalConflict, match="in order"):
        restarted.acknowledge(2, "generation-2", second_digest)
    for generation in range(1, 4):
        entry = restarted.next_pending()
        assert entry is not None
        assert entry.sequence == generation
        assert entry.manifest == _manifest(generation)
        restarted.acknowledge(entry.sequence, entry.manifest.generation_id, entry.manifest_sha256)
        restarted.acknowledge(entry.sequence, entry.manifest.generation_id, entry.manifest_sha256)
    assert EdgeJournal(path, dataset_id="source::sales").next_pending() is None


def test_duplicate_and_conflicting_generation(tmp_path):
    journal = EdgeJournal(tmp_path / "edge.db", dataset_id="source::sales")
    original = _manifest(1)
    assert journal.append(original) == journal.append(original) == 1
    with pytest.raises(EdgeJournalConflict, match="different manifest"):
        journal.append(replace(original, produced_at_ms=2000))
    pending = journal.next_pending()
    assert pending is not None
    assert pending.manifest == original
    with pytest.raises(EdgeJournalConflict, match="does not identify"):
        journal.acknowledge(1, "wrong-generation", pending.manifest_sha256)
    with pytest.raises(EdgeJournalConflict, match="does not identify"):
        journal.acknowledge(1, "generation-1", "0" * 64)


def test_journal_rejects_wrong_dataset(tmp_path):
    path = tmp_path / "edge.db"
    journal = EdgeJournal(path, dataset_id="source::sales")
    with pytest.raises(EdgeJournalConflict, match="different dataset"):
        journal.append(replace(_manifest(1), dataset_id="other::sales"))
    with pytest.raises(EdgeJournalConflict, match="different dataset"):
        EdgeJournal(path, dataset_id="other::sales")


def test_journal_detects_manifest_corruption(tmp_path):
    path = tmp_path / "edge.db"
    journal = EdgeJournal(path, dataset_id="source::sales")
    journal.append(_manifest(1))
    entry = journal.next_pending()
    assert entry is not None
    with closing(sqlite3.connect(path)) as db, db:
        db.execute("UPDATE generations SET manifest = '{}'")
    with pytest.raises(EdgeJournalConflict, match="integrity check"):
        journal.next_pending()
    with pytest.raises(EdgeJournalConflict, match="integrity check"):
        journal.acknowledge(entry.sequence, entry.manifest.generation_id, entry.manifest_sha256)


def test_metadata_is_bound_to_the_record_and_covered_exactly(tmp_path):
    journal = EdgeJournal(tmp_path / "edge.db", dataset_id="source::sales")
    manifest = replace(_manifest(1), metadata_keys=["warehouse/metadata.json"])
    metadata = EdgeMetadata("warehouse/metadata.json", 3, hashlib.sha256(b"one").hexdigest())
    with pytest.raises(ValueError, match="metadata digests"):
        journal.append(manifest)
    with pytest.raises(ValueError, match="metadata digests"):
        journal.append(manifest, metadata=(metadata, metadata))
    assert journal.append(manifest, metadata=(metadata,)) == 1
    entry = journal.next_pending()
    assert entry is not None and entry.metadata == (metadata,)
    changed = replace(metadata, sha256=hashlib.sha256(b"two").hexdigest())
    with pytest.raises(EdgeJournalConflict, match="different manifest"):
        journal.append(manifest, metadata=(changed,))

    overlapping = replace(
        _manifest(2),
        metadata_keys=[metadata.key],
        splits=[SplitRef(metadata.key, metadata.size_bytes, 1, metadata.sha256)],
    )
    with pytest.raises(ValueError, match="metadata digests"):
        journal.append(overlapping, metadata=(metadata,))


def test_legacy_record_without_metadata_digests_fails_closed(tmp_path):
    path = tmp_path / "edge.db"
    journal = EdgeJournal(path, dataset_id="source::sales")
    journal.append(_manifest(1))
    old_payload = json.dumps(_manifest(1).to_dict(), sort_keys=True, separators=(",", ":"))
    old_digest = hashlib.sha256(old_payload.encode("utf-8")).hexdigest()
    with closing(sqlite3.connect(path)) as db, db:
        db.execute(
            "UPDATE generations SET manifest = ?, manifest_sha256 = ?",
            (old_payload, old_digest),
        )
    with pytest.raises(EdgeJournalConflict, match="unsupported journal manifest envelope"):
        journal.next_pending()
    with pytest.raises(EdgeJournalConflict, match="unsupported journal manifest envelope"):
        journal.acknowledge(1, "generation-1", old_digest)


def test_concurrent_append_assigns_each_generation_once(tmp_path):
    journal = EdgeJournal(tmp_path / "edge.db", dataset_id="source::sales")
    with ThreadPoolExecutor(max_workers=4) as executor:
        sequences = list(executor.map(journal.append, [_manifest(1)] * 8))
    assert sequences == [1] * 8
    assert journal.append(_manifest(2)) == 2


def test_another_process_can_resume_pending_work(tmp_path):
    path = tmp_path / "edge.db"
    journal = EdgeJournal(path, dataset_id="source::sales")
    journal.append(_manifest(1))
    journal.append(_manifest(2))
    code = (
        "import sys; "
        "from enterprise.control.edge_journal import EdgeJournal; "
        "j = EdgeJournal(sys.argv[1], dataset_id='source::sales'); "
        "e = j.next_pending(); "
        "assert e is not None; "
        "j.acknowledge(e.sequence, e.manifest.generation_id, e.manifest_sha256)"
    )
    subprocess.run([sys.executable, "-c", code, str(path)], check=True, timeout=30)
    entry = EdgeJournal(path, dataset_id="source::sales").next_pending()
    assert entry is not None
    assert entry.manifest == _manifest(2)

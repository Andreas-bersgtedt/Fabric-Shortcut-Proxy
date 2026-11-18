"""Verify and copy edge objects without publishing or acknowledging them."""

from contextlib import closing
import hashlib

from enterprise.control.edge_journal import EdgeJournalEntry, EdgeMetadata, encode_edge_record
from fabric_shortcut_proxy.runtime.artifact_store import ArtifactStore, _normalize_key


class EdgeTransferConflict(RuntimeError):
    """The recorded generation or one of its objects failed verification."""


def copy_edge_generation(
    entry: EdgeJournalEntry,
    source: ArtifactStore,
    destination: ArtifactStore,
    *,
    dataset_id: str,
    allowed_prefix: str,
) -> int:
    """Copy one generation; the regional Manager must still authorize and publish it."""
    if not allowed_prefix or _normalize_key(allowed_prefix) != allowed_prefix:
        raise ValueError("allowed prefix must be a non-empty canonical key")
    if entry.manifest.dataset_id != dataset_id:
        raise EdgeTransferConflict("generation belongs to another dataset")
    if isinstance(entry.sequence, bool) or not isinstance(entry.sequence, int) or entry.sequence < 1:
        raise ValueError("edge sequence must be a positive integer")
    encoded = encode_edge_record(entry.manifest, entry.metadata)
    if hashlib.sha256(encoded.encode("utf-8")).hexdigest() != entry.manifest_sha256:
        raise EdgeTransferConflict("edge record failed its integrity check")
    objects = tuple(
        EdgeMetadata(split.object_key, split.size_bytes, split.content_hash)
        for split in entry.manifest.splits
    ) + entry.metadata
    if not objects or any(not item.key.startswith(allowed_prefix + "/") for item in objects):
        raise EdgeTransferConflict("generation contains an object outside its allowed prefix")
    for item in objects:
        if not source.verify(item.key, size=item.size_bytes, content_hash=item.sha256):
            raise EdgeTransferConflict(f"edge object failed verification: {item.key}")
    for item in objects:
        with closing(source.get_stream(item.key)) as stream:
            destination.put_stream_if_absent(item.key, stream, length=item.size_bytes)
        if not destination.verify(item.key, size=item.size_bytes, content_hash=item.sha256):
            raise EdgeTransferConflict(f"copied object failed verification: {item.key}")
    return len(objects)

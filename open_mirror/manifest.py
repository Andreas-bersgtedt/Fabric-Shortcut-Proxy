"""Landing-zone data-file numbering.

Fabric Open Mirroring expects data files named as a 20-digit, monotonically
increasing integer with a ``.parquet`` (or delimited-text) extension, e.g.
``00000000000000000001.parquet``. The mirroring service deletes processed files
but leaves the last one, so the publisher derives the next index from whatever
numbered files remain in the folder.
"""
from __future__ import annotations

FILE_INDEX_WIDTH = 20
_PARQUET_EXT = ".parquet"


def format_file_name(index: int, *, extension: str = _PARQUET_EXT) -> str:
    """Return the zero-padded 20-digit data-file name for ``index`` (1-based)."""
    if index < 1:
        raise ValueError("file index is 1-based; must be >= 1")
    if index >= 10**FILE_INDEX_WIDTH:
        raise ValueError("file index exceeds the 20-digit landing-zone limit")
    ext = extension if extension.startswith(".") else f".{extension}"
    return f"{index:0{FILE_INDEX_WIDTH}d}{ext}"


def parse_file_index(name: str) -> int | None:
    """Return the integer index of a numbered data file, or ``None`` if it isn't one.

    Metadata files (``_metadata.json``, ``_partnerEvents.json``) and processed-file
    folders (``_ProcessedFiles`` / ``_FilesReadyToDelete``) are ignored.
    """
    n = (name or "").strip()
    if not n or n.startswith("_"):
        return None
    stem, _, ext = n.rpartition(".")
    if not stem or not ext:
        return None
    if not stem.isdigit():
        return None
    return int(stem)


def next_file_index(existing_names: list[str]) -> int:
    """Next 1-based data-file index given the folder's current contents."""
    highest = 0
    for name in existing_names or []:
        idx = parse_file_index(name)
        if idx is not None and idx > highest:
            highest = idx
    return highest + 1

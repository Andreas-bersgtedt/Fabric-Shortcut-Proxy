"""Validate the virtual Iceberg table with the pyiceberg reference reader.

Uses a custom FileIO that maps s3://fabric-iceberg-poc/<key> to the running
HTTP server, so we test the ACTUAL served bytes (metadata, avro manifests,
parquet). If pyiceberg can scan the table and return 50000 rows, the table is
Iceberg-spec compliant and OneLake's Iceberg->Delta conversion should succeed.
"""
from __future__ import annotations

import io
import os
import urllib.request

from pyiceberg.io import InputFile, InputStream, OutputFile, FileIO

SERVER = os.environ.get("S3EMU_SERVER", "http://127.0.0.1:9000")
BUCKET = os.environ.get("S3EMU_BUCKET", "fabric-iceberg-poc")
EXPECTED_ROWS = int(os.environ.get("EXPECTED_ROWS", "50000"))


def _to_http(location: str) -> str:
    # s3://fabric-iceberg-poc/warehouse/... -> http://server/fabric-iceberg-poc/warehouse/...
    if location.startswith("s3://"):
        return SERVER + "/" + location[len("s3://"):]
    if location.startswith("http"):
        return location
    return SERVER + "/" + location.lstrip("/")


class _HTTPInputFile(InputFile):
    def __init__(self, location: str):
        self._location = location
        self._url = _to_http(location)
        self._data: bytes | None = None

    def _fetch(self) -> bytes:
        if self._data is None:
            self._data = urllib.request.urlopen(self._url).read()
        return self._data

    @property
    def location(self) -> str:
        return self._location

    def __len__(self) -> int:
        return len(self._fetch())

    def exists(self) -> bool:
        try:
            self._fetch()
            return True
        except Exception:
            return False

    def open(self, _seekable: bool = True) -> InputStream:
        return io.BytesIO(self._fetch())


class _HTTPFileIO(FileIO):
    def new_input(self, location: str) -> InputFile:
        return _HTTPInputFile(location)

    def new_output(self, location: str) -> OutputFile:
        raise NotImplementedError("read-only")

    def delete(self, location: str) -> None:
        raise NotImplementedError("read-only")


def _discover_metadata_location() -> str:
    """Find the table's current root ``metadata.json`` via ListObjectsV2.

    The served object layout (warehouse prefix, canonical vs legacy paths) is a
    server-side detail, so we discover the metadata key from the bucket listing
    instead of hardcoding it. Picks the highest ``vN.metadata.json``.
    """
    import xml.etree.ElementTree as ET

    listing = urllib.request.urlopen(f"{SERVER}/{BUCKET}/?list-type=2").read()
    root = ET.fromstring(listing)
    ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
    keys = [e.text or "" for e in root.findall(".//s3:Contents/s3:Key", ns)]
    metas = [k for k in keys if k.endswith(".metadata.json")]
    if not metas:
        raise SystemExit(f"no *.metadata.json found in bucket {BUCKET!r}; keys={keys[:20]}")

    def _version(key: str) -> int:
        name = key.rsplit("/", 1)[-1]           # e.g. v3.metadata.json
        digits = name[1:].split(".", 1)[0]
        return int(digits) if name.startswith("v") and digits.isdigit() else -1

    return f"s3://{BUCKET}/{max(metas, key=_version)}"


def main() -> None:
    meta_loc = _discover_metadata_location()
    print("Loading table from", meta_loc)

    from pyiceberg.serializers import FromInputFile
    from pyiceberg.catalog.noop import NoopCatalog
    from pyiceberg.table import StaticTable

    io = _HTTPFileIO()
    metadata = FromInputFile.table_metadata(io.new_input(meta_loc))
    tbl = StaticTable(
        identifier=("static-table", meta_loc),
        metadata_location=meta_loc,
        metadata=metadata,
        io=io,
        catalog=NoopCatalog("static-table"),
    )
    print("Schema:")
    print(tbl.schema())
    print("\nCurrent snapshot:", tbl.current_snapshot())

    print("\nScanning table -> Arrow ...")
    arrow = tbl.scan().to_arrow()
    print(f"ROWS SCANNED = {arrow.num_rows}")
    print(f"COLUMNS = {arrow.column_names}")
    print(arrow.slice(0, 3).to_pydict())
    print("\nSUCCESS: pyiceberg read the table" if arrow.num_rows == EXPECTED_ROWS else "\nUNEXPECTED ROW COUNT")


if __name__ == "__main__":
    main()

"""Read-only PyIceberg FileIO with per-file mount-scope enforcement."""

from __future__ import annotations

import io
import os
from pathlib import Path
from urllib.parse import unquote, urlsplit

from pyiceberg.io import FileIO, InputFile

_BLOCK_SIZE = 256 * 1024


def _inside(path: str, root: str) -> bool:
    try:
        return os.path.commonpath(
            (os.path.normcase(path), os.path.normcase(root))
        ) == os.path.normcase(root)
    except ValueError:
        return False


def _file_uri_path(uri_path: str) -> str:
    path = unquote(uri_path)
    if (
        os.name == "nt" and len(path) >= 3 and path[0] == "/"
        and path[1].isalpha() and path[2] == ":"
    ):
        path = path[1:]
    return os.path.normpath(path)


def _not_found(exc: Exception) -> bool:
    if getattr(exc, "status_code", None) == 404:
        return True
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        code = str((response.get("Error") or {}).get("Code") or "")
        return code in ("404", "NoSuchKey", "NotFound", "BlobNotFound")
    return isinstance(exc, FileNotFoundError)


class ScopedIcebergFileIO(FileIO):
    """Only open Iceberg files whose resolved location is inside the mount."""

    def __init__(self, properties: dict):
        super().__init__(properties)
        self._backend = str(properties["_fsp_backend"])
        self._mount = properties["_fsp_mount"]
        self._scope_root = properties["_fsp_scope_root"]
        self._table_root = str(properties["_fsp_table_root"])
        self._client = properties.get("_fsp_client")
        if self._backend == "local":
            self._local_root = os.path.realpath(self._mount.root)
            self._scope_root = os.path.realpath(self._scope_root)
            if not _inside(self._scope_root, self._local_root):
                raise ValueError(
                    "local Iceberg mount prefix escapes its configured root"
                )
        elif self._client is None:
            raise ValueError(
                "remote Iceberg FileIO requires a configured storage client"
            )

    def _local_path(self, location: str) -> str:
        parsed = urlsplit(location)
        if parsed.scheme:
            if parsed.scheme.lower() != "file" or parsed.netloc not in (
                "",
                "localhost",
            ):
                raise ValueError("local Iceberg file URI must use file://")
            path = _file_uri_path(parsed.path)
        elif os.path.isabs(location):
            path = location
        else:
            root = urlsplit(self._table_root)
            path = os.path.join(_file_uri_path(root.path), location)
        resolved = os.path.realpath(path)
        if not _inside(resolved, self._scope_root):
            raise ValueError(
                "Iceberg file is outside the configured local mount prefix"
            )
        return resolved

    def _resolve(self, location: str) -> tuple[str, str]:
        if self._backend == "local":
            path = self._local_path(location)
            return Path(path).as_uri(), path
        from storage.objectstore_reader import _remote_object_key

        key = _remote_object_key(self._mount, location, table_root=self._table_root)
        scheme = "s3" if self._backend == "s3" else "az"
        return f"{scheme}://{self._mount.root}/{key}", key

    def new_input(self, location: str) -> InputFile:
        normalized, key = self._resolve(location)
        return _ScopedInputFile(self, normalized, key)

    def new_output(self, location: str):
        raise PermissionError("remote Iceberg mounts are read-only")

    def delete(self, location):
        raise PermissionError("remote Iceberg mounts are read-only")

    def _size(self, key: str) -> int:
        if self._backend == "local":
            return os.path.getsize(key)
        if self._backend == "s3":
            return int(
                self._client.head_object(Bucket=self._mount.root, Key=key)[
                    "ContentLength"
                ]
            )
        return int(self._client.get_blob_client(key).get_blob_properties().size)

    def _exists(self, key: str) -> bool:
        if self._backend == "local":
            return os.path.isfile(key)
        try:
            self._size(key)
        except Exception as exc:
            if _not_found(exc):
                return False
            raise
        return True

    def _read_range(self, key: str, start: int, size: int) -> bytes:
        if self._backend == "local":
            with open(key, "rb") as source:
                source.seek(start)
                return source.read(size)
        if self._backend == "s3":
            response = self._client.get_object(
                Bucket=self._mount.root,
                Key=key,
                Range=f"bytes={start}-{start + size - 1}",
            )
            body = response["Body"]
            try:
                return body.read()
            finally:
                body.close()
        return (
            self._client.get_blob_client(key)
            .download_blob(
                offset=start,
                length=size,
            )
            .readall()
        )


class _ScopedInputFile(InputFile):
    def __init__(self, file_io: ScopedIcebergFileIO, location: str, key: str):
        super().__init__(location)
        self._file_io = file_io
        self._key = key
        self._file_size: int | None = None

    def __len__(self) -> int:
        if self._file_size is None:
            self._file_size = self._file_io._size(self._key)
        return self._file_size

    def exists(self) -> bool:
        return self._file_io._exists(self._key)

    def open(self, seekable: bool = True):
        if not seekable:
            raise ValueError("Iceberg scans require seekable input streams")
        return io.BufferedReader(_RangeReader(self._file_io, self._key, len(self)))


class _RangeReader(io.RawIOBase):
    def __init__(self, file_io: ScopedIcebergFileIO, key: str, size: int):
        self._file_io = file_io
        self._key = key
        self._size = size
        self._position = 0
        self._cached_start = -1
        self._cached_data = b""

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self._position

    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        if whence == os.SEEK_SET:
            position = offset
        elif whence == os.SEEK_CUR:
            position = self._position + offset
        elif whence == os.SEEK_END:
            position = self._size + offset
        else:
            raise ValueError(f"invalid seek mode: {whence}")
        if position < 0:
            raise ValueError("negative seek position")
        self._position = position
        return position

    def readinto(self, buffer) -> int:
        if self.closed:
            raise ValueError("I/O operation on closed file")
        if self._position >= self._size or not buffer:
            return 0
        block_start = self._position // _BLOCK_SIZE * _BLOCK_SIZE
        if block_start != self._cached_start:
            block_size = min(_BLOCK_SIZE, self._size - block_start)
            self._cached_data = self._file_io._read_range(
                self._key, block_start, block_size
            )
            self._cached_start = block_start
        offset = self._position - self._cached_start
        count = min(
            len(buffer), len(self._cached_data) - offset, self._size - self._position
        )
        if count <= 0:
            return 0
        buffer[:count] = self._cached_data[offset : offset + count]
        self._position += count
        return count

"""Cooperative configuration locking and revisions for operator writes."""
from __future__ import annotations

import hashlib
import json
import os
import threading
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from fastapi.responses import JSONResponse

_LOCK = threading.RLock()
_LOCAL = threading.local()

REVISION_RESPONSES = {
    200: {
        "headers": {
            "ETag": {
                "description": "Opaque configuration revision; send unchanged in If-Match.",
                "schema": {"type": "string"},
            },
        },
    },
}
WRITE_REVISION_RESPONSES = {
    **REVISION_RESPONSES,
    409: {
        "description": "The supplied revision is stale; no updates were made.",
        "headers": {
            "ETag": {"schema": {"type": "string"}},
            "X-Correlation-ID": {"schema": {"type": "string"}},
        },
        "content": {
            "application/problem+json": {
                "schema": {
                    "type": "object",
                    "required": ["type", "title", "status", "code", "correlationId"],
                    "properties": {
                        "type": {"type": "string"},
                        "title": {"type": "string"},
                        "status": {"type": "integer", "const": 409},
                        "code": {"type": "string", "const": "stale_revision"},
                        "correlationId": {"type": "string"},
                    },
                },
            },
        },
    },
}
WRITE_REVISION_OPENAPI = {
    "parameters": [{
        "name": "If-Match", "in": "header", "required": False,
        "description": "Exact strong ETag from current settings. Omit only for legacy callers.",
        "schema": {"type": "string"},
    }],
}


@contextmanager
def configuration_transaction() -> Iterator[None]:
    """Serialize cooperating threads/processes without awaiting inside the lock."""
    from fabric_shortcut_proxy import config

    with _LOCK:
        depth = getattr(_LOCAL, "depth", 0)
        if depth:
            _LOCAL.depth = depth + 1
            try:
                yield
            finally:
                _LOCAL.depth = depth
            return

        path = Path(config._config_path(".fsp-configuration.lock"))
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a+b") as handle:
            if os.name == "nt":
                import msvcrt

                handle.seek(0, os.SEEK_END)
                if handle.tell() == 0:
                    handle.write(b"\0")
                    handle.flush()
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            _LOCAL.depth = 1
            try:
                yield
            finally:
                _LOCAL.depth = 0
                if os.name == "nt":
                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def configuration_revision() -> str:
    """Hash persisted bytes and explicit overrides while holding the transaction."""
    from fabric_shortcut_proxy import config

    digest = hashlib.sha256()
    paths = {
        Path(config._config_path(name)).resolve()
        for name in {*config._SETTINGS_TO_FILE_MAP.values(),
                     "config.connection.json", "config.tables.json"}
    }
    paths.add(Path(config.config_file_path()).resolve())
    from fabric_shortcut_proxy.security.credential_store import _default_store_path

    paths.add(Path(config.CREDENTIAL_STORE_PATH or _default_store_path()).resolve())
    for path in sorted(paths):
        name = str(path).encode()
        digest.update(len(name).to_bytes(8, "big"))
        digest.update(name)
        try:
            content = path.read_bytes()
        except FileNotFoundError:
            digest.update(b"\0")
        else:
            digest.update(b"\1")
            digest.update(len(content).to_bytes(8, "big"))
            digest.update(content)
    overrides = {
        setting["env"]: os.environ[setting["env"]]
        for setting in config._SETTINGS_REGISTRY.values()
        if setting["env"] and setting["env"] in os.environ
    }
    overrides.update({
        key: value for key, value in os.environ.items()
        if key.startswith(("DB_URL_", "FSP_TOKENIZATION_KEY_"))
    })
    digest.update(json.dumps(
        overrides,
        sort_keys=True, separators=(",", ":"), ensure_ascii=True,
    ).encode())
    return f'"{digest.hexdigest()}"'


def check_revision(if_match: str | None, correlation_id: str) -> JSONResponse | None:
    """Reject non-exact revisions; omitted headers retain the native UI contract."""
    if if_match is None:
        return None
    current = configuration_revision()
    if if_match == current:
        return None
    from fabric_shortcut_proxy.observability.logging import get_logger

    correlation = correlation_id if (
        correlation_id and len(correlation_id) <= 128
        and all(32 <= ord(char) < 127 for char in correlation_id)
    ) else str(uuid.uuid4())
    get_logger(__name__).warning(
        "configuration_revision_rejected",
        code="stale_revision", correlation_id=correlation,
    )
    return JSONResponse(
        {
            "type": "about:blank", "title": "Configuration revision is stale",
            "status": 409, "code": "stale_revision",
            "correlationId": correlation,
        },
        status_code=409,
        media_type="application/problem+json",
        headers={"ETag": current, "X-Correlation-ID": correlation},
    )


def revision_response(response: JSONResponse) -> JSONResponse:
    if response.status_code < 400:
        response.headers["ETag"] = configuration_revision()
        response.headers["Cache-Control"] = "no-store"
    return response

"""Signed placement policy bundles with last-known-good outage fallback."""
from __future__ import annotations

import base64
import binascii
import json
import os
import tempfile
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Generic, TypeVar
from urllib.parse import urlsplit

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from fabric_shortcut_proxy.runtime.artifact_store import (
    ArtifactStore,
    ObjectNotFound,
)


_MAX_BUNDLE_BYTES = 4 * 1024 * 1024
_SCHEMA_VERSION = 1
_ALLOWED_FIELDS = {
    "schema_version",
    "revision",
    "issued_at_ms",
    "key_id",
    "placement",
    "signature",
}
T = TypeVar("T")


class PolicyBundleError(ValueError):
    """Base error for policy bundle loading and verification."""


class PolicyBundleUnavailable(PolicyBundleError):
    """The remote bundle and a usable cached bundle are unavailable."""


class PolicyBundleValidationError(PolicyBundleError):
    """A bundle is malformed, unsigned, invalid, or rolled back."""


@dataclass(frozen=True)
class VerifiedPolicyBundle:
    revision: int
    issued_at_ms: int
    key_id: str
    placement: dict
    raw: bytes


@dataclass(frozen=True)
class LoadedPolicyBundle(Generic[T]):
    value: T
    revision: int
    key_id: str
    from_cache: bool


class _RemoteUnavailable(Exception):
    pass


def _canonical_bytes(value: dict) -> bytes:
    signed = {key: item for key, item in value.items() if key != "signature"}
    return json.dumps(
        signed,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def verify_policy_bundle(
    raw: bytes,
    public_key_pem: bytes,
    *,
    expected_key_id: str = "",
) -> VerifiedPolicyBundle:
    if len(raw) > _MAX_BUNDLE_BYTES:
        raise PolicyBundleValidationError("policy bundle exceeds 4 MiB")
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PolicyBundleValidationError("policy bundle must be valid JSON") from exc
    if not isinstance(value, dict):
        raise PolicyBundleValidationError("policy bundle must be a JSON object")
    unknown = sorted(set(value) - _ALLOWED_FIELDS)
    if unknown:
        raise PolicyBundleValidationError(
            f"policy bundle has unknown fields: {unknown}"
        )
    missing = sorted(_ALLOWED_FIELDS - set(value))
    if missing:
        raise PolicyBundleValidationError(
            f"policy bundle is missing fields: {missing}"
        )
    if value["schema_version"] != _SCHEMA_VERSION:
        raise PolicyBundleValidationError("unsupported policy bundle schema version")
    revision = value["revision"]
    issued_at_ms = value["issued_at_ms"]
    if (
        isinstance(revision, bool)
        or not isinstance(revision, int)
        or revision < 1
    ):
        raise PolicyBundleValidationError(
            "policy bundle revision must be a positive integer"
        )
    if (
        isinstance(issued_at_ms, bool)
        or not isinstance(issued_at_ms, int)
        or issued_at_ms < 0
    ):
        raise PolicyBundleValidationError(
            "policy bundle issued_at_ms must be a non-negative integer"
        )
    key_id = value["key_id"]
    if not isinstance(key_id, str) or not key_id.strip():
        raise PolicyBundleValidationError(
            "policy bundle key_id must be a non-empty string"
        )
    key_id = key_id.strip()
    if expected_key_id and key_id != expected_key_id:
        raise PolicyBundleValidationError(
            f"policy bundle key_id {key_id!r} does not match the pinned key"
        )
    placement = value["placement"]
    if not isinstance(placement, dict):
        raise PolicyBundleValidationError(
            "policy bundle placement must be a JSON object"
        )
    signature_text = value["signature"]
    if not isinstance(signature_text, str) or not signature_text:
        raise PolicyBundleValidationError(
            "policy bundle signature must be non-empty base64"
        )
    try:
        signature = base64.b64decode(signature_text, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise PolicyBundleValidationError(
            "policy bundle signature must be valid base64"
        ) from exc
    try:
        public_key = serialization.load_pem_public_key(public_key_pem)
    except (TypeError, ValueError) as exc:
        raise PolicyBundleValidationError(
            "policy bundle public key must be valid PEM"
        ) from exc
    if not isinstance(public_key, Ed25519PublicKey):
        raise PolicyBundleValidationError(
            "policy bundle public key must use Ed25519"
        )
    try:
        public_key.verify(signature, _canonical_bytes(value))
    except InvalidSignature as exc:
        raise PolicyBundleValidationError(
            "policy bundle signature verification failed"
        ) from exc
    return VerifiedPolicyBundle(
        revision=revision,
        issued_at_ms=issued_at_ms,
        key_id=key_id,
        placement=placement,
        raw=bytes(raw),
    )


def _fetch_bundle(source_url: str, timeout_seconds: float) -> bytes:
    _validate_source_url(source_url)
    request = urllib.request.Request(
        source_url,
        headers={"Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(
            request,
            timeout=timeout_seconds,
        ) as response:
            _validate_source_url(response.geturl())
            raw = response.read(_MAX_BUNDLE_BYTES + 1)
    except urllib.error.HTTPError as exc:
        if exc.code in {408, 429} or 500 <= exc.code <= 599:
            raise _RemoteUnavailable(str(exc)) from exc
        raise PolicyBundleValidationError(
            f"policy bundle endpoint returned HTTP {exc.code}"
        ) from exc
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise _RemoteUnavailable(str(exc)) from exc
    if len(raw) > _MAX_BUNDLE_BYTES:
        raise PolicyBundleValidationError("policy bundle exceeds 4 MiB")
    return raw


def _validate_source_url(source_url: str) -> None:
    parsed = urlsplit(source_url)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.fragment
    ):
        raise PolicyBundleValidationError(
            "FSP_POLICY_BUNDLE_URL must be an HTTPS URL without credentials or a fragment"
        )


def _read_file(path: str, description: str) -> bytes:
    try:
        return Path(path).read_bytes()
    except OSError as exc:
        raise PolicyBundleValidationError(
            f"cannot read {description} {path!r}: {exc}"
        ) from exc


def _write_cache(path: str, raw: bytes) -> None:
    target = Path(path)
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        handle, temporary = tempfile.mkstemp(
            prefix=f".{target.name}.",
            dir=target.parent,
        )
        try:
            with os.fdopen(handle, "wb") as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
            os.chmod(temporary, 0o600)
            os.replace(temporary, target)
        except BaseException:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass
            raise
    except OSError as exc:
        raise PolicyBundleValidationError(
            f"cannot persist policy bundle cache {path!r}: {exc}"
        ) from exc


def load_policy_bundle(
    *,
    source_url: str,
    public_key_file: str,
    cache_file: str,
    value_loader: Callable[[dict], T],
    expected_key_id: str = "",
    timeout_seconds: float = 5.0,
    fetcher: Callable[[str, float], bytes] | None = None,
) -> LoadedPolicyBundle[T]:
    if not public_key_file:
        raise PolicyBundleValidationError(
            "FSP_POLICY_BUNDLE_PUBLIC_KEY_FILE is required"
        )
    if not cache_file:
        raise PolicyBundleValidationError(
            "FSP_POLICY_BUNDLE_CACHE_FILE is required"
        )
    if timeout_seconds <= 0:
        raise PolicyBundleValidationError(
            "FSP_POLICY_BUNDLE_TIMEOUT_SECONDS must be positive"
        )
    public_key = _read_file(public_key_file, "policy bundle public key")
    cached: VerifiedPolicyBundle | None = None
    cache_path = Path(cache_file)
    if cache_path.exists():
        cached = verify_policy_bundle(
            _read_file(cache_file, "policy bundle cache"),
            public_key,
            expected_key_id=expected_key_id,
        )
    try:
        raw = (fetcher or _fetch_bundle)(source_url, timeout_seconds)
    except _RemoteUnavailable as exc:
        if cached is None:
            raise PolicyBundleUnavailable(
                "policy bundle endpoint is unavailable and no last-known-good bundle exists"
            ) from exc
        return LoadedPolicyBundle(
            value=value_loader(cached.placement),
            revision=cached.revision,
            key_id=cached.key_id,
            from_cache=True,
        )
    remote = verify_policy_bundle(
        raw,
        public_key,
        expected_key_id=expected_key_id,
    )
    if cached is not None and remote.revision < cached.revision:
        raise PolicyBundleValidationError(
            f"policy bundle rollback rejected: revision {remote.revision} "
            f"is older than cached revision {cached.revision}"
        )
    if (
        cached is not None
        and remote.revision == cached.revision
        and remote.raw != cached.raw
    ):
        raise PolicyBundleValidationError(
            f"policy bundle revision {remote.revision} was reused with different content"
        )
    loaded = value_loader(remote.placement)
    _write_cache(cache_file, raw)
    return LoadedPolicyBundle(
        value=loaded,
        revision=remote.revision,
        key_id=remote.key_id,
        from_cache=False,
    )


def load_policy_bundle_from_store(
    *,
    store: ArtifactStore,
    object_key: str,
    public_key_file: str,
    cache_file: str,
    value_loader: Callable[[dict], T],
    expected_key_id: str = "",
) -> LoadedPolicyBundle[T]:
    if not object_key.strip():
        raise PolicyBundleValidationError(
            "FSP_POLICY_BUNDLE_BLOB_NAME is required"
        )

    def fetch(_source: str, _timeout: float) -> bytes:
        try:
            return store.get(object_key)
        except (ObjectNotFound, OSError) as exc:
            raise _RemoteUnavailable(str(exc)) from exc
        except Exception as exc:
            try:
                from azure.core.exceptions import (
                    HttpResponseError,
                    ServiceRequestError,
                    ServiceResponseError,
                )
            except ImportError:
                raise
            if isinstance(
                exc,
                (ServiceRequestError, ServiceResponseError),
            ):
                raise _RemoteUnavailable(str(exc)) from exc
            if isinstance(exc, HttpResponseError) and (
                exc.status_code in {408, 429}
                or (
                    exc.status_code is not None
                    and 500 <= exc.status_code <= 599
                )
            ):
                raise _RemoteUnavailable(str(exc)) from exc
            raise

    return load_policy_bundle(
        source_url=f"artifact-store:{object_key}",
        public_key_file=public_key_file,
        cache_file=cache_file,
        value_loader=value_loader,
        expected_key_id=expected_key_id,
        fetcher=fetch,
    )

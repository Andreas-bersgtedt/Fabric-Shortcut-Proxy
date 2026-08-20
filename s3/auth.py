"""
AWS Signature Version 4 verification (Plan item H3).

Verifies the ``Authorization: AWS4-HMAC-SHA256 ...`` header that S3 clients
(including Microsoft Fabric's S3-compatible shortcut) attach to each request,
against the credentials configured for this proxy.

Design notes
------------
* Read-only proxy: requests carry no body, so the signed payload hash is taken
  directly from the ``x-amz-content-sha256`` header (usually ``UNSIGNED-PAYLOAD``
  for S3 reads) exactly as AWS SigV4 for S3 specifies -- we never need to hash a
  body.
* Verification is enabled by default via ``config.REQUIRE_SIGV4``. Deployments
  that enable it must configure matching S3 credentials.
* On failure we return an S3-style error code (``AccessDenied`` /
  ``InvalidAccessKeyId`` / ``SignatureDoesNotMatch``) which the middleware maps
  to a 403.
"""
from __future__ import annotations

import hashlib
import hmac
from typing import Callable, Mapping
from urllib.parse import quote, unquote

_ALGORITHM = "AWS4-HMAC-SHA256"
_SERVICE = "s3"
_EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()


class SigV4Error(Exception):
    """Raised on a verification failure, carrying an S3 error code."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


def _hmac(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def _derive_signing_key(secret: str, date_stamp: str, region: str, service: str) -> bytes:
    k_date = _hmac(("AWS4" + secret).encode("utf-8"), date_stamp)
    k_region = _hmac(k_date, region)
    k_service = _hmac(k_region, service)
    return _hmac(k_service, "aws4_request")


def _canonical_query(query_string: str) -> str:
    """Canonical query string per AWS SigV4 (RFC 3986 percent-encoding).

    Every parameter name and value is URI-encoded (``/`` -> ``%2F``, space ->
    ``%20``, etc.) and the pairs are sorted by encoded name. Real S3 clients --
    including Microsoft Fabric's shortcut signer and the AWS SDKs -- sign this
    encoded form, so the server must reconstruct it here regardless of how the
    query arrived on the wire. We normalize by decoding first, then re-encoding,
    so both raw (``delimiter=/``) and pre-encoded (``delimiter=%2F``) wire forms
    canonicalize identically.
    """
    if not query_string:
        return ""
    pairs: list[tuple[str, str]] = []
    for pair in query_string.split("&"):
        if not pair:
            continue
        key, _, value = pair.partition("=")
        key = quote(unquote(key), safe="-_.~")
        value = quote(unquote(value), safe="-_.~")
        pairs.append((key, value))
    pairs.sort()
    return "&".join(f"{k}={v}" for k, v in pairs)


def _trim(value: str) -> str:
    # AWS canonicalization: collapse internal whitespace runs, strip ends.
    return " ".join(value.split())


def _parse_authorization(header: str) -> tuple[str, list[str], str]:
    """Return (credential, signed_headers, signature) from an Authorization header."""
    if not header or not header.startswith(_ALGORITHM):
        raise SigV4Error("AccessDenied", "Missing or unsupported Authorization header.")
    rest = header[len(_ALGORITHM):].strip()
    parts = {}
    for item in rest.split(","):
        item = item.strip()
        if "=" in item:
            k, _, v = item.partition("=")
            parts[k.strip()] = v.strip()
    try:
        credential = parts["Credential"]
        signed_headers = parts["SignedHeaders"].split(";")
        signature = parts["Signature"]
    except KeyError as exc:
        raise SigV4Error("AccessDenied", f"Malformed Authorization header: missing {exc}.")
    return credential, signed_headers, signature


def verify_signature(
    method: str,
    path: str,
    query_string: str,
    headers: Mapping[str, str],
    *,
    access_key_id: str | None = None,
    secret_access_key: str | None = None,
    secret_resolver: Callable[[str], str | None] | None = None,
) -> str:
    """Verify a SigV4-signed request. Raises :class:`SigV4Error` on any failure.

    ``headers`` must be a case-insensitive mapping of the request headers.
    ``path`` is the (already client-encoded) request path, ``query_string`` the
    raw query string. Provide either a single ``access_key_id`` +
    ``secret_access_key`` (legacy), or a ``secret_resolver`` that maps a presented
    access-key id to its signing secret (multi-key / ACL mode). Returns the
    verified access-key id (the caller's authenticated identity).
    """
    # Case-insensitive lookup helper.
    lower = {k.lower(): v for k, v in headers.items()}

    auth = lower.get("authorization", "")
    credential, signed_headers, provided_sig = _parse_authorization(auth)

    # Credential = AK/DATE/REGION/s3/aws4_request
    cred_parts = credential.split("/")
    if len(cred_parts) != 5:
        raise SigV4Error("AccessDenied", "Malformed credential scope.")
    cred_key, date_stamp, region, service, terminator = cred_parts
    if service != _SERVICE or terminator != "aws4_request":
        raise SigV4Error("AccessDenied", "Unexpected credential scope.")
    if secret_resolver is not None:
        secret = secret_resolver(cred_key)
        if not secret:
            raise SigV4Error("InvalidAccessKeyId", "The access key id is not recognized.")
    else:
        if cred_key != access_key_id:
            raise SigV4Error("InvalidAccessKeyId", "The access key id does not match.")
        secret = secret_access_key or ""

    amz_date = lower.get("x-amz-date")
    if not amz_date:
        raise SigV4Error("AccessDenied", "Missing x-amz-date header.")

    payload_hash = lower.get("x-amz-content-sha256", _EMPTY_SHA256)

    signed_headers = sorted(h.lower() for h in signed_headers)
    canonical_headers = ""
    for name in signed_headers:
        if name not in lower:
            raise SigV4Error("SignatureDoesNotMatch", f"Signed header {name!r} not present.")
        canonical_headers += f"{name}:{_trim(lower[name])}\n"

    canonical_uri = quote(path, safe="/-_.~")
    canonical_query = _canonical_query(query_string)
    signed_headers_str = ";".join(signed_headers)

    canonical_request = (
        f"{method}\n{canonical_uri}\n{canonical_query}\n"
        f"{canonical_headers}\n{signed_headers_str}\n{payload_hash}"
    )

    scope = f"{date_stamp}/{region}/{service}/aws4_request"
    string_to_sign = (
        f"{_ALGORITHM}\n{amz_date}\n{scope}\n"
        f"{hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()}"
    )

    signing_key = _derive_signing_key(secret, date_stamp, region, service)
    expected_sig = hmac.new(
        signing_key, string_to_sign.encode("utf-8"), hashlib.sha256
    ).hexdigest()

    if not hmac.compare_digest(expected_sig, provided_sig):
        raise SigV4Error("SignatureDoesNotMatch", "The request signature does not match.")
    return cred_key

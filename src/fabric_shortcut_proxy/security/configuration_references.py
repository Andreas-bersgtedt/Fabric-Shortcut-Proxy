"""Reference-only database URLs backed by the existing encrypted local store."""
from __future__ import annotations

import re

from fabric_shortcut_proxy.security.credential_store import (
    CredentialStore,
    looks_masked,
)

CREDENTIAL_REFERENCE_PREFIX = "fsp-credential://"


def credential_record_id(reference: str) -> str:
    """Extract only a record id, preserving the reference's spelling elsewhere."""
    match = re.fullmatch(
        r"(?:fsp-credential://)?([a-zA-Z0-9][a-zA-Z0-9_-]{0,127})",
        reference,
    )
    if match is None:
        raise ValueError("credential reference must be fsp-credential://<record-id> or a bare record id")
    return match.group(1)


def validate_database_reference(value: object) -> dict[str, str]:
    if (
        not isinstance(value, dict)
        or set(value) != {"kind", "reference"}
        or value.get("kind") != "fsp_credential_store"
        or not isinstance(value.get("reference"), str)
        or not value["reference"].strip()
        or value["reference"] != value["reference"].strip()
    ):
        raise ValueError("database_url_reference requires an fsp_credential_store URL record id")
    credential_record_id(value["reference"])
    return {"kind": value["kind"], "reference": value["reference"]}


def resolve_database_reference(value: object, *, store: CredentialStore | None = None) -> str:
    reference = validate_database_reference(value)
    if store is None:
        from fabric_shortcut_proxy import system_config

        store = CredentialStore(system_config.CREDENTIAL_STORE_PATH or None)
    # No read-through here: typed configuration never contacts a vault or writes
    # credentials. Key Vault hydration must populate the encrypted cache first.
    url = store.get_url(credential_record_id(reference["reference"]))
    if not url or looks_masked(url):
        raise ValueError("referenced database credential is absent or unreadable")
    from sqlalchemy.engine import make_url
    from sqlalchemy.exc import ArgumentError

    try:
        make_url(url)
    except ArgumentError as exc:
        raise ValueError("referenced database credential is not a database URL") from exc
    return url

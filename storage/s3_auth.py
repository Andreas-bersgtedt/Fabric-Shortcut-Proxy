"""
Outbound S3 authentication for ``s3`` mounts (devplan/StorageProxy.md, Phase 2).

Resolves the upstream credential for a mount and builds an authenticated boto3 S3
client. It aims for **maximum coverage** of real-world S3 and S3-compatible stores
(AWS S3, MinIO, Ceph RGW, Cloudflare R2, Wasabi, Backblaze B2, Dell ECS,
DigitalOcean Spaces, Oracle/IBM COS, …) by delegating to botocore's own credential
machinery wherever possible.

Split of responsibilities:
  * **Secret material** (access keys, session tokens, role config, process command)
    lives ONLY in the encrypted credential store, keyed by the mount's
    ``credential`` id — never in ``config.mounts.json`` and never logged.
  * **Non-secret connection knobs** (endpoint, region, addressing style, signature
    version, TLS verification, FIPS/dualstack) live on the :class:`Mount`.

Supported auth modes (``mode`` in the stored credential blob):
  ``static``       — access key + secret key.
  ``session``      — static keys + session token (STS temporary credentials).
  ``assume_role``  — STS AssumeRole (auto-refreshing); optional ``source`` creds.
  ``web_identity`` — STS AssumeRoleWithWebIdentity (OIDC / EKS IRSA).
  ``profile``      — a named profile from ``~/.aws`` (incl. source_profile chains).
  ``sso``          — an IAM Identity Center profile (token cache + refresh).
  ``instance``     — the default provider chain (EC2/ECS/EKS instance role, env).
  ``process``      — an external ``credential_process`` command.
  ``anonymous``    — unsigned requests (public buckets).

boto3/botocore are imported lazily so the core install (local/NFS/SMB mounts) needs
no cloud SDK; ``build_s3_client`` raises a clear install hint when they're absent.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping


SUPPORTED_MODES = frozenset({
    "static", "session", "assume_role", "web_identity",
    "profile", "sso", "instance", "process", "anonymous",
})

_DEFAULT_SESSION_NAME = "fsp-proxy"


@dataclass(frozen=True)
class S3AuthConfig:
    """Parsed upstream S3 credential (secret material). Never logged."""
    mode: str
    access_key: str = ""
    secret_key: str = ""
    session_token: str = ""
    role_arn: str = ""
    external_id: str = ""
    session_name: str = _DEFAULT_SESSION_NAME
    duration_seconds: int = 3600
    web_identity_token_file: str = ""
    profile: str = ""
    credential_process: str = ""
    source: "S3AuthConfig | None" = None   # base creds for assume_role


@dataclass(frozen=True)
class S3ClientOptions:
    """Non-secret connection knobs for the upstream endpoint."""
    endpoint: str = ""
    region: str = ""
    addressing_style: str = ""     # auto | path | virtual
    signature_version: str = ""    # s3v4 | s3 ("" = botocore default)
    verify: Any = None             # None=default | False=skip TLS verify | str=CA bundle
    use_fips: bool = False
    use_dualstack: bool = False


def parse_s3_auth(d: Mapping[str, Any]) -> S3AuthConfig:
    """Parse a stored credential blob into an :class:`S3AuthConfig`.

    When ``mode`` is omitted it is inferred from the presence of static keys
    (``session`` if a token is present, else ``static``).
    """
    mode = str(d.get("mode") or "").strip().lower()
    if not mode:
        mode = "session" if d.get("session_token") else ("static" if d.get("access_key") else "")
    source = None
    src = d.get("source")
    if isinstance(src, Mapping):
        source = parse_s3_auth(src)
    return S3AuthConfig(
        mode=mode,
        access_key=str(d.get("access_key") or "").strip(),
        secret_key=str(d.get("secret_key") or ""),
        session_token=str(d.get("session_token") or ""),
        role_arn=str(d.get("role_arn") or "").strip(),
        external_id=str(d.get("external_id") or "").strip(),
        session_name=str(d.get("session_name") or _DEFAULT_SESSION_NAME).strip() or _DEFAULT_SESSION_NAME,
        duration_seconds=int(d.get("duration_seconds") or 3600),
        web_identity_token_file=str(d.get("web_identity_token_file") or "").strip(),
        profile=str(d.get("profile") or "").strip(),
        credential_process=str(d.get("credential_process") or "").strip(),
        source=source,
    )


def validate_s3_auth(auth: S3AuthConfig) -> list[str]:
    """Return a list of problems with a parsed auth config (empty = OK)."""
    problems: list[str] = []
    if not auth.mode:
        return ["missing auth 'mode' (and no static keys to infer one)"]
    if auth.mode not in SUPPORTED_MODES:
        return [f"unsupported auth mode {auth.mode!r} (use one of {sorted(SUPPORTED_MODES)})"]
    if auth.mode in ("static", "session"):
        if not auth.access_key:
            problems.append("static/session auth needs 'access_key'")
        if not auth.secret_key:
            problems.append("static/session auth needs 'secret_key'")
        if auth.mode == "session" and not auth.session_token:
            problems.append("session auth needs 'session_token'")
    elif auth.mode == "assume_role":
        if not auth.role_arn:
            problems.append("assume_role auth needs 'role_arn'")
        if auth.source is not None:
            problems.extend(f"source: {p}" for p in validate_s3_auth(auth.source))
    elif auth.mode == "web_identity":
        if not auth.role_arn:
            problems.append("web_identity auth needs 'role_arn'")
        if not auth.web_identity_token_file:
            problems.append("web_identity auth needs 'web_identity_token_file'")
    elif auth.mode in ("profile", "sso"):
        if not auth.profile:
            problems.append(f"{auth.mode} auth needs 'profile'")
    elif auth.mode == "process":
        if not auth.credential_process:
            problems.append("process auth needs 'credential_process'")
    # instance / anonymous need nothing.
    return problems


def options_from_mount(mount) -> S3ClientOptions:
    """Derive :class:`S3ClientOptions` from a :class:`storage.mounts.Mount`."""
    verify: Any = None
    vt = (getattr(mount, "verify_tls", "") or "").strip()
    if vt:
        verify = False if vt.lower() in ("false", "0", "no", "off") else vt
    # Custom endpoints (MinIO/Ceph/…) default to path-style addressing.
    addressing = getattr(mount, "addressing_style", "") or ("path" if getattr(mount, "endpoint", "") else "auto")
    return S3ClientOptions(
        endpoint=getattr(mount, "endpoint", ""),
        region=getattr(mount, "region", ""),
        addressing_style=addressing,
        signature_version=getattr(mount, "signature_version", ""),
        verify=verify,
        use_fips=bool(getattr(mount, "use_fips", False)),
        use_dualstack=bool(getattr(mount, "use_dualstack", False)),
    )


def resolve_s3_auth(mount, *, store=None) -> S3AuthConfig:
    """Resolve a mount's upstream auth from the credential store or inline mode.

    A mount with a ``credential`` id reads its (encrypted) blob from the store; a
    credential-less mount must declare an explicit ``auth`` mode (``anonymous`` /
    ``instance``) so ambient host credentials are never picked up by surprise.
    """
    cid = (getattr(mount, "credential", "") or "").strip()
    if cid:
        st = store
        if st is None:
            from security.credential_store import CredentialStore
            st = CredentialStore()
        blob = st.get_secret(cid)
        if blob is None:
            raise KeyError(f"s3 credential {cid!r} not found (or unreadable) in the credential store")
        return parse_s3_auth(blob)
    auth_mode = (getattr(mount, "auth", "") or "").strip().lower()
    if auth_mode:
        return parse_s3_auth({"mode": auth_mode})
    raise KeyError(
        f"mount {getattr(mount, 'bucket', '?')!r}: set a 'credential' id or an explicit "
        "'auth' mode ('anonymous' or 'instance')")


# ---------------------------------------------------------------------------
# Client factory
# ---------------------------------------------------------------------------

def _require_boto3():
    try:
        import boto3  # noqa: F401
        from botocore.config import Config  # noqa: F401
    except ImportError as exc:  # pragma: no cover - exercised only without the extra
        raise RuntimeError(
            "the 's3' mount backend needs boto3; install it with "
            "pip install 'fabric-shortcut-proxy[s3proxy]'") from exc


def _base_config(auth: S3AuthConfig, opts: S3ClientOptions):
    from botocore import UNSIGNED
    from botocore.config import Config

    cfg: dict[str, Any] = {
        "s3": {"addressing_style": opts.addressing_style or "auto"},
        "retries": {"max_attempts": 5, "mode": "standard"},
    }
    if opts.signature_version:
        sv = opts.signature_version.strip().lower()
        cfg["signature_version"] = "s3v4" if sv in ("s3v4", "v4", "sigv4") else sv
    if opts.use_fips:
        cfg["use_fips_endpoint"] = True
    if opts.use_dualstack:
        cfg["use_dualstack_endpoint"] = True
    if auth.mode == "anonymous":
        cfg["signature_version"] = UNSIGNED
    return Config(**cfg)


def _client_kwargs(opts: S3ClientOptions) -> dict[str, Any]:
    kwargs: dict[str, Any] = {}
    if opts.endpoint:
        kwargs["endpoint_url"] = opts.endpoint
    if opts.region:
        kwargs["region_name"] = opts.region
    if opts.verify is not None:
        kwargs["verify"] = opts.verify
    return kwargs


def build_s3_client(auth: S3AuthConfig, opts: S3ClientOptions):
    """Build an authenticated boto3 S3 client for the given auth + options."""
    _require_boto3()
    import boto3

    ckw = _client_kwargs(opts)
    ckw["config"] = _base_config(auth, opts)

    if auth.mode in ("static", "session"):
        return boto3.client(
            "s3",
            aws_access_key_id=auth.access_key,
            aws_secret_access_key=auth.secret_key,
            aws_session_token=(auth.session_token or None),
            **ckw,
        )
    if auth.mode in ("anonymous", "instance"):
        # anonymous -> UNSIGNED via config; instance -> default provider chain.
        return boto3.client("s3", **ckw)
    if auth.mode in ("profile", "sso"):
        return boto3.Session(profile_name=auth.profile or None).client("s3", **ckw)
    if auth.mode == "assume_role":
        return _assume_role_client(auth, opts, ckw)
    if auth.mode == "web_identity":
        return _web_identity_client(auth, opts, ckw)
    if auth.mode == "process":
        return _process_client(auth, opts, ckw)
    raise ValueError(f"unsupported s3 auth mode: {auth.mode!r}")


def _botocore_session(region: str):
    from botocore.session import Session as BotocoreSession
    s = BotocoreSession()
    if region:
        s.set_config_variable("region", region)
    return s


def _assume_role_client(auth: S3AuthConfig, opts: S3ClientOptions, ckw: dict):
    import boto3
    from botocore.credentials import (
        AssumeRoleCredentialFetcher,
        DeferredRefreshableCredentials,
    )

    base = _botocore_session(opts.region)
    src = auth.source
    if src is not None and src.mode in ("static", "session"):
        base.set_credentials(src.access_key, src.secret_key, src.session_token or None)
    elif src is not None and src.mode in ("profile", "sso"):
        from botocore.session import Session as BotocoreSession
        base = BotocoreSession(profile=src.profile or None)
        if opts.region:
            base.set_config_variable("region", opts.region)
    # else: source omitted -> base uses the default provider chain / instance role.

    extra: dict[str, Any] = {"RoleSessionName": auth.session_name or _DEFAULT_SESSION_NAME}
    if auth.external_id:
        extra["ExternalId"] = auth.external_id
    if auth.duration_seconds:
        extra["DurationSeconds"] = int(auth.duration_seconds)

    fetcher = AssumeRoleCredentialFetcher(
        client_creator=base.create_client,
        source_credentials=base.get_credentials(),
        role_arn=auth.role_arn,
        extra_args=extra,
    )
    creds = DeferredRefreshableCredentials(method="assume-role", refresh_using=fetcher.fetch_credentials)
    session = _botocore_session(opts.region)
    session._credentials = creds
    return boto3.Session(botocore_session=session).client("s3", **ckw)


def _web_identity_client(auth: S3AuthConfig, opts: S3ClientOptions, ckw: dict):
    import boto3
    from botocore.credentials import (
        AssumeRoleWithWebIdentityCredentialFetcher,
        DeferredRefreshableCredentials,
    )
    from botocore.utils import FileWebIdentityTokenLoader

    base = _botocore_session(opts.region)
    extra: dict[str, Any] = {"RoleSessionName": auth.session_name or _DEFAULT_SESSION_NAME}
    if auth.duration_seconds:
        extra["DurationSeconds"] = int(auth.duration_seconds)

    fetcher = AssumeRoleWithWebIdentityCredentialFetcher(
        client_creator=base.create_client,
        web_identity_token_loader=FileWebIdentityTokenLoader(auth.web_identity_token_file),
        role_arn=auth.role_arn,
        extra_args=extra,
    )
    creds = DeferredRefreshableCredentials(
        method="assume-role-with-web-identity", refresh_using=fetcher.fetch_credentials)
    session = _botocore_session(opts.region)
    session._credentials = creds
    return boto3.Session(botocore_session=session).client("s3", **ckw)


def _process_client(auth: S3AuthConfig, opts: S3ClientOptions, ckw: dict):
    import json
    import shlex
    import subprocess

    import boto3
    from botocore.credentials import DeferredRefreshableCredentials

    argv = shlex.split(auth.credential_process)   # no shell => no injection surface

    def _refresh() -> dict:
        out = subprocess.check_output(argv)        # noqa: S603 - operator-configured command
        data = json.loads(out)
        return {
            "access_key": data["AccessKeyId"],
            "secret_key": data["SecretAccessKey"],
            "token": data.get("SessionToken"),
            "expiry_time": data.get("Expiration"),
        }

    creds = DeferredRefreshableCredentials(method="custom-process", refresh_using=_refresh)
    session = _botocore_session(opts.region)
    session._credentials = creds
    return boto3.Session(botocore_session=session).client("s3", **ckw)

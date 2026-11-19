"""Outbound Google Cloud Storage authentication for ``gcs`` mounts.

GCS mounts deliberately use only Application Default Credentials or anonymous
access. Secret material must not be embedded in mount configuration.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


SUPPORTED_MODES = frozenset({"adc", "anonymous"})


@dataclass(frozen=True)
class GCSAuthConfig:
    mode: str


def parse_gcs_auth(d: Mapping[str, Any]) -> GCSAuthConfig:
    mode = str(d.get("mode") or "").strip().lower()
    return GCSAuthConfig(mode=mode)


def validate_gcs_auth(auth: GCSAuthConfig) -> list[str]:
    if not auth.mode:
        return ["missing auth 'mode' (use 'adc' or 'anonymous')"]
    if auth.mode not in SUPPORTED_MODES:
        return [f"unsupported auth mode {auth.mode!r} (use 'adc' or 'anonymous')"]
    return []


def resolve_gcs_auth(mount, *, store=None) -> GCSAuthConfig:
    """Resolve GCS mount auth without reading secret material.

    The default is ADC for private buckets and workload identity. ``anonymous``
    is the only other explicitly supported mode for public buckets.
    """
    if (getattr(mount, "credential", "") or "").strip():
        raise KeyError(
            f"mount {getattr(mount, 'bucket', '?')!r}: GCS mounts use ADC/workload "
            "identity and do not accept embedded credentials"
        )
    auth_mode = (getattr(mount, "auth", "") or "adc").strip().lower()
    return parse_gcs_auth({"mode": auth_mode})

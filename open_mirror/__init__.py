"""Open Mirroring publisher package.

Emits a Microsoft Fabric Open Mirroring landing-zone layout (per-table folders,
``_metadata.json``, optional ``_partnerEvents.json``, and monotonically numbered
Parquet files with a trailing ``__rowMarker__`` column for incremental changes)
using the repo's existing source connectors and schema reflection.

See ``devplan/Open_Mirroring_Integration_Plan.md`` for the design.
"""
from __future__ import annotations

from open_mirror.config import (
    OpenMirrorTableTarget,
    OpenMirrorTarget,
    load_targets,
    target_from_dict,
)
from open_mirror.landing_zone import (
    LandingZoneBackend,
    LocalLandingZone,
    is_onelake_uri,
    open_landing_zone,
    table_relative_path,
)
from open_mirror.manifest import format_file_name, next_file_index, parse_file_index
from open_mirror.metadata import build_partner_events, build_table_metadata
from open_mirror.publisher import (
    ROW_MARKER_COLUMN,
    LandingZonePublisher,
    build_landing_parquet,
)
from open_mirror.source import (
    PublishResult,
    publish_initial_load,
    publish_target_initial_load,
)

__all__ = [
    "OpenMirrorTableTarget",
    "OpenMirrorTarget",
    "load_targets",
    "target_from_dict",
    "LandingZoneBackend",
    "LocalLandingZone",
    "is_onelake_uri",
    "open_landing_zone",
    "table_relative_path",
    "format_file_name",
    "next_file_index",
    "parse_file_index",
    "build_partner_events",
    "build_table_metadata",
    "ROW_MARKER_COLUMN",
    "LandingZonePublisher",
    "build_landing_parquet",
    "PublishResult",
    "publish_initial_load",
    "publish_target_initial_load",
]


def __getattr__(name):
    # Lazy so importing the package never pulls in the optional Azure SDK.
    if name in ("OneLakeLandingZone", "_parse_onelake_url"):
        from open_mirror import onelake
        return getattr(onelake, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

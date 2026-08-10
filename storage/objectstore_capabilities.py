"""
Object-store format capability matrix and fail-closed policy validation (issue #12).

Parallel to ``db/capabilities.py`` but keyed by table *format* (``delta`` /
``iceberg``) rather than SQL flavor: object-store sources have no compute engine,
so tokenization runs proxy-side in ``storage/tokenizer.py``. This module declares
which transforms each format supports and validates a mount's column policy at load
time, failing closed on anything unsupported — mirroring the pushdown behavior
where a table requesting an unsupported transform is rejected rather than silently
downgraded.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from config import ColumnDef


@dataclass(frozen=True)
class ObjectStoreFormatCapabilities:
    format: str
    supports_deterministic_tokenization: bool = True
    supports_random_tokenization: bool = True

    def to_dict(self) -> dict:
        return {
            "format": self.format,
            "supports_deterministic_tokenization": self.supports_deterministic_tokenization,
            "supports_random_tokenization": self.supports_random_tokenization,
        }


_CAPABILITIES: dict[str, ObjectStoreFormatCapabilities] = {
    "delta": ObjectStoreFormatCapabilities(format="delta"),
    "iceberg": ObjectStoreFormatCapabilities(format="iceberg"),
}

SUPPORTED_FORMATS: tuple[str, ...] = tuple(_CAPABILITIES)


def get_format_capabilities(fmt: str) -> ObjectStoreFormatCapabilities | None:
    return _CAPABILITIES.get((fmt or "").strip().lower())


def capabilities_summary() -> dict:
    """Format -> capability dict, for surfacing in /readyz and the monitor."""
    return {fmt: caps.to_dict() for fmt, caps in _CAPABILITIES.items()}


def validate_object_store_policy(
    *, format: str, key_column: str | None, columns: "list[ColumnDef]"
) -> None:
    """Fail closed on an unsupported format or a mistargeted/unsupported transform.

    Raises ``ValueError`` when: the format is unknown; a transform sits on the
    key/ordering column; or the format cannot perform a requested transform kind.
    ``ColumnTransform`` / ``ColumnDef`` already enforce kind validity, key_ref
    presence, and string output type at construction.
    """
    caps = get_format_capabilities(format)
    if caps is None:
        raise ValueError(
            f"Unsupported object-store format {format!r}; supported: {SUPPORTED_FORMATS}"
        )
    key = (key_column or "").strip()
    for column in columns:
        transform = column.transform
        if transform is None:
            continue
        if key and key in {column.name, column.source_name}:
            raise ValueError(
                f"key column {key!r} cannot have a transform (object-store mount)"
            )
        if transform.kind == "deterministic_hash" and not caps.supports_deterministic_tokenization:
            raise ValueError(
                f"format {caps.format!r} does not support deterministic tokenization"
            )
        if transform.kind == "random_token" and not caps.supports_random_tokenization:
            raise ValueError(
                f"format {caps.format!r} does not support random tokenization"
            )

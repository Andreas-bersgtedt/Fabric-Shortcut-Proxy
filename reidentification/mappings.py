"""Validated, secret-free source lookup mappings for re-identification."""
from __future__ import annotations

import json
import os
import re
import tempfile
from dataclasses import dataclass


class ReidentificationMappingError(ValueError):
    """Raised when a source lookup mapping is unsafe or inconsistent."""


_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


def _identifier(value: object, field: str) -> str:
    text = str(value or "").strip()
    if not _IDENTIFIER.fullmatch(text):
        raise ReidentificationMappingError(f"{field} must be a simple SQL identifier")
    return text


@dataclass(frozen=True)
class LookupMapping:
    """One approved source-side durable-token lookup definition."""

    policy_id: str
    table_id: str
    column_id: str
    lookup_column: str
    clear_text_column: str
    primary_key_column: str

    def __post_init__(self) -> None:
        if not self.policy_id or any(char.isspace() for char in self.policy_id):
            raise ReidentificationMappingError("policy_id must be non-empty and contain no whitespace")
        _identifier(self.table_id, "table_id")
        _identifier(self.column_id, "column_id")
        _identifier(self.lookup_column, "lookup_column")
        _identifier(self.clear_text_column, "clear_text_column")
        _identifier(self.primary_key_column, "primary_key_column")

    @classmethod
    def from_dict(cls, raw: object) -> "LookupMapping":
        if not isinstance(raw, dict):
            raise ReidentificationMappingError("re-identification mapping must be an object")
        forbidden = {"key", "secret", "token", "value", "sql", "connection"}
        if forbidden.intersection(raw):
            raise ReidentificationMappingError("mapping must not contain secrets, tokens, SQL, or connection details")
        try:
            return cls(
                policy_id=str(raw["policy_id"]).strip(),
                table_id=_identifier(raw["table_id"], "table_id"),
                column_id=_identifier(raw["column_id"], "column_id"),
                lookup_column=_identifier(raw["lookup_column"], "lookup_column"),
                clear_text_column=_identifier(raw["clear_text_column"], "clear_text_column"),
                primary_key_column=_identifier(raw["primary_key_column"], "primary_key_column"),
            )
        except KeyError as exc:
            raise ReidentificationMappingError(f"mapping is missing {exc.args[0]!r}") from exc

    def to_public(self) -> dict:
        return {
            "policy_id": self.policy_id,
            "table_id": self.table_id,
            "column_id": self.column_id,
            "lookup_column": self.lookup_column,
            "clear_text_column": self.clear_text_column,
            "primary_key_column": self.primary_key_column,
        }


class LookupMappings:
    """Collection of approved mappings with policy and table assignment checks."""

    def __init__(self, mappings: list[LookupMapping] | None = None) -> None:
        self._mappings: dict[tuple[str, str, str], LookupMapping] = {}
        for mapping in mappings or []:
            key = (mapping.policy_id, mapping.table_id, mapping.column_id)
            if key in self._mappings:
                raise ReidentificationMappingError("duplicate policy/table/column mapping")
            self._mappings[key] = mapping

    @classmethod
    def from_dict(cls, raw: object) -> "LookupMappings":
        if not isinstance(raw, dict) or not isinstance(raw.get("mappings"), list):
            raise ReidentificationMappingError("re-identification config must contain a mappings list")
        return cls([LookupMapping.from_dict(item) for item in raw["mappings"]])

    def to_dict(self) -> dict:
        return {"mappings": [mapping.to_public() for mapping in self._mappings.values()]}

    def list_public(self) -> list[dict]:
        return [self._mappings[key].to_public() for key in sorted(self._mappings)]

    def get(self, policy_id: str, table_id: str, column_id: str) -> LookupMapping:
        try:
            return self._mappings[(policy_id, table_id, column_id)]
        except KeyError:
            raise ReidentificationMappingError("re-identification mapping is not configured") from None

    def replace(self, mapping: LookupMapping) -> None:
        self._mappings[(mapping.policy_id, mapping.table_id, mapping.column_id)] = mapping

    def remove(self, policy_id: str, table_id: str, column_id: str) -> None:
        try:
            del self._mappings[(policy_id, table_id, column_id)]
        except KeyError:
            raise ReidentificationMappingError("re-identification mapping is not configured") from None

    def validate(self) -> None:
        """Ensure every mapping names an enabled durable policy and its table column."""
        import config
        from tokenization import load_default_registry

        policies = load_default_registry()
        tables = {table.name: table for table in config.TABLES if table.enabled}
        for mapping in self._mappings.values():
            policy = policies.get(mapping.policy_id)
            if policy.kind != "durable_token" or policy.algorithm != "sha256":
                raise ReidentificationMappingError(
                    f"policy {mapping.policy_id!r} must be an enabled durable sha256 policy"
                )
            table = tables.get(mapping.table_id)
            if table is None or table.schema is None:
                raise ReidentificationMappingError(
                    f"mapping table {mapping.table_id!r} is not an enabled configured table with a schema"
                )
            column = next((item for item in table.schema if item.name == mapping.column_id), None)
            if column is None or column.policy_id != mapping.policy_id:
                raise ReidentificationMappingError(
                    f"mapping column {mapping.column_id!r} is not assigned to policy {mapping.policy_id!r}"
                )
            if column.source_name != mapping.clear_text_column:
                raise ReidentificationMappingError(
                    "clear_text_column must match the configured source column"
                )
            if table.key_column and table.key_column != mapping.primary_key_column:
                raise ReidentificationMappingError(
                    "primary_key_column must match the configured table key_column"
                )


def default_mappings_path() -> str:
    configured = os.environ.get("REIDENTIFICATION_MAPPING_FILE", "").strip()
    if configured:
        return configured
    directory = os.environ.get("FSP_CONFIG_DIR", "").strip()
    return os.path.join(directory, "config.reidentification.json") if directory else "config.reidentification.json"


def load_mappings(path: str) -> LookupMappings:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return LookupMappings.from_dict(json.load(handle))
    except FileNotFoundError:
        return LookupMappings()
    except (OSError, json.JSONDecodeError) as exc:
        raise ReidentificationMappingError(f"unable to load re-identification mappings: {exc}") from exc


def load_default_mappings() -> LookupMappings:
    return load_mappings(default_mappings_path())


def save_mappings(path: str, mappings: LookupMappings) -> None:
    directory = os.path.dirname(os.path.abspath(path)) or "."
    os.makedirs(directory, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".reidentification-", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(mappings.to_dict(), handle, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except OSError as exc:
        raise ReidentificationMappingError(f"unable to save re-identification mappings: {exc}") from exc
    finally:
        if os.path.exists(temporary):
            os.remove(temporary)
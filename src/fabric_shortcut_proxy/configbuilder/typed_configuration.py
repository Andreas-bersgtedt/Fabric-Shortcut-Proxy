"""Lossless, persisted source/table representation for versioned clients."""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Annotated, Literal, TypeVar

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from fabric_shortcut_proxy import config
from fabric_shortcut_proxy.security.configuration_references import (
    credential_record_id,
    resolve_database_reference,
)
from fabric_shortcut_proxy.security.credential_store import (
    CredentialStore,
    tokenization_env_var,
)

CONTRACT_HEADER = "X-FSP-Configuration-Contract"
CONTRACT_VERSION = "3.1.0"
CONTRACT_OPENAPI = {
    "parameters": [{
        "name": CONTRACT_HEADER, "in": "header", "required": False,
        "description": "3.1.0 opts into persisted, reference-only source/table settings.",
        "schema": {"type": "string", "enum": [CONTRACT_VERSION]},
    }],
}

PositiveInt = Annotated[int, Field(gt=0)]
NonnegativeInt = Annotated[int, Field(ge=0)]
Nonempty = Annotated[str, Field(min_length=1)]


class ContractError(ValueError):
    """Safe error text only; never include input values or native diagnostics."""


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    @field_validator("*")
    @classmethod
    def no_padded_strings(cls, value):
        if isinstance(value, str) and value != value.strip():
            raise ValueError("leading/trailing whitespace is not supported")
        return value


class SecretReference(StrictModel):
    kind: Literal["fsp_credential_store"]
    reference: str

    @field_validator("reference")
    @classmethod
    def valid_record_reference(cls, value: str) -> str:
        credential_record_id(value)
        return value


class Connection(StrictModel):
    id: Annotated[str, Field(pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_-]{0,127}$")]
    database_url_reference: SecretReference
    query_timeout_seconds: PositiveInt = 300
    query_max_rows: PositiveInt = 500_000
    max_retries: NonnegativeInt = 3
    retry_backoff_seconds: NonnegativeInt = 1
    validate_source_schema: bool = True


class Transform(StrictModel):
    kind: Nonempty
    key_reference: SecretReference | None = None
    domain: str | None = None
    normalization: str | None = None


class Column(StrictModel):
    field_id: PositiveInt
    name: Nonempty
    source: Nonempty | None = None
    type: Nonempty
    nullable: bool = True
    transform: Transform | None = None


class Table(StrictModel):
    name: Nonempty
    source_table: Nonempty
    key_column: Nonempty
    connection: Nonempty = "default"
    num_splits: PositiveInt | None = None
    split_strategy: Literal["auto", "modulo", "range", "date"] | None = None
    split_balance: Literal["span", "count"] | None = None
    split_target_rows: PositiveInt | None = None
    columns: list[Column] = Field(default_factory=list, alias="schema")


class Settings(StrictModel):
    connections: list[Connection]
    tables: list[Table]


Model = TypeVar("Model", bound=StrictModel)


def _parse(model: type[Model], value: object) -> Model:
    try:
        return model.model_validate(value)
    except ValidationError as exc:
        # Pydantic's default error includes input values, including extra secrets.
        raise ContractError("configuration does not match the typed source/table contract") from exc


def _read(name: str) -> dict:
    try:
        value = json.loads(Path(config._config_path(name)).read_text(encoding="utf-8-sig"))
    except FileNotFoundError:
        return {}
    except (OSError, ValueError) as exc:
        raise ContractError("persisted configuration cannot be read") from exc
    if not isinstance(value, dict):
        raise ContractError("persisted configuration must be a JSON object")
    return value


def _store() -> CredentialStore:
    return CredentialStore(config.CREDENTIAL_STORE_PATH or None)


def _connection(raw: dict, defaults: dict, *, default: bool = False) -> dict:
    if not isinstance(raw, dict):
        raise ContractError("native connection must be an object")
    mapping = {
        "query_timeout_seconds": ("query_timeout_seconds", "query_timeout_seconds", defaults.get("query_timeout", 30)),
        "query_max_rows": ("query_max_rows", "query_max_rows", 500_000),
        "max_retries": ("db_max_retries", "db_max_retries", 2),
        "retry_backoff_seconds": ("db_retry_backoff_seconds", "db_retry_backoff_seconds", defaults.get("db_retry_backoff", 0.5)),
        "validate_source_schema": ("validate_source_schema", "validate_source_schema", True),
    }
    if not default and set(raw) - {
        "id", "db_url", "db_url_reference", *(item[0] for item in mapping.values()),
    }:
        raise ContractError("native connection has fields the typed contract cannot represent")
    if raw.get("db_url") or not raw.get("db_url_reference"):
        raise ContractError("legacy database URLs need explicit credential-reference migration")
    result = {
        "id": "default" if default else raw.get("id"),
        "database_url_reference": raw["db_url_reference"],
    }
    for name, (key, fallback_key, fallback) in mapping.items():
        value = raw.get(key, defaults.get(fallback_key, fallback))
        if name == "retry_backoff_seconds" and type(value) is float and value.is_integer():
            value = int(value)
        result[name] = value
    return result


def _table_from_native(raw: dict) -> dict:
    # Parsing below forbids unknown fields rather than projecting them away.
    if not isinstance(raw, dict) or not isinstance(raw.get("schema", []), list):
        raise ContractError("native table must have a column array")
    result = dict(raw)
    columns = []
    for column in result.get("schema", []):
        if not isinstance(column, dict):
            raise ContractError("native column must be an object")
        item = dict(column)
        if item.get("transform") is not None:
            if not isinstance(item["transform"], dict):
                raise ContractError("native transform must be an object")
            transform = dict(item["transform"])
            if "key_reference" in transform:
                raise ContractError("native transforms must use key_ref")
            if "key_ref" in transform:
                transform["key_reference"] = {
                    "kind": "fsp_credential_store", "reference": transform.pop("key_ref"),
                }
            item["transform"] = transform
        columns.append(item)
    result["schema"] = columns
    return result


def read_settings() -> Settings:
    connection_document = _read("config.connection.json")
    default = connection_document.get("connection")
    if not isinstance(default, dict):
        raise ContractError("an explicit default connection section is required")
    named = connection_document.get("connections", [])
    tables = _read("config.tables.json").get("tables")
    if not isinstance(named, list) or not isinstance(tables, list):
        raise ContractError("explicit connections and tables arrays are required")
    try:
        settings = _parse(Settings, {
            "connections": [_connection(default, default, default=True)]
            + [_connection(entry, default) for entry in named],
            "tables": [_table_from_native(entry) for entry in tables],
        })
    except (TypeError, AttributeError) as exc:
        raise ContractError("native configuration is not representable") from exc
    _validate(settings)
    return settings


def _validate(settings: Settings) -> dict[str, str]:
    ids = [connection.id for connection in settings.connections]
    if ids.count("default") != 1 or len(ids) != len(set(ids)):
        raise ContractError("connections must have unique ids and exactly one default")
    from fabric_shortcut_proxy.security.credential_store import env_var_for

    if len({env_var_for(cid) for cid in ids}) != len(ids):
        raise ContractError("connection ids must have distinct native environment names")
    urls = {}
    store = _store()
    for connection in settings.connections:
        try:
            url = resolve_database_reference(
                connection.database_url_reference.model_dump(), store=store,
            )
        except ValueError as exc:
            raise ContractError("database credential reference is absent or unreadable") from exc
        # Native environment overrides must not silently redirect a typed binding.
        override = os.environ.get(env_var_for(connection.id))
        if override is not None and override != url:
            raise ContractError("database environment override conflicts with the saved reference")
        urls[connection.id] = url
    names = [table.name for table in settings.tables]
    if len(names) != len(set(names)):
        raise ContractError("table names must be unique")
    for table in settings.tables:
        if table.connection not in urls:
            raise ContractError("table references an undefined connection")
        from fabric_shortcut_proxy.db.capabilities import capabilities_for_db_url
        from fabric_shortcut_proxy.iceberg.schema import _iceberg_to_pa

        capabilities = capabilities_for_db_url(urls[table.connection])
        for column in table.columns:
            try:
                _iceberg_to_pa(column.type)
            except ValueError as exc:
                raise ContractError("unsupported Iceberg column type") from exc
            transform = column.transform
            if transform and capabilities.tokenization_backend(
                transform.kind, config.TOKENIZATION_FALLBACK,
            ) == "none":
                raise ContractError("column transform is unsupported for this source")
            if transform and transform.key_reference:
                try:
                    ref = credential_record_id(transform.key_reference.reference)
                    env = tokenization_env_var(ref)
                except ValueError as exc:
                    raise ContractError("unsupported tokenization key reference") from exc
                key = store.get_tokenization_key(ref)
                if not key or (os.environ.get(env) and os.environ[env] != key):
                    raise ContractError("tokenization key reference is absent or overridden")
    _, errors = config.validate_setting_updates({
        "db_url": urls["default"],
        "connections": [
            {"id": cid, "db_url": url} for cid, url in urls.items() if cid != "default"
        ],
        "tables": _native_tables(settings),
    })
    if errors:
        raise ContractError("native source/table validation rejected the configuration")
    return urls


def _native_tables(settings: Settings) -> list[dict]:
    tables = []
    for table in settings.tables:
        value = table.model_dump(exclude_none=True, by_alias=True)
        for column in value["schema"]:
            transform = column.get("transform")
            if transform and "key_reference" in transform:
                transform["key_ref"] = transform.pop("key_reference")["reference"]
        tables.append(value)
    return tables


def apply_settings(body: object) -> dict:
    if not isinstance(body, dict) or set(body) != {"settings"}:
        raise ContractError('body must be {"settings": {"connections": [...]} or {"tables": [...]}}')
    updates = body["settings"]
    if not isinstance(updates, dict) or set(updates) not in ({"connections"}, {"tables"}):
        raise ContractError("replace exactly one section per request: connections or tables")
    current = read_settings()
    candidate = _parse(Settings, {**current.model_dump(by_alias=True), **updates})
    _validate(candidate)
    native_tables = _native_tables(candidate)
    native_connections = []
    default_updates = {}
    for connection in candidate.connections:
        native = {
            "id": connection.id,
            "db_url_reference": connection.database_url_reference.model_dump(),
            "query_timeout_seconds": connection.query_timeout_seconds,
            "query_max_rows": connection.query_max_rows,
            "db_max_retries": connection.max_retries,
            "db_retry_backoff_seconds": connection.retry_backoff_seconds,
            "validate_source_schema": connection.validate_source_schema,
        }
        if connection.id == "default":
            default_updates = {
                "db_url": "", "db_url_reference": native["db_url_reference"],
                "query_timeout_seconds": connection.query_timeout_seconds,
                "query_max_rows": connection.query_max_rows,
                "db_max_retries": connection.max_retries,
                "db_retry_backoff_seconds": connection.retry_backoff_seconds,
                "validate_source_schema": connection.validate_source_schema,
            }
        else:
            native_connections.append(native)
    persisted = (
        {**default_updates, "connections": native_connections}
        if "connections" in updates else {"tables": native_tables}
    )
    config.write_config_updates(persisted)
    saved = read_settings()
    if saved != candidate:
        raise ContractError("persisted readback does not match the requested configuration")
    return {
        "ok": True, "settings": saved.model_dump(exclude_none=True, by_alias=True),
        "applied_live": [], "restart_required": list(updates),
    }

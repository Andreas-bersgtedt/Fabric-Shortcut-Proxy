"""Optional runtime module catalog and desired-profile management."""
from __future__ import annotations

import importlib.util
import importlib.metadata
import json
import os
import pathlib
import platform
import sys
import tempfile
from dataclasses import dataclass


@dataclass(frozen=True)
class Module:
    id: str
    extra: str
    capability: str
    packages: tuple[str, ...]
    imports: tuple[str, ...]
    depends_on: tuple[str, ...] = ()
    platforms: tuple[str, ...] = ()


CATALOG: tuple[Module, ...] = (
    Module("postgres", "postgres", "PostgreSQL source", ("asyncpg",), ("asyncpg",)),
    Module("oracle", "oracle", "Oracle source", ("oracledb",), ("oracledb",)),
    Module("redshift", "redshift", "Amazon Redshift source", ("sqlalchemy-redshift", "redshift-connector"), ("sqlalchemy_redshift", "redshift_connector")),
    Module("teradata", "teradata", "Teradata source", ("teradatasqlalchemy",), ("teradatasqlalchemy",)),
    Module("impala", "impala", "Apache Impala source", ("impyla",), ("impala",)),
    Module("s3proxy", "s3proxy", "S3 or MinIO mounted storage", ("boto3",), ("boto3",)),
    Module("azureblob", "azureblob", "Azure Blob or ADLS mounted storage", ("azure-storage-blob", "azure-identity"), ("azure.storage.blob", "azure.identity")),
    Module("keyvault", "keyvault", "Azure Key Vault secret source", ("azure-keyvault-secrets", "azure-identity"), ("azure.keyvault.secrets", "azure.identity")),
    Module("onelake", "onelake", "Fabric OneLake Open Mirroring", ("azure-storage-file-datalake", "azure-identity"), ("azure.storage.filedatalake", "azure.identity")),
    Module("objectstore", "objectstore", "Delta and Iceberg object-store readers", ("deltalake", "pyiceberg"), ("deltalake", "pyiceberg")),
    Module("credentials", "credentials", "Fernet credential encryption", ("cryptography",), ("cryptography",), platforms=("linux", "darwin")),
)

_CATALOG = {module.id: module for module in CATALOG}


def _config_path(filename: str) -> pathlib.Path:
    directory = os.environ.get("FSP_CONFIG_DIR", "").strip()
    return pathlib.Path(directory or ".") / filename


def _read_json(filename: str) -> dict:
    try:
        data = json.loads(_config_path(filename).read_text(encoding="utf-8-sig"))
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def _required_from_config() -> dict[str, str]:
    required: dict[str, str] = {}
    connections = _read_json("config.connection.json")
    for connection in connections.get("connections", []):
        url = str((connection or {}).get("db_url") or "").lower()
        if url.startswith("postgresql+asyncpg"):
            required["postgres"] = "configured PostgreSQL source"
        elif url.startswith("oracle+oracledb"):
            required["oracle"] = "configured Oracle source"
        elif url.startswith("redshift"):
            required["redshift"] = "configured Redshift source"
        elif url.startswith("teradatasql"):
            required["teradata"] = "configured Teradata source"
        elif url.startswith("impala"):
            required["impala"] = "configured Impala source"
    mounts = _read_json("config.mounts.json")
    for mount in mounts.get("mounts", []):
        backend = str((mount or {}).get("backend") or "").lower()
        if backend == "s3":
            required["s3proxy"] = "configured S3 mount"
        elif backend == "azure":
            required["azureblob"] = "configured Azure mount"
    system = _read_json("config.system.json")
    if system.get("keyvault_uri") or os.environ.get("FSP_KEYVAULT_URI"):
        required["keyvault"] = "configured Key Vault"
    if system.get("open_mirror_publish") or os.environ.get("OPEN_MIRROR_PUBLISH"):
        required["onelake"] = "enabled Open Mirroring"
    tables = _read_json("config.tables.json")
    serialized = json.dumps(tables).lower()
    if "deterministic_hash" in serialized or "random_token" in serialized:
        required["objectstore"] = "configured tokenization or object-store reader"
    if platform.system().lower() in ("linux", "darwin"):
        required["credentials"] = "non-Windows encrypted credential backend"
    return required


def _installed(module: Module) -> tuple[bool, dict[str, str]]:
    versions: dict[str, str] = {}
    for package in module.packages:
        try:
            versions[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            return False, versions
    return True, versions


def _active(module: Module, installed: bool) -> tuple[bool, str]:
    if not installed:
        return False, "package missing"
    missing = [name for name in module.imports if importlib.util.find_spec(name) is None]
    if missing:
        return False, "missing import: " + ", ".join(missing)
    return True, "imports available"


def desired_profile() -> list[str]:
    data = _read_json("config.modules.json")
    selected = data.get("modules", {}).get("desired", [])
    if not isinstance(selected, list):
        return []
    return sorted({str(value) for value in selected if str(value) in _CATALOG})


def required_modules() -> dict[str, str]:
    return _required_from_config()


def module_status() -> dict:
    desired = set(desired_profile())
    required = required_modules()
    rows = []
    for module in CATALOG:
        installed, versions = _installed(module)
        active, detail = _active(module, installed)
        rows.append({
            "id": module.id,
            "extra": module.extra,
            "capability": module.capability,
            "packages": list(module.packages),
            "desired": module.id in desired,
            "required": module.id in required,
            "required_reason": required.get(module.id),
            "installed": installed,
            "active": active,
            "detail": detail,
            "versions": versions,
            "platforms": list(module.platforms),
            "restart_required": True,
        })
    return {
        "modules": rows,
        "desired": sorted(desired),
        "required": required,
        "interpreter": sys.executable,
        "python": platform.python_version(),
        "platform": platform.platform(),
        "config_file": str(_config_path("config.modules.json")),
    }


def module_plan(desired: list[str]) -> dict:
    clean = sorted({str(value) for value in desired if str(value) in _CATALOG})
    required = required_modules()
    blocked = sorted(set(required) - set(clean))
    current = set(desired_profile())
    return {
        "desired": clean,
        "required": required,
        "blocked": blocked,
        "add": sorted(set(clean) - current),
        "remove": sorted(current - set(clean)),
        "restart_required": bool(clean != sorted(current)),
    }


def save_desired_profile(desired: list[str]) -> dict:
    plan = module_plan(desired)
    if plan["blocked"]:
        raise ValueError("Cannot disable required modules: " + ", ".join(plan["blocked"]))
    path = _config_path("config.modules.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"schema_version": 1, "modules": {"desired": plan["desired"]}}
    fd, temporary = tempfile.mkstemp(prefix=".config.modules.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
            handle.write("\n")
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return {**plan, "path": str(path)}

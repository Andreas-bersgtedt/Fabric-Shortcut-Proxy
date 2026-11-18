"""Explicitly opt-in cloud conformance fixtures with isolated cleanup."""
from __future__ import annotations

from contextlib import contextmanager
import os
import re
from types import SimpleNamespace
from urllib.parse import urlsplit
import uuid

from fabric_shortcut_proxy.runtime.artifact_store import _normalize_key
from fabric_shortcut_proxy.runtime.azure_artifact_store import AzureBlobArtifactStore
from fabric_shortcut_proxy.runtime.cloud_artifact_store import build_cloud_data_store_from_settings


def selected_live_providers() -> list[str]:
    raw = os.environ.get("FSP_LIVE_ARTIFACT_PROVIDERS", "").strip()
    if not raw:
        return []
    providers = [item.strip().lower() for item in raw.split(",")]
    if any(item not in {"azure", "s3", "gcs"} for item in providers):
        raise ValueError("FSP_LIVE_ARTIFACT_PROVIDERS must contain only azure, s3 or gcs")
    if len(set(providers)) != len(providers):
        raise ValueError("FSP_LIVE_ARTIFACT_PROVIDERS must not contain duplicates")
    return [f"live-{provider}" for provider in providers]


def live_prefix() -> str:
    root = os.environ.get("FSP_LIVE_ARTIFACT_PREFIX", "").strip()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_/-]{2,100}", root):
        raise ValueError("FSP_LIVE_ARTIFACT_PREFIX must be a non-root test prefix")
    if root.endswith("/") or "//" in root:
        raise ValueError("FSP_LIVE_ARTIFACT_PREFIX must contain non-empty path segments")
    return f"{root}/{uuid.uuid4().hex}/"


class ScopedAzureContainer:
    """Keep all store operations, including its mutation lock, under one test prefix."""

    def __init__(self, container, prefix: str):
        self.container = container
        self.prefix = prefix

    def get_blob_client(self, name: str):
        return self.container.get_blob_client(self.prefix + _normalize_key(name))

    def _objects(self, objects):
        for item in objects:
            if not item.name.startswith(self.prefix):
                raise RuntimeError("Azure test listing escaped its isolated prefix")
            yield SimpleNamespace(
                name=item.name[len(self.prefix):],
                size=getattr(item, "size", 0),
                last_modified=getattr(item, "last_modified", None),
                etag=getattr(item, "etag", None),
            )

    def list_blobs(self, *, name_starts_with=""):
        return self._objects(
            self.container.list_blobs(name_starts_with=self.prefix + name_starts_with)
        )

    def walk_blobs(self, *, name_starts_with="", delimiter="/"):
        return self._objects(
            self.container.walk_blobs(
                name_starts_with=self.prefix + name_starts_with, delimiter=delimiter,
            )
        )


def cleanup_azure(container, prefix: str) -> None:
    def names():
        return [
            item.name for item in container.list_blobs(
                name_starts_with=prefix, include=["uncommittedblobs"],
            )
        ]

    keys = names()
    if any(not key.startswith(prefix) for key in keys):
        raise RuntimeError("Azure test cleanup escaped its isolated prefix")
    for key in dict.fromkeys(keys):
        container.delete_blob(key)
    if names():
        raise RuntimeError("Azure test cleanup left objects in its isolated prefix")


def cleanup_cloud(store) -> None:
    for item in store.list():
        if not store.delete(item.key):
            raise RuntimeError("cloud test object disappeared during cleanup")
    if store.list():
        raise RuntimeError("cloud test cleanup left objects in its isolated prefix")


@contextmanager
def live_store(provider: str):
    prefix = live_prefix()
    if provider == "azure":
        from azure.identity import DefaultAzureCredential
        from azure.storage.blob import BlobServiceClient

        account_url = os.environ.get("FSP_LIVE_AZURE_ACCOUNT_URL", "").strip()
        container_name = os.environ.get("FSP_LIVE_AZURE_CONTAINER", "").strip()
        url = urlsplit(account_url)
        if (
            url.scheme != "https" or not url.hostname or url.username or url.password
            or url.query or url.fragment or url.path not in {"", "/"}
            or not container_name or "/" in container_name
        ):
            raise ValueError("live Azure acceptance requires a credential-free account URL and container")
        credential = DefaultAzureCredential()
        client = BlobServiceClient(account_url=account_url, credential=credential)
        container = client.get_container_client(container_name)
        store = AzureBlobArtifactStore(container=ScopedAzureContainer(container, prefix))
        try:
            yield store
        finally:
            try:
                try:
                    cleanup_azure(container, prefix)
                except Exception as exc:
                    raise RuntimeError(f"live Azure cleanup failed under {prefix}") from exc
            finally:
                client.close()
                credential.close()
    elif provider in {"s3", "gcs"}:
        settings = {
            "bucket": os.environ.get(f"FSP_LIVE_{provider.upper()}_BUCKET", "").strip(),
            "prefix": prefix,
        }
        if provider == "s3":
            settings["region"] = os.environ.get("FSP_LIVE_S3_REGION", "").strip()
        store = build_cloud_data_store_from_settings(provider, settings)
        try:
            yield store
        finally:
            try:
                cleanup_cloud(store)
            except Exception as exc:
                raise RuntimeError(f"live {provider} cleanup failed under {prefix}") from exc
    else:
        raise ValueError("unsupported live artifact provider")

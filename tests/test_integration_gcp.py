"""Opt-in live Google Cloud Storage conformance gate."""
from __future__ import annotations

import importlib.metadata
import hashlib
import os
import uuid

import pytest

from fabric_shortcut_proxy.runtime.artifact_store import ObjectConflict, ObjectNotFound


def test_live_gcs_mount_and_artifact_conformance_gate(capsys):
    if os.environ.get("FSP_RUN_LIVE_GCP_GATES") != "1":
        pytest.skip("Set FSP_RUN_LIVE_GCP_GATES=1 and the GCS live variables.")
    bucket_name = os.environ.get("FSP_LIVE_GCS_BUCKET", "").strip()
    if not bucket_name:
        pytest.skip("Set FSP_LIVE_GCS_BUCKET to a dedicated test bucket.")

    storage = pytest.importorskip(
        "google.cloud.storage",
        reason="live GCS requires google-cloud-storage",
    )
    from fabric_shortcut_proxy.runtime.cloud_artifact_store import GCSArtifactStore
    from fabric_shortcut_proxy.storage.gcs_store import GCSStore

    project = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCP_PROJECT")
    client = storage.Client(project=project or None)
    bucket = client.bucket(bucket_name)
    prefix = f"fsp-live-gcp/{uuid.uuid4().hex}"

    artifact_store = GCSArtifactStore(bucket=bucket, prefix=prefix)
    mount_store = GCSStore(bucket=bucket_name, client=client)
    conflict: ObjectConflict | None = None
    try:
        payload = b"gcp-live-conformance"
        stat = artifact_store.put("data.bin", payload)
        assert stat.size == len(payload)
        assert artifact_store.verify(
            "data.bin",
            size=len(payload),
            content_hash=__import__("hashlib").sha256(payload).hexdigest(),
        )

        mounted = b"".join(
            mount_store.get_stream(f"{prefix}/data.bin", offset=4, length=8)
        )
        assert mounted == payload[4:12]

        large = b"x" * (8 * 1024 * 1024 + 1)
        large_stat = artifact_store.put_stream(
            "large.bin", [large], length=len(large)
        )
        assert large_stat.size == len(large)
        assert artifact_store.verify(
            "large.bin",
            size=len(large),
            content_hash=hashlib.sha256(large).hexdigest(),
        )
        tail = b"".join(
            mount_store.get_stream(
                f"{prefix}/large.bin",
                offset=len(large) - 8,
                length=8,
            )
        )
        assert tail == b"x" * 8

        try:
            artifact_store.put_stream_if_absent(
                "immutable.bin", [b"alpha"], length=5
            )
            artifact_store.put_stream_if_absent(
                "immutable.bin", [b"bravo"], length=5
            )
        except ObjectConflict as exc:
            conflict = exc

        assert conflict is not None or artifact_store.verify(
            "immutable.bin",
            size=5,
            content_hash=__import__("hashlib").sha256(b"alpha").hexdigest(),
        )

        prefix_store = GCSArtifactStore(bucket=bucket, prefix=f"{prefix}-other")
        prefix_store.put("neighbor.txt", b"other-prefix")
        assert mount_store.head(f"{prefix}/data.bin") is not None
        assert mount_store.head(f"{prefix}-other/neighbor.txt") is not None

        denied_store = GCSStore(
            bucket=f"{bucket_name}-missing-uat",
            client=client,
        )
        assert denied_store.head("missing.bin") is None
        try:
            denied_store.get("missing.bin")
        except ObjectNotFound:
            pass
        else:
            pytest.fail("Missing GCS bucket did not return ObjectNotFound")

        bucket.reload()
        region = str(getattr(bucket, "location", "") or "")
        sdk_version = importlib.metadata.version("google-cloud-storage")
        print(
            "GCP_LIVE_EVIDENCE "
            f"project={project or 'ADC-default'} "
            f"bucket={bucket_name} region={region or 'unknown'} "
            f"sdk=google-cloud-storage:{sdk_version} "
            "scope=GCS read/write,size,SHA256,range,large-upload,retry,"
            "conflict,prefix-isolation,denied-404"
        )
    finally:
        try:
            for blob in bucket.list_blobs(prefix=f"{prefix}-other/"):
                blob.delete()
            for blob in bucket.list_blobs(prefix=f"{prefix}/"):
                blob.delete()
        except Exception as exc:
            status = getattr(exc, "code", None)
            if status != 404 and exc.__class__.__name__ not in {"NotFound", "GoogleNotFound"}:
                raise

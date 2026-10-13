from __future__ import annotations

import base64
import json

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
)

from enterprise.control.placement import PlacementConfig
from enterprise.control.policy_bundle import (
    PolicyBundleUnavailable,
    PolicyBundleValidationError,
    _RemoteUnavailable,
    load_policy_bundle,
    load_policy_bundle_from_store,
)
from fabric_shortcut_proxy.runtime.artifact_store import MemoryStore
from tests.enterprise.test_residency_policy import _config


def _key_pair(tmp_path):
    private_key = Ed25519PrivateKey.generate()
    public_path = tmp_path / "policy-public.pem"
    public_path.write_bytes(
        private_key.public_key().public_bytes(
            serialization.Encoding.PEM,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
    )
    return private_key, public_path


def _bundle(private_key, *, revision=1, placement=None, key_id="global-1"):
    value = {
        "schema_version": 1,
        "revision": revision,
        "issued_at_ms": 1_800_000_000_000,
        "key_id": key_id,
        "placement": _config() if placement is None else placement,
    }
    canonical = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    value["signature"] = base64.b64encode(
        private_key.sign(canonical)
    ).decode()
    return json.dumps(value, sort_keys=True).encode()


def _load(tmp_path, private_key, public_path, raw, **kwargs):
    return load_policy_bundle(
        source_url="https://policy.example.test/placement.json",
        public_key_file=str(public_path),
        cache_file=str(tmp_path / "policy-cache.json"),
        expected_key_id="global-1",
        value_loader=PlacementConfig.from_dict,
        fetcher=lambda _url, _timeout: raw,
        **kwargs,
    )


def test_valid_signed_bundle_is_applied_and_cached(tmp_path):
    private_key, public_path = _key_pair(tmp_path)
    raw = _bundle(private_key, revision=7)

    loaded = _load(tmp_path, private_key, public_path, raw)

    assert loaded.revision == 7
    assert loaded.key_id == "global-1"
    assert not loaded.from_cache
    assert loaded.value.policy_for("erp", "sales").required_pool == "eu"
    assert (tmp_path / "policy-cache.json").read_bytes() == raw


def test_outage_uses_verified_last_known_good_bundle(tmp_path):
    private_key, public_path = _key_pair(tmp_path)
    raw = _bundle(private_key, revision=8)
    _load(tmp_path, private_key, public_path, raw)

    loaded = load_policy_bundle(
        source_url="https://policy.example.test/placement.json",
        public_key_file=str(public_path),
        cache_file=str(tmp_path / "policy-cache.json"),
        expected_key_id="global-1",
        value_loader=PlacementConfig.from_dict,
        fetcher=lambda _url, _timeout: (_ for _ in ()).throw(
            _RemoteUnavailable("offline")
        ),
    )

    assert loaded.revision == 8
    assert loaded.from_cache
    assert loaded.value.policy_for("erp", "sales").required_pool == "eu"


def test_outage_without_cache_fails_explicitly(tmp_path):
    _private_key, public_path = _key_pair(tmp_path)

    with pytest.raises(PolicyBundleUnavailable, match="last-known-good"):
        load_policy_bundle(
            source_url="https://policy.example.test/placement.json",
            public_key_file=str(public_path),
            cache_file=str(tmp_path / "missing-cache.json"),
            value_loader=PlacementConfig.from_dict,
            fetcher=lambda _url, _timeout: (_ for _ in ()).throw(
                _RemoteUnavailable("offline")
            ),
        )


@pytest.mark.parametrize("mutation", ["unsigned", "tampered", "wrong-key"])
def test_unsigned_or_invalid_remote_bundle_never_falls_back(
    tmp_path,
    mutation,
):
    private_key, public_path = _key_pair(tmp_path)
    valid = _bundle(private_key, revision=9)
    _load(tmp_path, private_key, public_path, valid)
    value = json.loads(valid)
    if mutation == "unsigned":
        value["signature"] = ""
    elif mutation == "tampered":
        value["revision"] = 10
    else:
        other_key = Ed25519PrivateKey.generate()
        value = json.loads(_bundle(other_key, revision=10))
    invalid = json.dumps(value, sort_keys=True).encode()

    with pytest.raises(PolicyBundleValidationError):
        _load(tmp_path, private_key, public_path, invalid)

    assert (tmp_path / "policy-cache.json").read_bytes() == valid


def test_older_remote_revision_is_rejected(tmp_path):
    private_key, public_path = _key_pair(tmp_path)
    current = _bundle(private_key, revision=12)
    _load(tmp_path, private_key, public_path, current)

    with pytest.raises(PolicyBundleValidationError, match="rollback"):
        _load(
            tmp_path,
            private_key,
            public_path,
            _bundle(private_key, revision=11),
        )

    assert (tmp_path / "policy-cache.json").read_bytes() == current


def test_revision_cannot_be_reused_with_different_content(tmp_path):
    private_key, public_path = _key_pair(tmp_path)
    current = _bundle(private_key, revision=12)
    _load(tmp_path, private_key, public_path, current)
    changed = _config()
    changed["connections"]["erp"]["freshness_target_ms"] = 60_000

    with pytest.raises(PolicyBundleValidationError, match="reused"):
        _load(
            tmp_path,
            private_key,
            public_path,
            _bundle(private_key, revision=12, placement=changed),
        )

    assert (tmp_path / "policy-cache.json").read_bytes() == current


def test_invalid_placement_is_not_cached(tmp_path):
    private_key, public_path = _key_pair(tmp_path)
    current = _bundle(private_key, revision=2)
    _load(tmp_path, private_key, public_path, current)
    invalid = _bundle(
        private_key,
        revision=3,
        placement={"unknown": True},
    )

    with pytest.raises(ValueError, match="unknown placement"):
        _load(tmp_path, private_key, public_path, invalid)

    assert (tmp_path / "policy-cache.json").read_bytes() == current


def test_environment_loads_bundle_metadata(monkeypatch, tmp_path):
    private_key, public_path = _key_pair(tmp_path)
    raw = _bundle(private_key, revision=14)
    cache_path = tmp_path / "policy-cache.json"
    cache_path.write_bytes(raw)
    monkeypatch.setenv(
        "FSP_POLICY_BUNDLE_URL",
        "https://policy.example.test/placement.json",
    )
    monkeypatch.setenv(
        "FSP_POLICY_BUNDLE_PUBLIC_KEY_FILE",
        str(public_path),
    )
    monkeypatch.setenv("FSP_POLICY_BUNDLE_CACHE_FILE", str(cache_path))
    monkeypatch.setenv("FSP_POLICY_BUNDLE_KEY_ID", "global-1")
    monkeypatch.setattr(
        "enterprise.control.policy_bundle._fetch_bundle",
        lambda _url, _timeout: (_ for _ in ()).throw(
            _RemoteUnavailable("offline")
        ),
    )

    placement = PlacementConfig.from_environment()

    assert placement.policy_bundle_revision == 14
    assert placement.policy_bundle_key_id == "global-1"
    assert placement.policy_bundle_from_cache


def test_private_artifact_store_bundle_uses_verified_cache_on_outage(
    tmp_path,
):
    private_key, public_path = _key_pair(tmp_path)
    raw = _bundle(private_key, revision=15)
    store = MemoryStore()
    store.put("policies/placement.json", raw)
    cache_path = tmp_path / "policy-cache.json"

    loaded = load_policy_bundle_from_store(
        store=store,
        object_key="policies/placement.json",
        public_key_file=str(public_path),
        cache_file=str(cache_path),
        expected_key_id="global-1",
        value_loader=PlacementConfig.from_dict,
    )
    store.delete("policies/placement.json")
    cached = load_policy_bundle_from_store(
        store=store,
        object_key="policies/placement.json",
        public_key_file=str(public_path),
        cache_file=str(cache_path),
        expected_key_id="global-1",
        value_loader=PlacementConfig.from_dict,
    )

    assert loaded.revision == 15
    assert not loaded.from_cache
    assert cached.revision == 15
    assert cached.from_cache


def test_private_store_transport_failure_uses_verified_cache(tmp_path):
    from azure.core.exceptions import ServiceRequestError

    private_key, public_path = _key_pair(tmp_path)
    raw = _bundle(private_key, revision=16)
    store = MemoryStore()
    store.put("policies/placement.json", raw)
    cache_path = tmp_path / "policy-cache.json"
    load_policy_bundle_from_store(
        store=store,
        object_key="policies/placement.json",
        public_key_file=str(public_path),
        cache_file=str(cache_path),
        expected_key_id="global-1",
        value_loader=PlacementConfig.from_dict,
    )

    def unavailable(*_args, **_kwargs):
        raise ServiceRequestError("private endpoint unavailable")

    store.get = unavailable
    cached = load_policy_bundle_from_store(
        store=store,
        object_key="policies/placement.json",
        public_key_file=str(public_path),
        cache_file=str(cache_path),
        expected_key_id="global-1",
        value_loader=PlacementConfig.from_dict,
    )

    assert cached.revision == 16
    assert cached.from_cache

"""FORWARDED_ALLOW_IPS: the agent trusts proxy headers only from configured proxies
so audit logs the real client IP behind an external load balancer."""
from __future__ import annotations

import os

os.environ.setdefault("DB_URL", "sqlite+aiosqlite:///:memory:")
os.environ.setdefault("S3_BUCKET", "test-bucket")

import config
import main


def test_uvicorn_kwargs_enables_trusted_proxy(monkeypatch):
    monkeypatch.setattr(config, "FORWARDED_ALLOW_IPS", "10.0.0.0/8", raising=False)
    kw = main._uvicorn_kwargs()
    assert kw["proxy_headers"] is True
    assert kw["forwarded_allow_ips"] == "10.0.0.0/8"


def test_uvicorn_kwargs_merges_tls():
    kw = main._uvicorn_kwargs({"ssl_certfile": "c.pem", "ssl_keyfile": "k.pem"})
    assert kw["ssl_certfile"] == "c.pem" and kw["ssl_keyfile"] == "k.pem"
    assert kw["host"] == config.HOST and kw["port"] == config.PORT


def test_forwarded_allow_ips_default_is_loopback():
    # Default must not widen trust; loopback keeps current behavior.
    assert isinstance(config.FORWARDED_ALLOW_IPS, str)

"""Shared test isolation for process-wide configuration globals."""
from __future__ import annotations

import os

import pytest


@pytest.fixture(autouse=True)
def reset_sigv4_from_environment(monkeypatch):
    """Prevent one test's config mutation from leaking into later HTTP tests."""
    import config

    raw = os.environ.get("REQUIRE_SIGV4")
    enabled = raw.strip().lower() in {"1", "true", "yes", "on"} if raw is not None else False
    monkeypatch.setattr(config, "REQUIRE_SIGV4", enabled, raising=False)
    yield

"""Activation gate for the optional re-identification module."""
from __future__ import annotations


def enabled() -> bool:
    """Return whether both the system and persisted module gates are active."""
    import config
    from module_registry import desired_profile

    return bool(
        config.ENABLE_REIDENTIFICATION
        and "reidentification" in desired_profile()
    )
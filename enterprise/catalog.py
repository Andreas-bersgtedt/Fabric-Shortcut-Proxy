"""Global federated catalog service entry point."""
from __future__ import annotations

from fabric_shortcut_proxy import config
from enterprise.control.catalog_app import app


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host=config.CONTROL_HOST,
        port=config.CONTROL_PORT,
        log_level="info",
    )

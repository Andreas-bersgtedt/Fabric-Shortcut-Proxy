"""
Manager / Controller entry point (Phase 1).

    python -m enterprise.manager

Starts the control‑plane REST server on ``CONTROL_HOST:CONTROL_PORT`` and
supervises a single local Agent (the S3 server, ``main.py``), restarting it on
crash. The Agent serves the Fabric‑facing S3 data plane on ``PORT`` exactly as
before — point your Fabric shortcut at the Agent, not the Manager.

Standalone mode (no Manager) is still just ``python main.py``.
"""
from __future__ import annotations

# Hydrate DB credentials from the Manager's encrypted credential store into the
# process environment BEFORE config is imported, so both the Manager and the
# Agents it spawns see the full (password-bearing) DB URLs. An env var already
# set (e.g. via -DbUrl) always wins; the store only fills what is missing.
from security.credential_store import hydrate_environment

hydrate_environment()

import config
from enterprise.control.manager_app import app


if __name__ == "__main__":
    import uvicorn

    _tls = {}
    if config.TLS_CERT_FILE and config.TLS_KEY_FILE:
        _tls = {"ssl_certfile": config.TLS_CERT_FILE, "ssl_keyfile": config.TLS_KEY_FILE}
    server = uvicorn.Server(
        uvicorn.Config(app, host=config.CONTROL_HOST, port=config.CONTROL_PORT, log_level="info", **_tls)
    )
    # Expose the server so the /_manager "Shutdown" action can request a graceful
    # exit (stops all Agents via the lifespan, then quits the Manager).
    app.state.uvicorn_server = server
    server.run()

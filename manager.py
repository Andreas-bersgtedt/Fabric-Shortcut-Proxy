"""
Manager / Controller entry point (Phase 1).

    python manager.py

Starts the control‑plane REST server on ``CONTROL_HOST:CONTROL_PORT`` and
supervises a single local Agent (the S3 server, ``main.py``), restarting it on
crash. The Agent serves the Fabric‑facing S3 data plane on ``PORT`` exactly as
before — point your Fabric shortcut at the Agent, not the Manager.

Standalone mode (no Manager) is still just ``python main.py``.
"""
from __future__ import annotations

import config
from control.manager_app import app


if __name__ == "__main__":
    import uvicorn

    server = uvicorn.Server(
        uvicorn.Config(app, host=config.CONTROL_HOST, port=config.CONTROL_PORT, log_level="info")
    )
    # Expose the server so the /_manager "Shutdown" action can request a graceful
    # exit (stops all Agents via the lifespan, then quits the Manager).
    app.state.uvicorn_server = server
    server.run()

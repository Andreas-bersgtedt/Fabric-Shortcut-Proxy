"""Isolation contract: the Lite core must never import the enterprise package.

Importing ``main`` (the single-node entry point) must not pull in ``enterprise``
or ``enterprise.control`` at module-import time. The cluster hooks in main.py are
lazy and flag-guarded, so a Lite-only install (where the enterprise package is not
present at all) imports and runs cleanly. This test fails the moment a core module
grows a top-level import into the enterprise surface.
"""
from __future__ import annotations

import os
import subprocess
import sys
import textwrap


def test_lite_core_does_not_import_enterprise():
    code = textwrap.dedent(
        """
        import sys
        import main  # noqa: F401

        leaked = sorted(
            name for name in sys.modules
            if name == "enterprise" or name.startswith("enterprise.")
            or name == "control" or name.startswith("control.")
        )
        if leaked:
            print("LEAKED:" + ",".join(leaked))
            sys.exit(1)
        """
    )
    env = dict(os.environ)
    env["DB_URL"] = "sqlite+aiosqlite:///:memory:"
    env.setdefault("S3_BUCKET", "test-bucket")
    # Cluster flags off: the guarded enterprise imports must not fire even so.
    env.pop("MANAGER_URL", None)
    env.pop("RETENTION_GC", None)

    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    proc = subprocess.run(
        [sys.executable, "-c", code],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, (
        "Lite core imported enterprise/cluster modules at import time:\n"
        + proc.stdout
        + proc.stderr
    )

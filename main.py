"""Source-checkout launcher retained for existing deployment commands."""
from __future__ import annotations

import runpy
import sys
from pathlib import Path

_source_root = str(Path(__file__).resolve().parent / "src")
if _source_root not in sys.path:
    sys.path.insert(0, _source_root)

if __name__ == "__main__":
    runpy.run_module("fabric_shortcut_proxy.main", run_name="__main__", alter_sys=True)
else:
    from fabric_shortcut_proxy import main as _implementation

    sys.modules[__name__] = _implementation

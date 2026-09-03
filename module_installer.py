"""Controlled dependency installation for the Config Builder module profile."""
from __future__ import annotations

import asyncio
import os
import re
import sys
from datetime import datetime, timezone

from module_registry import module_plan, save_desired_profile


_STATE: dict = {
    "status": "idle",
    "operation_id": None,
    "started_at": None,
    "finished_at": None,
    "returncode": None,
    "output": "",
    "error": None,
}
_TASK: asyncio.Task | None = None
_SECRET_PATTERN = re.compile(r"(?i)(://[^:/\s]+:)[^@/\s]+(@)")


def status() -> dict:
    return dict(_STATE)


def _redact(text: str) -> str:
    return _SECRET_PATTERN.sub(r"\1***\2", text)[-12000:]


async def _install(desired: list[str], operation_id: str) -> None:
    global _STATE
    _STATE.update({
        "status": "running",
        "operation_id": operation_id,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "finished_at": None,
        "returncode": None,
        "output": "",
        "error": None,
    })
    try:
        result = save_desired_profile(desired)
        extras = ",".join(result["desired"])
        requirement = ".[" + extras + "]" if extras else "."
        command = [sys.executable, "-m", "pip", "install", "-e", requirement, "-e", "./enterprise"]
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            cwd=os.getcwd(),
        )
        output, _ = await process.communicate()
        _STATE["returncode"] = process.returncode
        _STATE["output"] = _redact(output.decode(errors="replace"))
        _STATE["status"] = "succeeded" if process.returncode == 0 else "failed"
        if process.returncode != 0:
            _STATE["error"] = "Dependency installation failed; the Manager was not restarted."
    except Exception as exc:  # noqa: BLE001 - retain operation state for the UI
        _STATE.update({"status": "failed", "error": _redact(str(exc))})
    finally:
        _STATE["finished_at"] = datetime.now(timezone.utc).isoformat()


def start_install(desired: list[str]) -> dict:
    global _TASK
    if _TASK is not None and not _TASK.done():
        raise ValueError("A dependency installation is already running")
    plan = module_plan(desired)
    if plan["blocked"]:
        raise ValueError("Cannot disable required modules: " + ", ".join(plan["blocked"]))
    operation_id = datetime.now(timezone.utc).strftime("module-%Y%m%dT%H%M%S%fZ")
    _TASK = asyncio.create_task(_install(plan["desired"], operation_id))
    return {"status": "accepted", "operation_id": operation_id, **plan}

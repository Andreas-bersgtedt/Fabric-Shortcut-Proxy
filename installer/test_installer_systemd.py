"""Regression tests for the POSIX installer boundary."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest


INSTALLER = Path(__file__).with_name("install.sh")


pytestmark = pytest.mark.skipif(os.name == "nt", reason="installer requires POSIX utilities")


def run_installer(tmp_path: Path, *args: str) -> subprocess.CompletedProcess[str]:
    answers = tmp_path / "answers"
    answers.write_text(
        "\n".join(
            [
                "APPLY=APPLY",
                "install_dir=/opt/fabric-shortcut-proxy",
                "service_user=fsp",
                "service_group=fsp",
                "unit_name=fabric-shortcut-proxy.service",
                "identity_mode=managed_identity",
                "keyvault_mode=disabled",
                "secret_backend=env-file",
                "tls_mode=disabled",
                "start_service=no",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    env = os.environ.copy()
    env["FSP_INSTALLER_STATE_DIR"] = str(tmp_path / "state")
    return subprocess.run(
        ["/bin/sh", str(INSTALLER), "--no-color", "--answers", str(answers), *args],
        text=True,
        capture_output=True,
        env=env,
        check=False,
    )


def test_dry_run_does_not_write_state(tmp_path: Path) -> None:
    result = run_installer(tmp_path, "--dry-run")

    assert result.returncode == 0, result.stderr
    assert not (tmp_path / "state").exists()
    assert "Dry-run complete" in result.stdout


def test_resume_rejects_corrupt_state(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    (state_dir / "installer-state").write_text("not-a-state\n", encoding="utf-8")

    result = run_installer(tmp_path, "--resume")

    assert result.returncode != 0
    assert "corrupt or incompatible" in result.stderr


def test_answers_reject_unknown_keys(tmp_path: Path) -> None:
    answers = tmp_path / "invalid-answers"
    answers.write_text("APPLY=APPLY\nunknown=value\n", encoding="utf-8")
    env = os.environ.copy()
    env["FSP_INSTALLER_STATE_DIR"] = str(tmp_path / "state")

    result = subprocess.run(
        ["/bin/sh", str(INSTALLER), "--no-color", "--answers", str(answers), "--dry-run"],
        text=True,
        capture_output=True,
        env=env,
        check=False,
    )

    assert result.returncode != 0
    assert "answers file validation failed" in result.stderr

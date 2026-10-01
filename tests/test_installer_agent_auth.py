from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SHELL_INSTALLER = (ROOT / "installer" / "install.sh").read_text(encoding="utf-8")
CPP_INSTALLER = (ROOT / "installer" / "main.cpp").read_text(encoding="utf-8")


def test_shell_installer_enables_and_consumes_agent_token_by_default() -> None:
    assert "GENERATE_AGENT_TOKEN=yes" in SHELL_INSTALLER
    assert "'AGENT_AUTH_MODE=required'" in SHELL_INSTALLER
    assert '"AGENT_TOKEN=$AGENT_TOKEN_VALUE"' in SHELL_INSTALLER
    assert "set_keyvault_secret agent-token" in SHELL_INSTALLER
    assert 'printf \'%s\\n\' "EnvironmentFile=$ENV_FILE"' in SHELL_INSTALLER
    assert "unused AGENT_TOKEN" not in SHELL_INSTALLER


def test_installers_accept_rotation_references_without_serializing_values() -> None:
    for key in (
        "agent_token_reference",
        "agent_token_previous_reference",
        "agent_token_previous_valid_until",
    ):
        assert key in SHELL_INSTALLER
        assert f'"{key}"' in CPP_INSTALLER

    assert "AGENT_TOKEN_PREVIOUS=$AGENT_TOKEN_PREVIOUS_VALUE" in SHELL_INSTALLER
    assert "set_keyvault_secret agent-token-previous" in SHELL_INSTALLER
    assert "AGENT_TOKEN_VALUE" not in SHELL_INSTALLER.split("save_state() {", 1)[1].split(
        "load_state() {", 1
    )[0]
    assert "AGENT_TOKEN_PREVIOUS_VALUE" not in SHELL_INSTALLER.split(
        "save_state() {", 1
    )[1].split("load_state() {", 1)[0]


def test_cpp_installer_defaults_to_active_agent_credential() -> None:
    assert (
        '"Generate active Agent credential (yes/no)", "yes", {"no", "yes"}'
        in CPP_INSTALLER
    )
    assert "unused AGENT_TOKEN" not in CPP_INSTALLER

"""Offline regressions for the dedicated live-source fixture initializer."""
from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy.dialects.oracle import dialect
from sqlalchemy.exc import DBAPIError


_PATH = (
    Path(__file__).parent
    / "fixtures"
    / "source_capabilities"
    / "seed_local_fixture.py"
)
_SPEC = importlib.util.spec_from_file_location("source_capability_fixture", _PATH)
assert _SPEC is not None and _SPEC.loader is not None
fixture = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(fixture)


class FixtureConnection:
    def __init__(self, error: DBAPIError):
        self.dialect = dialect()
        self.error = error
        self.statements: list[str] = []

    def execute(self, statement, params=None):
        sql = str(statement)
        self.statements.append(sql)
        if sql.startswith("DROP "):
            raise self.error


def _oracle_error(code: int | None, message: str) -> DBAPIError:
    diagnostic = SimpleNamespace(code=code, message=message)
    return DBAPIError("DROP VIEW", {}, Exception(diagnostic))


def test_oracle_seed_ignores_structured_missing_object_code(monkeypatch):
    monkeypatch.setenv("ORACLE_USERNAME", "fsp94")
    connection = FixtureConnection(
        _oracle_error(
            942,
            'ORA-00942: table or view "FSP94"."ISSUE94_ROWS_VIEW" does not exist',
        )
    )

    assert fixture._seed_oracle(connection) == (
        "FSP94.ISSUE94_ROWS",
        "FSP94.ISSUE94_ROWS_VIEW",
    )
    assert len(connection.statements) == 5
    assert connection.statements[-1].startswith("CREATE VIEW ")


@pytest.mark.parametrize("code", [1031, None])
def test_oracle_seed_does_not_ignore_other_or_unstructured_errors(monkeypatch, code):
    monkeypatch.setenv("ORACLE_USERNAME", "fsp94")
    error = _oracle_error(code, "ORA-00942: table or view does not exist")
    connection = FixtureConnection(error)

    with pytest.raises(DBAPIError) as raised:
        fixture._seed_oracle(connection)

    assert raised.value is error
    assert len(connection.statements) == 1

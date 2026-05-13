"""Tests for ``microbus-py manifest``."""

from __future__ import annotations

import sys
import textwrap
from typing import TYPE_CHECKING

import yaml
from click.testing import CliRunner

from microbus_py.cli.main import main

if TYPE_CHECKING:
    from pathlib import Path


_SVC_SRC = textwrap.dedent(
    """
    from pydantic import BaseModel
    from microbus_py import Connector, InMemoryBroker

    class _In(BaseModel):
        a: int

    class _Out(BaseModel):
        b: int

    svc = Connector(hostname="cli.test", description="Test", transport=InMemoryBroker())

    @svc.function(route="/echo", method="POST", description="echoes")
    async def echo(inp: _In) -> _Out:
        return _Out(b=inp.a)
    """
)


def _write_module(tmp_path: Path, name: str = "fakemod") -> str:
    pkg = tmp_path / name
    pkg.mkdir()
    (pkg / "__init__.py").write_text(_SVC_SRC)
    sys.path.insert(0, str(tmp_path))
    return name


def test_manifest_cmd_prints_yaml(tmp_path: Path) -> None:
    name = _write_module(tmp_path, "manifest_test_a")
    try:
        runner = CliRunner()
        result = runner.invoke(main, ["manifest", name])
        assert result.exit_code == 0, result.output
        parsed = yaml.safe_load(result.output)
        assert parsed["general"]["hostname"] == "cli.test"
        assert "echo" in parsed["functions"]
        assert parsed["functions"]["echo"]["description"] == "echoes"
    finally:
        sys.path.remove(str(tmp_path))
        sys.modules.pop(name, None)


def test_manifest_cmd_module_missing_svc_errors(tmp_path: Path) -> None:
    pkg = tmp_path / "manifest_test_b"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("# no svc\n")
    sys.path.insert(0, str(tmp_path))
    try:
        runner = CliRunner()
        result = runner.invoke(main, ["manifest", "manifest_test_b"])
        assert result.exit_code != 0
        assert "svc" in result.output.lower()
    finally:
        sys.path.remove(str(tmp_path))
        sys.modules.pop("manifest_test_b", None)


def test_manifest_cmd_unimportable_module_errors(tmp_path: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["manifest", "no_such_module_xyz_123"])
    assert result.exit_code != 0

"""Tests for the ``microbus-py`` CLI entry point and ``run`` command."""

from __future__ import annotations

import asyncio
import os
import signal as sigmod
import sys
import textwrap
from typing import TYPE_CHECKING

from click.testing import CliRunner

from microbus_py.cli import run_cmd
from microbus_py.cli.main import main
from microbus_py.cli.run_cmd import _wait_forever

if TYPE_CHECKING:
    from pathlib import Path

    import pytest


def test_main_help() -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["--help"])
    assert result.exit_code == 0
    assert "scaffold" in result.output
    assert "manifest" in result.output
    assert "run" in result.output


def test_main_no_args_shows_help() -> None:
    runner = CliRunner()
    result = runner.invoke(main, [])
    assert result.exit_code in (0, 2)
    assert "Usage" in result.output


_FAKE_SVC_SRC = textwrap.dedent(
    """
    class _Fake:
        def __init__(self) -> None:
            self.startup_calls = 0
            self.shutdown_calls = 0
        async def startup(self) -> None:
            self.startup_calls += 1
        async def shutdown(self) -> None:
            self.shutdown_calls += 1

    svc = _Fake()
    """
)


def test_run_cmd_invokes_startup_and_shutdown(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pkg_name = "run_cmd_test_pkg"
    pkg = tmp_path / pkg_name
    pkg.mkdir()
    (pkg / "__init__.py").write_text(_FAKE_SVC_SRC)
    sys.path.insert(0, str(tmp_path))
    sys.modules.pop(pkg_name, None)

    captured: dict[str, object] = {}

    async def fake_wait_forever() -> None:
        captured["waited"] = True

    monkeypatch.setattr(run_cmd, "_wait_forever", fake_wait_forever)

    try:
        runner = CliRunner()
        result = runner.invoke(main, ["run", pkg_name])
        assert result.exit_code == 0, result.output
        mod = sys.modules[pkg_name]
        svc = mod.svc
        assert svc.startup_calls == 1
        assert svc.shutdown_calls == 1
        assert captured.get("waited") is True
    finally:
        sys.path.remove(str(tmp_path))
        sys.modules.pop(pkg_name, None)


def test_run_cmd_unimportable_module_errors(tmp_path: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["run", "absolutely_no_such_mod_qq"])
    assert result.exit_code != 0


def test_run_cmd_module_missing_svc_errors(tmp_path: Path) -> None:
    pkg_name = "run_cmd_no_svc_pkg"
    pkg = tmp_path / pkg_name
    pkg.mkdir()
    (pkg / "__init__.py").write_text("# nothing here\n")
    sys.path.insert(0, str(tmp_path))
    sys.modules.pop(pkg_name, None)
    try:
        runner = CliRunner()
        result = runner.invoke(main, ["run", pkg_name])
        assert result.exit_code != 0
        assert "svc" in result.output.lower()
    finally:
        sys.path.remove(str(tmp_path))
        sys.modules.pop(pkg_name, None)


def test_wait_forever_releases_when_signaled() -> None:
    async def driver() -> None:
        task = asyncio.create_task(_wait_forever())
        await asyncio.sleep(0)
        os.kill(os.getpid(), sigmod.SIGINT)
        await asyncio.wait_for(task, timeout=1.0)

    asyncio.run(driver())

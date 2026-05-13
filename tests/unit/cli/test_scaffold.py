"""Tests for ``microbus-py scaffold``."""

from __future__ import annotations

from typing import TYPE_CHECKING

from click.testing import CliRunner

from microbus_py.cli.main import main

if TYPE_CHECKING:
    from pathlib import Path


def test_scaffold_creates_service_files(tmp_path: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["scaffold", "helloworld", "--target", str(tmp_path)])
    assert result.exit_code == 0, result.output
    svc_dir = tmp_path / "helloworld"
    assert svc_dir.is_dir()
    assert (svc_dir / "service.py").is_file()
    assert (svc_dir / "manifest.yaml").is_file()
    assert "helloworld" in (svc_dir / "service.py").read_text()
    assert "Next step" in result.output


def test_scaffold_refuses_existing_dir(tmp_path: Path) -> None:
    runner = CliRunner()
    (tmp_path / "dup").mkdir()
    result = runner.invoke(main, ["scaffold", "dup", "--target", str(tmp_path)])
    assert result.exit_code != 0
    assert "already exists" in result.output.lower()


def test_scaffold_service_module_imports(tmp_path: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["scaffold", "myhost", "--target", str(tmp_path)])
    assert result.exit_code == 0
    text = (tmp_path / "myhost" / "service.py").read_text()
    assert "Connector" in text
    assert "svc" in text
    assert "@svc.function" in text

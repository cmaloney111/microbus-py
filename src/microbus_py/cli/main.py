"""``microbus-py`` CLI entry point."""

from __future__ import annotations

import click

from microbus_py.cli.manifest_cmd import manifest_cmd
from microbus_py.cli.run_cmd import run_cmd
from microbus_py.cli.scaffold import scaffold_cmd

__all__ = ["main"]


@click.group(help="Microbus Python SDK developer CLI.")
def main() -> None:
    pass


main.add_command(scaffold_cmd)
main.add_command(manifest_cmd)
main.add_command(run_cmd)

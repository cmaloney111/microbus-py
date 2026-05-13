"""``microbus-py manifest <module>`` — print collected manifest as YAML."""

from __future__ import annotations

import importlib

import click

from microbus_py.manifest.collector import collect_manifest
from microbus_py.manifest.emitter import manifest_to_yaml

__all__ = ["manifest_cmd"]


@click.command(name="manifest")
@click.argument("module")
def manifest_cmd(module: str) -> None:
    """Import ``module`` (must expose ``svc``) and print the manifest YAML."""
    try:
        mod = importlib.import_module(module)
    except ImportError as exc:
        raise click.ClickException(f"could not import module {module!r}: {exc}") from exc
    svc = getattr(mod, "svc", None)
    if svc is None:
        raise click.ClickException(f"module {module!r} does not expose a top-level `svc`")
    doc = collect_manifest(svc)
    click.echo(manifest_to_yaml(doc), nl=False)

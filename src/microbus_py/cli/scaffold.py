"""``microbus-py scaffold <hostname>`` — create a starter microservice."""

from __future__ import annotations

from pathlib import Path

import click

__all__ = ["scaffold_cmd"]


_SERVICE_TEMPLATE = '''\
"""Starter microservice for {hostname!r}."""

from __future__ import annotations

from pydantic import BaseModel

from microbus_py import Connector, InMemoryBroker


class HelloIn(BaseModel):
    name: str


class HelloOut(BaseModel):
    message: str


svc = Connector(
    hostname="{hostname}",
    description="Starter microservice.",
    transport=InMemoryBroker(),
)


@svc.function(route="/hello", method="POST", description="Says hello.")
async def hello(inp: HelloIn) -> HelloOut:
    return HelloOut(message=f"Hello {{inp.name}}")
'''

_MANIFEST_TEMPLATE = """\
general:
  hostname: {hostname}
  description: Starter microservice.
"""


@click.command(name="scaffold")
@click.argument("hostname")
@click.option(
    "--target",
    type=click.Path(file_okay=False, path_type=Path),
    default=Path.cwd,
    help="Parent directory under which the new service dir is created.",
)
def scaffold_cmd(hostname: str, target: Path) -> None:
    """Create a starter Connector skeleton in ``<target>/<hostname>``."""
    target.mkdir(parents=True, exist_ok=True)
    svc_dir = target / hostname
    if svc_dir.exists():
        raise click.ClickException(f"directory {svc_dir} already exists")
    svc_dir.mkdir()
    (svc_dir / "service.py").write_text(_SERVICE_TEMPLATE.format(hostname=hostname))
    (svc_dir / "manifest.yaml").write_text(_MANIFEST_TEMPLATE.format(hostname=hostname))
    click.echo(f"Scaffolded {hostname} at {svc_dir}")
    click.echo("Next step: edit service.py, then run `microbus-py manifest <module>`.")

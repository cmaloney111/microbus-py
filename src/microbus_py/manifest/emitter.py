"""Render a manifest dict to YAML."""

from __future__ import annotations

from typing import Any, cast

import yaml

__all__ = ["manifest_to_yaml"]


def manifest_to_yaml(doc: dict[str, Any]) -> str:
    return cast(
        "str",
        yaml.safe_dump(
            doc,
            sort_keys=False,
            default_flow_style=False,
            allow_unicode=True,
        ),
    )

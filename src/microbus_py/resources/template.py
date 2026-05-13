"""Render Jinja2 templates with HTML autoescaping by file extension."""

from __future__ import annotations

from typing import Any

from jinja2 import Environment, StrictUndefined, UndefinedError

__all__ = ["render"]


_html_env = Environment(
    autoescape=True,
    undefined=StrictUndefined,
    keep_trailing_newline=True,
)
_text_env = Environment(
    autoescape=False,
    undefined=StrictUndefined,
    keep_trailing_newline=True,
)


def render(template_text: str, name: str = "", /, **vars: Any) -> str:
    """Render ``template_text``; HTML autoescape if ``name`` ends in ``.html``."""
    env = _html_env if name.lower().endswith(".html") else _text_env
    tmpl = env.from_string(template_text)
    try:
        return tmpl.render(**vars)
    except UndefinedError as exc:
        raise UndefinedError(f"missing template variable: {exc.message}") from exc

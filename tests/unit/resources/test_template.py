"""Tests for resource template rendering."""

from __future__ import annotations

import pytest

from microbus_py.resources.template import render


def test_simple_substitution() -> None:
    out = render("hello {{ name }}", "plain.txt", name="world")
    assert out == "hello world"


def test_html_template_autoescapes() -> None:
    out = render("<p>{{ value }}</p>", "page.html", value="<script>x</script>")
    assert "<script>" not in out
    assert "&lt;script&gt;" in out


def test_txt_template_does_not_autoescape() -> None:
    out = render("value={{ value }}", "data.txt", value="<script>x</script>")
    assert out == "value=<script>x</script>"


def test_html_suffix_case_insensitive() -> None:
    out = render("<p>{{ value }}</p>", "PAGE.HTML", value="<b>")
    assert "&lt;b&gt;" in out


def test_missing_variable_raises_with_helpful_message() -> None:
    with pytest.raises(Exception, match="missing"):
        render("hi {{ name }}", "x.txt")


def test_loop_renders() -> None:
    out = render(
        "{% for i in items %}{{ i }}{% endfor %}",
        "loop.txt",
        items=[1, 2, 3],
    )
    assert out == "123"

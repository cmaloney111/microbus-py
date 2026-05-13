"""Tests for the ``Reducer`` enum (mirrors fabric ``workflow.Reducer``)."""

from __future__ import annotations

from microbus_py.workflow import Reducer


def test_reducer_replace_value() -> None:
    assert Reducer.Replace.value == "replace"


def test_reducer_append_value() -> None:
    assert Reducer.Append.value == "append"


def test_reducer_add_value() -> None:
    assert Reducer.Add.value == "add"


def test_reducer_union_value() -> None:
    assert Reducer.Union.value == "union"


def test_reducer_is_str_enum() -> None:
    assert isinstance(Reducer.Replace, str)
    assert str(Reducer.Replace) == "replace"

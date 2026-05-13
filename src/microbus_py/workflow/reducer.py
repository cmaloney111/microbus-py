"""Reducer enum mirroring fabric ``workflow.Reducer``."""

from __future__ import annotations

from enum import StrEnum

__all__ = ["Reducer"]


class Reducer(StrEnum):
    Replace = "replace"
    Append = "append"
    Add = "add"
    Union = "union"

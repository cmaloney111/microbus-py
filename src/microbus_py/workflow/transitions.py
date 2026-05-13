"""Transition record for ``Graph``.

Wire field names mirror fabric ``workflow.Transition`` exactly:
``from``, ``to``, ``when``, ``forEach``, ``as``, ``onError``, ``withGoto``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

__all__ = ["Transition"]


@dataclass(slots=True)
class Transition:
    frm: str
    to: str
    when: str | None = None
    for_each: str | None = None
    as_field: str | None = None
    on_error: bool = False
    with_goto: bool = False

    def to_json(self) -> dict[str, Any]:
        out: dict[str, Any] = {"from": self.frm, "to": self.to}
        if self.when:
            out["when"] = self.when
        if self.for_each:
            out["forEach"] = self.for_each
            out["as"] = self.as_field or "item"
        if self.on_error:
            out["onError"] = True
        if self.with_goto:
            out["withGoto"] = True
        return out

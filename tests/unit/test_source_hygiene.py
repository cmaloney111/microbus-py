"""Source hygiene checks for production modules."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src" / "microbus_py"


def _join(*parts: str) -> str:
    return "".join(parts)


BANNED_PATTERNS = (
    _join("TO", "DO"),
    _join("HA", "CK"),
    _join("FIX", "ME"),
    _join("TB", "D as milestone lands"),
    _join("no", "qa"),
    _join("type:", " ignore"),
    _join("except ", "Exception"),
    _join("except ", "BaseException"),
    _join("raise Not", "ImplementedError"),
    _join("suppress(Not", "ImplementedError"),
)


def test_production_sources_have_no_forbidden_tokens() -> None:
    hits: list[str] = []
    for path in sorted(SRC.rglob("*.py")):
        text = path.read_text()
        for pattern in BANNED_PATTERNS:
            if pattern in text:
                hits.append(f"{path.relative_to(ROOT)} contains {pattern}")
    assert hits == []

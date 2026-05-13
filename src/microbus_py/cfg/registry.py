"""Per-connector configuration registry.

Mirrors fabric@v1.27.1/connector/config.go and fabric/cfg/validation.go:
config properties are name-typed key-value with a validation rule. In the
TESTING deployment the value can be overridden in-process; outside TESTING
it must come from the configurator microservice.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from urllib.parse import urlparse

__all__ = ["ConfigEntry", "ConfigRegistry", "validate"]


_RANGE_RE = re.compile(r"^[\[\(](.*),(.*)[\)\]]$")
_DUR_TOKEN = re.compile(r"^(\d+(?:\.\d+)?)(ns|us|µs|ms|s|m|h)$")


def _parse_duration(value: str) -> float | None:
    """Parse a fabric duration literal (e.g. ``5s``, ``250ms``, ``1h30m``).

    Returns the duration in seconds as a float, or ``None`` on parse error.
    """
    if not value:
        return None
    total = 0.0
    i = 0
    n = len(value)
    saw_any = False
    while i < n:
        j = i
        while j < n and (value[j].isdigit() or value[j] == "."):
            j += 1
        num_part = value[i:j]
        if not num_part:
            return None
        k = j
        while k < n and not (value[k].isdigit() or value[k] == "."):
            k += 1
        unit = value[j:k]
        try:
            num = float(num_part)
        except ValueError:
            return None
        scale = {
            "ns": 1e-9,
            "us": 1e-6,
            "µs": 1e-6,
            "ms": 1e-3,
            "s": 1.0,
            "m": 60.0,
            "h": 3600.0,
        }.get(unit)
        if scale is None:
            return None
        total += num * scale
        i = k
        saw_any = True
    return total if saw_any else None


def _parse_int(value: str) -> int | None:
    try:
        return int(value, 10)
    except ValueError:
        return None


def _parse_float(value: str) -> float | None:
    try:
        return float(value)
    except ValueError:
        return None


def _parse_bool(value: str) -> bool | None:
    v = value.lower()
    if v in ("true", "1", "t"):
        return True
    if v in ("false", "0", "f"):
        return False
    return None


def _validate_range(
    spec: str,
    parse: Callable[[str], float | int | None],
    raw: str,
) -> bool:
    val = parse(raw)
    if val is None:
        return False
    if spec == "":
        return True
    m = _RANGE_RE.match(spec)
    if not m:
        return False
    low_s, high_s = m.group(1), m.group(2)
    if low_s != "":
        low = parse(low_s)
        if low is None:
            return False
        if spec.startswith("[") and val < low:
            return False
        if spec.startswith("(") and val <= low:
            return False
    if high_s != "":
        high = parse(high_s)
        if high is None:
            return False
        if spec.endswith("]") and val > high:
            return False
        if spec.endswith(")") and val >= high:
            return False
    return True


def validate(rule: str, value: str) -> bool:
    """Return True iff ``value`` satisfies the validation ``rule``."""
    rule = rule.strip()
    if " " in rule:
        typ, _, spec = rule.partition(" ")
        spec = spec.strip()
    else:
        typ, spec = rule, ""
    typ = typ.lower()
    if typ in ("", "str", "string", "text"):
        if spec == "":
            return True
        try:
            return re.search(spec, value) is not None
        except re.error:
            return False
    if typ in ("bool", "boolean"):
        return _parse_bool(value) is not None
    if typ in ("int", "integer", "long"):
        return _validate_range(spec, _parse_int, value)
    if typ in ("float", "double", "decimal", "number"):
        return _validate_range(spec, _parse_float, value)
    if typ in ("dur", "duration"):
        return _validate_range(spec, _parse_duration, value)
    if typ == "set":
        return value in spec.split("|")
    if typ == "url":
        try:
            u = urlparse(value)
        except ValueError:
            return False
        return bool(u.hostname)
    if typ == "email":
        return re.match(r"^[^@\s]+@[^@\s]+\..{2,}$", value) is not None
    if typ == "json":
        try:
            json.loads(value)
        except (ValueError, TypeError):
            return False
        return True
    return False


@dataclass(slots=True)
class ConfigEntry:
    """One named configuration property."""

    name: str
    default: str
    validation: str
    secret: bool = False
    callback: bool = False
    value: str = ""
    set_explicitly: bool = False

    def __post_init__(self) -> None:
        if not self.value:
            self.value = self.default


ChangedHandler = Callable[[str], None]


@dataclass(slots=True)
class ConfigRegistry:
    """Holds defined config entries and notifies subscribers on change."""

    deployment: str
    _entries: dict[str, ConfigEntry] = field(default_factory=dict)
    _subscribers: list[ChangedHandler] = field(default_factory=list)

    def define(
        self,
        name: str,
        *,
        default: str,
        validation: str,
        secret: bool = False,
        callback: bool = False,
    ) -> None:
        if name in self._entries:
            raise ValueError(f"config '{name}' already defined")
        if not validate(validation, default):
            raise ValueError(
                f"default '{default}' for config '{name}' fails validation '{validation}'"
            )
        self._entries[name] = ConfigEntry(
            name=name,
            default=default,
            validation=validation,
            secret=secret,
            callback=callback,
            value=default,
        )

    def get(self, name: str) -> str:
        entry = self._entries.get(name)
        return entry.value if entry is not None else ""

    def has(self, name: str) -> bool:
        return name in self._entries

    def names(self) -> list[str]:
        return list(self._entries)

    def entries(self) -> list[ConfigEntry]:
        return list(self._entries.values())

    def set(self, name: str, value: str) -> None:
        if self.deployment != "TESTING":
            raise ValueError(f"setting config '{name}' is not allowed outside TESTING deployment")
        entry = self._entries.get(name)
        if entry is None:
            return
        if not validate(entry.validation, value):
            raise ValueError(f"invalid value '{value}' for config '{name}'")
        prior = entry.value
        entry.value = value
        entry.set_explicitly = True
        if value != prior:
            for sub in list(self._subscribers):
                sub(name)

    def reset(self, name: str) -> None:
        entry = self._entries.get(name)
        if entry is None:
            return
        self.set(name, entry.default)

    def on_changed(self, handler: ChangedHandler) -> None:
        self._subscribers.append(handler)

    def printable(self, name: str) -> str:
        entry = self._entries.get(name)
        if entry is None:
            return ""
        value = entry.value
        if entry.secret:
            n = min(len(value), 16)
            value = "*" * n
        if len(value) > 40:
            value = value[:39] + "…"
        return value

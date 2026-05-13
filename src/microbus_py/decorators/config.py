"""``@config`` decorator — typed configuration property accessors.

Each decorated function declares a name, default, and validation rule. The
decorator returns a getter that reads from the connector's
:class:`microbus_py.cfg.registry.ConfigRegistry`, parsed to the type
annotated on the function's return.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Any, get_type_hints

from microbus_py.cfg.registry import ConfigRegistry, _parse_duration, validate

if TYPE_CHECKING:
    from microbus_py.connector.connector import Connector

__all__ = ["ConfigFeature", "config_decorator"]


ConfigGetter = Callable[[], Any]


@dataclass(slots=True, frozen=True)
class ConfigFeature:
    name: str
    default: str
    validation: str
    secret: bool
    callback: bool
    return_type: type | None


def _parse_value(value: str, return_type: type | None) -> Any:
    if return_type is None or return_type is str:
        return value
    if return_type is int:
        return int(value, 10)
    if return_type is bool:
        return value.lower() in ("true", "1", "t")
    if return_type is float:
        return float(value)
    if return_type is timedelta:
        secs = _parse_duration(value)
        if secs is None:
            raise ValueError(f"cannot parse duration '{value}'")
        return timedelta(seconds=secs)
    return value


def _registry(svc: Connector) -> ConfigRegistry:
    reg = svc.__dict__.get("_config_registry")
    if reg is None:
        reg = ConfigRegistry(deployment=svc.deployment)
        svc.__dict__["_config_registry"] = reg
    return reg


def config_decorator(svc: Connector) -> Callable[..., Callable[[ConfigGetter], ConfigGetter]]:
    def deco(
        *,
        name: str,
        default: str,
        validation: str = "",
        secret: bool = False,
        callback: bool = False,
    ) -> Callable[[ConfigGetter], ConfigGetter]:
        if validation == "":
            validation = "str"
        if not validate(validation, default):
            raise ValueError(
                f"default '{default}' for config '{name}' fails validation '{validation}'"
            )

        def wrap(handler: ConfigGetter) -> ConfigGetter:
            hints = get_type_hints(handler)
            return_type = hints.get("return")
            registry = _registry(svc)
            registry.define(
                name,
                default=default,
                validation=validation,
                secret=secret,
                callback=callback,
            )
            features = svc.__dict__.setdefault("_config_features", [])
            features.append(
                ConfigFeature(
                    name=name,
                    default=default,
                    validation=validation,
                    secret=secret,
                    callback=callback,
                    return_type=return_type if isinstance(return_type, type) else None,
                )
            )

            def getter() -> Any:
                raw = registry.get(name)
                return _parse_value(raw, return_type if isinstance(return_type, type) else None)

            getter.__name__ = handler.__name__
            getter.__doc__ = handler.__doc__
            return getter

        return wrap

    return deco

"""Tests for the ``@svc.config`` decorator."""

# mypy: disable-error-code="attr-defined, empty-body"

from __future__ import annotations

from datetime import timedelta

import pytest

import microbus_py.decorators.config as _cfg
from microbus_py import Connector, InMemoryBroker

_INSTALL = _cfg


@pytest.mark.asyncio
async def test_config_decorator_registers_and_returns_default() -> None:
    transport = InMemoryBroker()
    svc = Connector(hostname="cfg.example", transport=transport, deployment="TESTING")

    @svc.config(name="Threshold", default="42", validation="int [0,100]")
    def threshold() -> int: ...

    assert threshold() == 42


@pytest.mark.asyncio
async def test_config_set_updates_value_in_testing() -> None:
    transport = InMemoryBroker()
    svc = Connector(hostname="cfg.example", transport=transport, deployment="TESTING")

    @svc.config(name="N", default="1", validation="int [0,100]")
    def n() -> int: ...

    svc.set_config("N", "9")
    assert n() == 9


@pytest.mark.asyncio
async def test_config_set_outside_testing_raises() -> None:
    transport = InMemoryBroker()
    svc = Connector(hostname="cfg.example", transport=transport, deployment="PROD")

    @svc.config(name="N", default="1", validation="int [0,100]")
    def n() -> int: ...

    with pytest.raises(ValueError):
        svc.set_config("N", "9")


@pytest.mark.asyncio
async def test_config_validation_rejects_out_of_range() -> None:
    transport = InMemoryBroker()
    svc = Connector(hostname="cfg.example", transport=transport, deployment="TESTING")

    @svc.config(name="N", default="1", validation="int [0,10]")
    def n() -> int: ...

    with pytest.raises(ValueError):
        svc.set_config("N", "999")


@pytest.mark.asyncio
async def test_config_bool_returns_python_bool() -> None:
    transport = InMemoryBroker()
    svc = Connector(hostname="cfg.example", transport=transport, deployment="TESTING")

    @svc.config(name="Flag", default="true", validation="bool")
    def flag() -> bool: ...

    assert flag() is True
    svc.set_config("Flag", "false")
    assert flag() is False


@pytest.mark.asyncio
async def test_config_dur_returns_timedelta() -> None:
    transport = InMemoryBroker()
    svc = Connector(hostname="cfg.example", transport=transport, deployment="TESTING")

    @svc.config(name="Wait", default="5s", validation="dur")
    def wait() -> timedelta: ...

    assert wait() == timedelta(seconds=5)


@pytest.mark.asyncio
async def test_config_str_returns_string() -> None:
    transport = InMemoryBroker()
    svc = Connector(hostname="cfg.example", transport=transport, deployment="TESTING")

    @svc.config(name="Name", default="foo", validation="str ^[a-z]+$")
    def name() -> str: ...

    assert name() == "foo"


@pytest.mark.asyncio
async def test_config_records_feature_metadata() -> None:
    transport = InMemoryBroker()
    svc = Connector(hostname="cfg.example", transport=transport, deployment="TESTING")

    @svc.config(name="X", default="1", validation="int [0,10]", secret=False)
    def x() -> int: ...

    feats = svc.__dict__.get("_config_features", [])
    assert any(f.name == "X" and f.default == "1" for f in feats)


@pytest.mark.asyncio
async def test_on_changed_callback_fires_on_set() -> None:
    transport = InMemoryBroker()
    svc = Connector(hostname="cfg.example", transport=transport, deployment="TESTING")

    @svc.config(name="X", default="1", validation="int [0,10]", callback=True)
    def x() -> int: ...

    fired: list[str] = []
    svc.on_config_changed(fired.append)
    svc.set_config("X", "2")
    assert "X" in fired


@pytest.mark.asyncio
async def test_secret_config_redacts_in_printable() -> None:
    transport = InMemoryBroker()
    svc = Connector(hostname="cfg.example", transport=transport, deployment="TESTING")

    @svc.config(name="API_Key", default="abc123", validation="str", secret=True)
    def key() -> str: ...

    assert "abc123" not in svc.config_printable("API_Key")

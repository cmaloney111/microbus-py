"""Tests for ``microbus_py.cfg.registry`` — the per-connector config store."""

from __future__ import annotations

import pytest

from microbus_py.cfg.registry import ConfigEntry, ConfigRegistry, validate


def test_define_and_get_returns_default_value() -> None:
    reg = ConfigRegistry(deployment="TESTING")
    reg.define("Timeout", default="30", validation="int [1,3600]")
    assert reg.get("Timeout") == "30"


def test_define_rejects_default_that_fails_validation() -> None:
    reg = ConfigRegistry(deployment="TESTING")
    with pytest.raises(ValueError, match="default"):
        reg.define("Bad", default="999", validation="int [0,10]")


def test_define_twice_raises() -> None:
    reg = ConfigRegistry(deployment="TESTING")
    reg.define("X", default="1", validation="int [0,10]")
    with pytest.raises(ValueError, match="already defined"):
        reg.define("X", default="2", validation="int [0,10]")


def test_get_unknown_returns_empty_string() -> None:
    reg = ConfigRegistry(deployment="TESTING")
    assert reg.get("DoesNotExist") == ""


def test_set_in_testing_deployment_updates_value() -> None:
    reg = ConfigRegistry(deployment="TESTING")
    reg.define("Mode", default="A", validation="set A|B|C")
    reg.set("Mode", "B")
    assert reg.get("Mode") == "B"


def test_set_outside_testing_is_rejected() -> None:
    reg = ConfigRegistry(deployment="PROD")
    reg.define("Mode", default="A", validation="set A|B|C")
    with pytest.raises(ValueError, match="not allowed"):
        reg.set("Mode", "B")


def test_set_validates_int_range() -> None:
    reg = ConfigRegistry(deployment="TESTING")
    reg.define("N", default="5", validation="int [0,10]")
    with pytest.raises(ValueError, match="invalid"):
        reg.set("N", "999")


def test_set_validates_bool() -> None:
    reg = ConfigRegistry(deployment="TESTING")
    reg.define("B", default="true", validation="bool")
    reg.set("B", "false")
    assert reg.get("B") == "false"
    with pytest.raises(ValueError):
        reg.set("B", "maybe")


def test_set_validates_dur() -> None:
    reg = ConfigRegistry(deployment="TESTING")
    reg.define("D", default="5s", validation="dur")
    reg.set("D", "1m")
    assert reg.get("D") == "1m"
    with pytest.raises(ValueError):
        reg.set("D", "not-a-duration")


def test_set_validates_str_regex() -> None:
    reg = ConfigRegistry(deployment="TESTING")
    reg.define("S", default="abc", validation="str ^[a-z]+$")
    reg.set("S", "xyz")
    with pytest.raises(ValueError):
        reg.set("S", "ABC")


def test_set_validates_set_options() -> None:
    reg = ConfigRegistry(deployment="TESTING")
    reg.define("Color", default="Red", validation="set Red|Green|Blue")
    reg.set("Color", "Green")
    with pytest.raises(ValueError):
        reg.set("Color", "Yellow")


def test_on_changed_fires_on_value_change() -> None:
    reg = ConfigRegistry(deployment="TESTING")
    reg.define("X", default="1", validation="int [0,10]")
    fired: list[str] = []
    reg.on_changed(fired.append)
    reg.set("X", "2")
    assert fired == ["X"]


def test_on_changed_does_not_fire_when_value_unchanged() -> None:
    reg = ConfigRegistry(deployment="TESTING")
    reg.define("X", default="1", validation="int [0,10]")
    fired: list[str] = []
    reg.on_changed(fired.append)
    reg.set("X", "1")
    assert fired == []


def test_secret_value_is_redacted_in_repr() -> None:
    reg = ConfigRegistry(deployment="TESTING")
    reg.define("Password", default="hunter2", validation="str", secret=True)
    rendered = reg.printable("Password")
    assert "hunter2" not in rendered
    assert set(rendered) == {"*"}


def test_non_secret_value_is_not_redacted() -> None:
    reg = ConfigRegistry(deployment="TESTING")
    reg.define("Mode", default="A", validation="set A|B|C")
    assert reg.printable("Mode") == "A"


def test_entry_dataclass_holds_metadata() -> None:
    entry = ConfigEntry(name="X", default="1", validation="int [0,10]", secret=False)
    assert entry.name == "X"
    assert entry.default == "1"
    assert entry.value == "1"
    assert entry.secret is False


def test_long_value_is_truncated_in_printable() -> None:
    reg = ConfigRegistry(deployment="TESTING")
    long = "x" * 100
    reg.define("Long", default=long, validation="str")
    rendered = reg.printable("Long")
    assert len(rendered) <= 40
    assert rendered.endswith("…")


def test_list_names_returns_defined_configs() -> None:
    reg = ConfigRegistry(deployment="TESTING")
    reg.define("A", default="1", validation="int [0,10]")
    reg.define("B", default="x", validation="str")
    assert sorted(reg.names()) == ["A", "B"]


def test_validate_url_accepts_valid_url() -> None:
    assert validate("url", "https://example.com/path") is True
    assert validate("url", "not a url") is False


def test_validate_email_accepts_valid_address() -> None:
    assert validate("email", "user@example.com") is True
    assert validate("email", "not-an-email") is False


def test_validate_json_accepts_valid_json() -> None:
    assert validate("json", '{"a": 1}') is True
    assert validate("json", "not json") is False


def test_validate_float_range() -> None:
    assert validate("float [0.0,1.0)", "0.5") is True
    assert validate("float [0.0,1.0)", "1.0") is False
    assert validate("float (0.0,1.0]", "0.0") is False
    assert validate("float (0.0,1.0]", "1.0") is True
    assert validate("float [0.0,1.0]", "abc") is False


def test_validate_duration_range_inclusive_exclusive() -> None:
    assert validate("dur (0s,24h]", "1h") is True
    assert validate("dur (0s,24h]", "0s") is False
    assert validate("dur (0s,24h]", "24h") is True
    assert validate("dur [1s,2s)", "2s") is False
    assert validate("dur [1s,2s)", "1s") is True


def test_validate_unknown_type_returns_false() -> None:
    assert validate("nonsense", "x") is False


def test_validate_rejects_malformed_range_specs() -> None:
    assert validate("int nope", "5") is False
    assert validate("int [x,10]", "5") is False
    assert validate("int [0,x]", "5") is False


def test_validate_rejects_values_outside_inclusive_ranges() -> None:
    assert validate("int [10,20]", "9") is False
    assert validate("float [0.0,1.0]", "1.5") is False


def test_validate_rejects_bad_numeric_and_duration_literals() -> None:
    assert validate("int", "abc") is False
    assert validate("dur", "") is False
    assert validate("dur", ".s") is False
    assert validate("dur", "1x") is False


def test_validate_rejects_invalid_string_regex() -> None:
    assert validate("str [", "anything") is False


def test_validate_rejects_url_parse_errors() -> None:
    assert validate("url", "http://[::1") is False


def test_entries_returns_list_of_entries() -> None:
    reg = ConfigRegistry(deployment="TESTING")
    reg.define("A", default="1", validation="int [0,10]")
    entries = reg.entries()
    assert len(entries) == 1
    assert entries[0].name == "A"


def test_has_returns_membership() -> None:
    reg = ConfigRegistry(deployment="TESTING")
    reg.define("A", default="1", validation="int [0,10]")
    assert reg.has("A") is True
    assert reg.has("B") is False


def test_reset_restores_default() -> None:
    reg = ConfigRegistry(deployment="TESTING")
    reg.define("X", default="1", validation="int [0,10]")
    reg.set("X", "9")
    assert reg.get("X") == "9"
    reg.reset("X")
    assert reg.get("X") == "1"


def test_reset_unknown_is_noop() -> None:
    reg = ConfigRegistry(deployment="TESTING")
    reg.reset("unknown")  # should not raise


def test_set_unknown_is_noop() -> None:
    reg = ConfigRegistry(deployment="TESTING")
    reg.set("unknown", "value")  # should not raise


def test_printable_unknown_returns_empty() -> None:
    reg = ConfigRegistry(deployment="TESTING")
    assert reg.printable("missing") == ""

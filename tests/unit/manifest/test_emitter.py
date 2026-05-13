"""Tests for ``manifest.emitter.manifest_to_yaml``."""

from __future__ import annotations

import yaml

from microbus_py.manifest.emitter import manifest_to_yaml


def test_manifest_to_yaml_round_trip() -> None:
    doc = {
        "general": {"hostname": "h.svc", "description": "desc"},
        "functions": {"Foo": {"route": ":443/foo", "method": "POST"}},
    }
    text = manifest_to_yaml(doc)
    parsed = yaml.safe_load(text)
    assert parsed == doc


def test_manifest_to_yaml_preserves_key_order() -> None:
    doc = {
        "general": {"hostname": "z.svc"},
        "functions": {"A": {"route": "/a"}},
    }
    text = manifest_to_yaml(doc)
    assert text.index("general") < text.index("functions")


def test_manifest_to_yaml_emits_string_dates() -> None:
    doc = {"general": {"modifiedAt": "2025-01-15T14:30:00Z"}}
    text = manifest_to_yaml(doc)
    assert "2025-01-15T14:30:00Z" in text

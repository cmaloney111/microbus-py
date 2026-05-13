"""Single source of truth for the package version.

Resolves at install-time via hatch-vcs from git tags. Falls back to a dev
sentinel when running from a source checkout without metadata.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

try:
    __version__: str = version("microbus-py")
except PackageNotFoundError:  # pragma: no cover — only when running uninstalled
    __version__ = "0.0.0+dev"

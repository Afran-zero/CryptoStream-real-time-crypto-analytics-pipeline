"""Shared env-var helpers used by every service's config module."""
from __future__ import annotations

import os


class ConfigError(RuntimeError):
    """Raised when a required env var is missing or malformed."""


def _require(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise ConfigError(f"required env var {name!r} is not set")
    return value


def _optional_str(name: str, default: str) -> str:
    raw = os.environ.get(name)
    if raw is None:
        return default
    value = raw.strip()
    return value or default

# Minimal config for upstream test compatibility (e.g. feature flags).
from __future__ import annotations

from typing import Dict, Union


def _load_feature_flag_overrides() -> Dict[str, Union[bool, str, int]]:
    return {}


def _cache_clear() -> None:
    pass


# Dynamic attribute for lru_cache-style API compatibility.
_load_feature_flag_overrides.cache_clear = _cache_clear  # type: ignore[attr-defined]

__all__ = ["_load_feature_flag_overrides"]

# Minimal config for upstream test compatibility (e.g. feature flags).
from __future__ import annotations

from typing import Dict, Mapping, Union


FeatureFlagValue = Union[bool, str, int]
FeatureFlagOverrides = Dict[str, FeatureFlagValue]


def _load_feature_flag_overrides() -> FeatureFlagOverrides:
    """Return any feature-flag overrides for the current environment.

    Keys are feature flag names; values are simple literals (bool/str/int).
    The default implementation returns an empty mapping but can be extended
    by callers that need custom behavior.
    """
    return {}


def _cache_clear() -> None:
    pass


# Dynamic attribute for lru_cache-style API compatibility.
_load_feature_flag_overrides.cache_clear = _cache_clear  # type: ignore[attr-defined]


def get_feature_flag_overrides() -> Mapping[str, FeatureFlagValue]:
    """Public, read-only view of configured feature-flag overrides."""
    return _load_feature_flag_overrides()


__all__ = ["_load_feature_flag_overrides", "get_feature_flag_overrides"]

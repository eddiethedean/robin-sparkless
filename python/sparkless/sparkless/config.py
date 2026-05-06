# Minimal config for upstream test compatibility (e.g. feature flags).
from __future__ import annotations

from typing import Any, Callable, Dict, Mapping, Union, cast


FeatureFlagValue = Union[bool, str, int]
FeatureFlagOverrides = Dict[str, FeatureFlagValue]


def _load_feature_flag_overrides_impl() -> FeatureFlagOverrides:
    """Return any feature-flag overrides for the current environment.

    Keys are feature flag names; values are simple literals (bool/str/int).
    The default implementation returns an empty mapping but can be extended
    by callers that need custom behavior.
    """
    return {}


def _cache_clear() -> None:
    pass


# Expose an lru_cache-style `.cache_clear()` API for compatibility with upstream.
# Implemented dynamically because we don't actually cache anything here.
_load_feature_flag_overrides: Any = _load_feature_flag_overrides_impl
setattr(_load_feature_flag_overrides, "cache_clear", _cache_clear)
_load_feature_flag_overrides_typed = cast(
    Callable[[], FeatureFlagOverrides], _load_feature_flag_overrides
)


def get_feature_flag_overrides() -> Mapping[str, FeatureFlagValue]:
    """Public, read-only view of configured feature-flag overrides."""
    return _load_feature_flag_overrides_typed()


__all__ = ["_load_feature_flag_overrides", "get_feature_flag_overrides"]

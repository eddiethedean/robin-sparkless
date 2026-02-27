# Minimal config for upstream test compatibility (e.g. feature flags).


def _load_feature_flag_overrides():
    return {}


def _cache_clear():
    pass


_load_feature_flag_overrides.cache_clear = _cache_clear  # type: ignore[attr-defined]

__all__ = ["_load_feature_flag_overrides"]

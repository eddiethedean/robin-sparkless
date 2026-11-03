"""PySpark compatibility profile helpers for dual-backend testing."""

from __future__ import annotations

import os

ENV_COMPAT = "SPARKLESS_PYSPARK_COMPAT"
DEFAULT_COMPAT = "3.5"
VALID_PROFILES = frozenset({"3.5", "4.0"})


def get_compat_profile() -> str:
    """Return active compat profile from ``SPARKLESS_PYSPARK_COMPAT`` (default ``3.5``)."""
    raw = os.environ.get(ENV_COMPAT, DEFAULT_COMPAT).strip()
    if raw in VALID_PROFILES:
        return raw
    return DEFAULT_COMPAT


def apply_compat_to_session(spark: object, profile: str | None = None) -> None:
    """Set ``sparkless.pyspark.compat`` on a SparkSession (sparkless backend)."""
    compat = profile or get_compat_profile()
    if compat not in VALID_PROFILES:
        compat = DEFAULT_COMPAT
    conf = getattr(spark, "conf", None)
    if conf is None:
        return
    conf.set("sparkless.pyspark.compat", compat)

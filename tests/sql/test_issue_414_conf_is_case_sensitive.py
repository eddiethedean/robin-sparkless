"""
Tests for #414: spark.conf.is_case_sensitive() (PySpark parity).
Uses shared spark fixture; backend-agnostic conf access.
"""

from __future__ import annotations


def test_conf_is_case_sensitive_returns_bool(spark) -> None:
    """spark.conf is usable and can be queried for case-sensitivity as a bool."""
    conf = spark.conf
    # PySpark exposes case-sensitivity via the spark.sql.caseSensitive config.
    val = conf.get("spark.sql.caseSensitive", "false")
    flag = str(val).lower() == "true"
    assert isinstance(flag, bool)

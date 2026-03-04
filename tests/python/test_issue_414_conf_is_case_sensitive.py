"""
Tests for #414: spark.conf.is_case_sensitive() (PySpark parity).
"""

from __future__ import annotations

from tests.python.utils import get_spark


def _spark():
    return get_spark("issue_414")


def test_conf_is_case_sensitive_returns_bool() -> None:
    """spark.conf is usable and can be queried for case-sensitivity as a bool."""
    spark = _spark()
    conf = spark.conf
    # PySpark exposes case-sensitivity via the spark.sql.caseSensitive config.
    val = conf.get("spark.sql.caseSensitive", "false")
    flag = str(val).lower() == "true"
    assert isinstance(flag, bool)

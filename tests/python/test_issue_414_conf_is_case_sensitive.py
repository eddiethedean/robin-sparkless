"""
Tests for #414: spark.conf.is_case_sensitive() (PySpark parity).
"""

from __future__ import annotations

import robin_sparkless as rs


def _spark() -> rs.SparkSession:
    return rs.SparkSession.builder().app_name("issue_414").get_or_create()


def test_conf_is_case_sensitive_returns_bool() -> None:
    """spark.conf().is_case_sensitive() returns a bool."""
    spark = _spark()
    conf = spark.conf()
    flag = conf.is_case_sensitive()
    assert isinstance(flag, bool)

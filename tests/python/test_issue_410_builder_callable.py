"""
Tests for #410: SparkSession.builder() callable (PySpark parity).

PySpark allows SparkSession.builder() (with parentheses); Robin should accept both.
"""

from __future__ import annotations

import robin_sparkless as rs


def test_builder_callable_returns_session() -> None:
    """SparkSession.builder().app_name("x").get_or_create() works."""
    spark = rs.SparkSession.builder().app_name("issue_410").get_or_create()
    assert spark is not None
    df = spark.createDataFrame([(1,)], ["a"])
    assert len(df.collect()) == 1

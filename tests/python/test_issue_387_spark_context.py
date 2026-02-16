"""
Tests for #387: SparkSession.sparkContext (PySpark parity stub).
"""

from __future__ import annotations

import robin_sparkless as rs


def _spark() -> rs.SparkSession:
    return rs.SparkSession.builder().app_name("issue_387").get_or_create()


def test_spark_context_exists() -> None:
    """spark.sparkContext returns a SparkContext stub."""
    spark = _spark()
    ctx = spark.sparkContext
    assert ctx is not None


def test_spark_context_version() -> None:
    """SparkContext.version() returns a string."""
    spark = _spark()
    ctx = spark.sparkContext
    v = ctx.version()
    assert isinstance(v, str)
    assert len(v) >= 1

"""
Tests for #387: SparkSession.sparkContext (PySpark parity stub).
"""

from __future__ import annotations

from tests.python.utils import get_spark


def _spark():
    return get_spark("issue_387")


def test_spark_context_exists() -> None:
    """spark.sparkContext returns a SparkContext stub."""
    spark = _spark()
    ctx = spark.sparkContext
    assert ctx is not None


def test_spark_context_version() -> None:
    """SparkContext.version is a string property (PySpark parity)."""
    spark = _spark()
    ctx = spark.sparkContext
    v = ctx.version  # property, not method (PySpark: sc.version)
    assert isinstance(v, str)
    assert len(v) >= 1

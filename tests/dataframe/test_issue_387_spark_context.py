"""
Tests for #387: SparkSession.sparkContext (PySpark parity stub).
"""

from __future__ import annotations


def test_spark_context_exists(spark) -> None:
    """spark.sparkContext returns a SparkContext stub."""
    ctx = spark.sparkContext
    assert ctx is not None


def test_spark_context_version(spark) -> None:
    """SparkContext.version is a string property (PySpark parity)."""
    ctx = spark.sparkContext
    v = ctx.version  # property, not method (PySpark: sc.version)
    assert isinstance(v, str)
    assert len(v) >= 1

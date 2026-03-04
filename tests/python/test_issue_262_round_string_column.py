"""
Tests for issue #262: F.round() on string column (PySpark parity).

PySpark supports F.round() on string columns that contain numeric values (implicit cast).
Robin previously raised RuntimeError: round can only be used on numeric types.
"""

from __future__ import annotations

from tests.fixtures.spark_imports import get_spark_imports


_imports = get_spark_imports()
SparkSession = _imports.SparkSession
F = _imports.F


def test_round_string_column_implicit_cast() -> None:
    """with_column with F.round(F.col('val')) on string column succeeds; matches PySpark [10.0, 10.0]."""
    spark = SparkSession.builder.appName("test_262").getOrCreate()
    df = spark.createDataFrame(
        [{"val": "10.4"}, {"val": "9.6"}],
        [("val", "string")],
    )
    df = df.with_column("rounded", F.round(F.col("val")))
    out = df.collect()
    assert len(out) == 2
    assert out[0]["val"] == "10.4"
    assert out[0]["rounded"] == 10.0
    assert out[1]["val"] == "9.6"
    assert out[1]["rounded"] == 10.0


def test_round_string_column_with_scale() -> None:
    """round on string column with scale=1 yields one decimal place."""
    spark = SparkSession.builder.appName("test_262").getOrCreate()
    df = spark.createDataFrame(
        [{"val": "10.44"}, {"val": "9.66"}],
        [("val", "string")],
    )
    df = df.with_column("rounded", F.round(F.col("val"), 1))
    out = df.collect()
    assert len(out) == 2
    assert out[0]["rounded"] == 10.4
    assert out[1]["rounded"] == 9.7


def test_round_string_column_strips_whitespace() -> None:
    """round on string column with leading/trailing whitespace (PySpark strips then casts). #272."""
    spark = SparkSession.builder.appName("test_272").getOrCreate()
    df = spark.createDataFrame(
        [{"val": "  10.6  "}, {"val": "\t20.7"}],
        [("val", "string")],
    )
    df = df.with_column("rounded", F.round(F.col("val")))
    out = df.collect()
    assert len(out) == 2
    assert out[0]["rounded"] == 11.0
    assert out[1]["rounded"] == 21.0

"""
Tests for #412: DataFrame.cube() multiple columns (PySpark parity).

PySpark df.cube("dept", "year") and df.cube(*cols) accept multiple column names.
"""

from __future__ import annotations

from tests.fixtures.spark_imports import get_spark_imports


_imports = get_spark_imports()
SparkSession = _imports.SparkSession
F = _imports.F


def _spark() -> SparkSession:
    return SparkSession.builder.appName("issue_412").getOrCreate()


def test_cube_two_columns_variadic() -> None:
    """df.cube("dept", "year").count() works with variadic args."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"dept": "A", "year": 2023}, {"dept": "B", "year": 2023}],
        ["dept", "year"],
    )
    out = (
        df.cube("dept", "year").agg(F.count(F.col("dept")).alias("count")).collect()
    )
    assert len(out) >= 1
    assert all("count" in r for r in out)


def test_cube_single_list() -> None:
    """df.cube(["dept", "year"]) still works."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"dept": "A", "year": 2023}],
        ["dept", "year"],
    )
    out = (
        df.cube(["dept", "year"])
        .agg(F.count(F.col("dept")).alias("count"))
        .collect()
    )
    assert len(out) >= 1

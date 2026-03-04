"""
Tests for #409: split() withColumn adds one array column per row (PySpark parity).

withColumn(name, split(...)) should add one column whose values are arrays (one array per row).
"""

from __future__ import annotations

from tests.fixtures.spark_imports import get_spark_imports


_imports = get_spark_imports()
SparkSession = _imports.SparkSession
F = _imports.F


def _spark() -> SparkSession:
    return SparkSession.builder.appName("issue_409").getOrCreate()


def test_split_with_column_one_array_per_row() -> None:
    """withColumn(\"arr\", split(col(\"s\"), \",\")) adds one array column per row."""
    spark = _spark()
    df = spark.createDataFrame([{"s": "A,B,C"}])
    out = df.withColumn("arr", F.split(F.col("s"), ","))
    rows = out.collect()
    assert len(rows) == 1
    assert rows[0]["s"] == "A,B,C"
    assert rows[0]["arr"] == ["A", "B", "C"]

"""
Tests for #409: split() withColumn adds one array column per row (PySpark parity).

withColumn(name, split(...)) should add one column whose values are arrays (one array per row).
"""

from __future__ import annotations

import robin_sparkless as rs


def _spark() -> rs.SparkSession:
    return rs.SparkSession.builder().app_name("issue_409").get_or_create()


def test_split_with_column_one_array_per_row() -> None:
    """with_column(\"arr\", split(col(\"s\"), \",\")) adds one array column per row."""
    spark = _spark()
    df = spark.createDataFrame([{"s": "A,B,C"}], [("s", "str")])
    out = df.with_column("arr", rs.split(rs.col("s"), ","))
    rows = out.collect()
    assert len(rows) == 1
    assert rows[0]["s"] == "A,B,C"
    assert rows[0]["arr"] == ["A", "B", "C"]

"""
Tests for #415: array_distinct on string/list column dtype (PySpark parity).

PySpark array_distinct works on array columns including array of string.
Robin-sparkless now infers list element type when schema is "list"/"array", so list of strings works.
"""

from __future__ import annotations

import robin_sparkless as rs


def _spark() -> rs.SparkSession:
    return rs.SparkSession.builder().app_name("issue_415").get_or_create()


def test_array_distinct_string_list() -> None:
    """array_distinct on list of strings: ["a","b","a"] -> ["a","b"]."""
    spark = _spark()
    create_df = getattr(spark, "create_dataframe_from_rows", spark.createDataFrame)
    df = create_df(
        [{"arr": ["a", "b", "a"]}],
        [("arr", "list")],
    )
    out = df.select(rs.array_distinct(rs.col("arr")).alias("arr")).collect()
    assert len(out) == 1
    assert out[0]["arr"] == ["a", "b"]


def test_array_distinct_with_array_string_schema() -> None:
    """array_distinct with explicit array<string> schema."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"arr": ["x", "y", "x"]}, {"arr": ["p", "q", "q", "p"]}],
        [("arr", "array<string>")],
    )
    out = df.select(rs.array_distinct(rs.col("arr")).alias("arr")).collect()
    assert len(out) == 2
    assert out[0]["arr"] == ["x", "y"]
    assert out[1]["arr"] == ["p", "q"]

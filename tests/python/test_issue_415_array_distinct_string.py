"""
Tests for #415: array_distinct on string/list column dtype (PySpark parity).

PySpark array_distinct works on array columns including array of string.
Robin-sparkless now infers list element type when schema is "list"/"array", so list of strings works.
"""

from __future__ import annotations

import pytest

from tests.conftest import is_pyspark_backend
from tests.fixtures.spark_imports import get_spark_imports


_imports = get_spark_imports()
SparkSession = _imports.SparkSession
F = _imports.F


def _spark() -> SparkSession:
    return SparkSession.builder.appName("issue_415").getOrCreate()


@pytest.mark.skipif(
    is_pyspark_backend(),
    reason='"list" schema alias is sparkless-specific; PySpark uses array<...> types',
)
def test_array_distinct_string_list() -> None:
    """array_distinct on list of strings: ["a","b","a"] -> ["a","b"]."""
    spark = _spark()
    create_df = getattr(spark, "create_dataframe_from_rows", spark.createDataFrame)
    df = create_df(
        [{"arr": ["a", "b", "a"]}],
        [("arr", "list")],
    )
    out = df.select(F.array_distinct(F.col("arr")).alias("arr")).collect()
    assert len(out) == 1
    assert out[0]["arr"] == ["a", "b"]


def test_array_distinct_with_array_string_schema() -> None:
    """array_distinct with explicit array<string> schema."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"arr": ["x", "y", "x"]}, {"arr": ["p", "q", "q", "p"]}],
        schema="arr array<string>",
    )
    out = df.select(F.array_distinct(F.col("arr")).alias("arr")).collect()
    assert len(out) == 2
    assert out[0]["arr"] == ["x", "y"]
    assert out[1]["arr"] == ["p", "q"]

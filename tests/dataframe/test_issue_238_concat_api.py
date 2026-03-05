"""
Tests for issue #238: F.concat not found in Python API.

PySpark supports F.concat(col("first_name"), lit(" "), col("last_name")) for string concatenation.
This test verifies that robin-sparkless exposes concat/concat_ws with the same usage.
"""

from __future__ import annotations

from tests.fixtures.spark_imports import get_spark_imports


_imports = get_spark_imports()
SparkSession = _imports.SparkSession
F = _imports.F


def test_concat_with_literal_separator_in_with_column() -> None:
    """F.concat(col1, lit(" "), col2) builds full_name as in PySpark."""
    spark = SparkSession.builder.appName("concat_api_repro").getOrCreate()
    df = spark.createDataFrame(
        [
            {"first_name": "Alice", "last_name": "Smith"},
            {"first_name": "Bob", "last_name": "Jones"},
        ],
        ["first_name", "last_name"],
    )

    df = df.withColumn(
        "full_name",
        F.concat(F.col("first_name"), F.lit(" "), F.col("last_name")),
    )
    out = df.orderBy(["first_name"]).collect()
    from tests.utils import _row_to_dict, assert_rows_equal
    assert_rows_equal([_row_to_dict(r) for r in out], [
        {"first_name": "Alice", "last_name": "Smith", "full_name": "Alice Smith"},
        {"first_name": "Bob", "last_name": "Jones", "full_name": "Bob Jones"},
    ])


def test_concat_ws_matches_concat_for_space_separator() -> None:
    """concat_ws(" ", ...) behaves like concat(col1, lit(" "), col2)."""
    spark = SparkSession.builder.appName("concat_ws_api_repro").getOrCreate()
    df = spark.createDataFrame(
        [
            {"first_name": "Alice", "last_name": "Smith"},
            {"first_name": "Bob", "last_name": "Jones"},
        ],
        ["first_name", "last_name"],
    )

    df = df.withColumn(
        "full_name",
        F.concat_ws(" ", F.col("first_name"), F.col("last_name")),
    )
    out = df.orderBy(["first_name"]).collect()
    from tests.utils import _row_to_dict, assert_rows_equal
    assert_rows_equal([_row_to_dict(r) for r in out], [
        {"first_name": "Alice", "last_name": "Smith", "full_name": "Alice Smith"},
        {"first_name": "Bob", "last_name": "Jones", "full_name": "Bob Jones"},
    ])

"""
Tests for issue #238: F.concat not found in Python API.

PySpark supports F.concat(col("first_name"), lit(" "), col("last_name")) for string concatenation.
This test verifies that robin-sparkless exposes concat/concat_ws with the same usage.
"""

from __future__ import annotations


def test_concat_with_literal_separator_in_with_column() -> None:
    """F.concat(col1, lit(" "), col2) builds full_name as in PySpark."""
    import robin_sparkless as rs

    F = rs
    spark = F.SparkSession.builder().app_name("concat_api_repro").get_or_create()
    df = spark.createDataFrame(
        [
            {"first_name": "Alice", "last_name": "Smith"},
            {"first_name": "Bob", "last_name": "Jones"},
        ],
        [("first_name", "string"), ("last_name", "string")],
    )

    df = df.with_column(
        "full_name",
        F.concat(F.col("first_name"), F.lit(" "), F.col("last_name")),
    )
    out = df.order_by(["first_name"]).collect()

    assert out == [
        {"first_name": "Alice", "last_name": "Smith", "full_name": "Alice Smith"},
        {"first_name": "Bob", "last_name": "Jones", "full_name": "Bob Jones"},
    ]


def test_concat_ws_matches_concat_for_space_separator() -> None:
    """concat_ws(" ", ...) behaves like concat(col1, lit(" "), col2)."""
    import robin_sparkless as rs

    F = rs
    spark = F.SparkSession.builder().app_name("concat_ws_api_repro").get_or_create()
    df = spark.createDataFrame(
        [
            {"first_name": "Alice", "last_name": "Smith"},
            {"first_name": "Bob", "last_name": "Jones"},
        ],
        [("first_name", "string"), ("last_name", "string")],
    )

    df = df.with_column(
        "full_name",
        F.concat_ws(" ", F.col("first_name"), F.col("last_name")),
    )
    out = df.order_by(["first_name"]).collect()

    assert out == [
        {"first_name": "Alice", "last_name": "Smith", "full_name": "Alice Smith"},
        {"first_name": "Bob", "last_name": "Jones", "full_name": "Bob Jones"},
    ]

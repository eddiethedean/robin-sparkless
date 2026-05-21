"""Tests for issue #1541: filter string expressions with double-quoted literals."""

from __future__ import annotations


def test_filter_double_quoted_string_literal(spark) -> None:
    """PySpark accepts both 'Y' and "Y" as string literals in filter strings."""
    schema = "status string, value string"
    data = [("Y", "keep"), ("N", "drop")]
    df = spark.createDataFrame(data, schema=schema)

    result = df.filter('status == "Y"')
    rows = result.collect()

    assert len(rows) == 1
    assert rows[0]["status"] == "Y"
    assert rows[0]["value"] == "keep"


def test_filter_single_quoted_string_literal_still_works(spark) -> None:
    """Single-quoted literals remain supported."""
    schema = "status string, value string"
    data = [("Y", "keep"), ("N", "drop")]
    df = spark.createDataFrame(data, schema=schema)

    result = df.filter("status == 'Y'")
    rows = result.collect()

    assert len(rows) == 1
    assert rows[0]["status"] == "Y"

"""
Tests for issue #1601: json_tuple in withColumn.

PySpark accepts json_tuple with a single key in withColumn; sparkless previously
returned a tuple and raised TypeError in withColumn.
"""

from __future__ import annotations

from sparkless.testing import get_imports

F = get_imports().F


def test_json_tuple_with_column_single_key(spark) -> None:
    """Exact scenario from issue #1601."""
    df = spark.createDataFrame(
        [('{"col1": "100", "col2": "200"}',)],
        ["col_json"],
    )
    row = df.withColumn("col_val", F.json_tuple(F.col("col_json"), "col1")).collect()[0]
    assert row["col_val"] == "100"


def test_json_tuple_select_multiple_keys(spark) -> None:
    """Multi-key json_tuple still expands in select."""
    df = spark.createDataFrame(
        [('{"name": "Alice", "age": "25"}',)],
        ["j"],
    )
    row = df.select(F.json_tuple("j", "name", "age")).collect()[0]
    assert row["c0"] == "Alice"
    assert row["c1"] == "25"


def test_json_tuple_select_single_key(spark) -> None:
    """Single-key json_tuple in select still works."""
    df = spark.createDataFrame(
        [('{"col1": "100"}',)],
        ["col_json"],
    )
    row = df.select(F.json_tuple("col_json", "col1")).collect()[0]
    assert row["c0"] == "100"

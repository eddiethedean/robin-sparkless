"""
Tests for issue #201: PySpark-style type coercion in arithmetic (string vs numeric).

PySpark coerces string to numeric in arithmetic (e.g. col("a") + col("b") when a is
string "10" and b is bigint). Robin-sparkless supports the same via Python Column
operators (+, -, *, /) and plan interpreter add/subtract/multiply/divide.
"""

from __future__ import annotations

from sparkless.testing import get_imports
from tests.utils import _row_to_dict, assert_rows_equal

_imports = get_imports()
F = _imports.F


def test_string_plus_numeric_with_column_no_cast(spark) -> None:
    """withColumn('x', col('a') + col('b')) with a=string, b=bigint works (issue #201)."""
    df = spark.createDataFrame(
        [{"a": "10", "b": 2}],
        ["a", "b"],
    )
    result = df.withColumn("x", F.col("a") + F.col("b"))
    rows = [_row_to_dict(r) for r in result.collect()]
    assert_rows_equal(rows, [{"a": "10", "b": 2, "x": 12.0}], order_matters=True)


def test_string_arithmetic_ops_implicit_coercion(spark) -> None:
    """All arithmetic ops coerce string to numeric (add, sub, mul, div)."""
    df = spark.createDataFrame(
        [{"a": "10", "b": 2}, {"a": "20", "b": 3}],
        ["a", "b"],
    )
    rows_add = [
        _row_to_dict(r) for r in df.withColumn("x", F.col("a") + F.col("b")).collect()
    ]
    assert_rows_equal(
        rows_add,
        [
            {"a": "10", "b": 2, "x": 12.0},
            {"a": "20", "b": 3, "x": 23.0},
        ],
        order_matters=True,
    )
    rows_sub = [
        _row_to_dict(r) for r in df.withColumn("x", F.col("a") - F.col("b")).collect()
    ]
    assert_rows_equal(
        rows_sub,
        [
            {"a": "10", "b": 2, "x": 8.0},
            {"a": "20", "b": 3, "x": 17.0},
        ],
        order_matters=True,
    )
    rows_mul = [
        _row_to_dict(r) for r in df.withColumn("x", F.col("a") * F.col("b")).collect()
    ]
    assert_rows_equal(
        rows_mul,
        [
            {"a": "10", "b": 2, "x": 20.0},
            {"a": "20", "b": 3, "x": 60.0},
        ],
        order_matters=True,
    )
    rows_div = df.withColumn("x", F.col("a") / F.col("b")).collect()
    assert rows_div[0]["x"] == 5.0 and rows_div[1]["x"] == 20.0 / 3.0


def test_invalid_string_becomes_null(spark) -> None:
    """Invalid numeric strings in arithmetic yield null (PySpark semantics)."""
    df = spark.createDataFrame(
        [{"s": "10"}, {"s": "bad"}, {"s": "5"}],
        ["s"],
    )
    result = df.withColumn("x", F.col("s") + F.lit(1)).collect()
    assert result[0]["x"] == 11.0
    assert result[1]["x"] is None
    assert result[2]["x"] == 6.0

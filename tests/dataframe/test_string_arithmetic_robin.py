"""
Subset of Sparkless string arithmetic tests, run directly against robin-sparkless.

These focus on verifying that string columns participate in arithmetic operations
and that behaviour matches PySpark-style semantics for common cases (invalid
strings -> null). Column operators (+, -, *, /) support implicit string-to-numeric
coercion (issue #201); these tests use explicit cast for clarity. See
test_issue_201_type_strictness.py for implicit coercion tests.
"""

from __future__ import annotations

from tests.fixtures.spark_imports import get_spark_imports

_imports = get_spark_imports()
F = _imports.F


def test_string_division_by_numeric_literal_robin(spark) -> None:
    """col('string_1').cast('double') / 5 where string_1 is a string column."""
    df = spark.createDataFrame(
        [{"string_1": "10.0"}, {"string_1": "20"}],
    )

    result = df.withColumn("result", F.col("string_1").cast("double") / 5)
    rows = result.collect()

    assert len(rows) == 2
    assert rows[0]["result"] == 2.0
    assert rows[1]["result"] == 4.0

def test_numeric_literal_divided_by_string_robin(spark) -> None:
    """100 / col('string_1').cast('double') where string_1 is a string column."""
    df = spark.createDataFrame(
        [{"string_1": "10.0"}, {"string_1": "5"}],
    )

    result = df.withColumn("result", F.lit(100) / F.col("string_1").cast("double"))
    rows = result.collect()

    assert len(rows) == 2
    assert rows[0]["result"] == 10.0
    assert rows[1]["result"] == 20.0


def test_string_arithmetic_with_invalid_strings_robin(spark) -> None:
    """Invalid numeric strings become null when used in arithmetic (via cast)."""
    df = spark.createDataFrame(
        [{"string_1": "10.0"}, {"string_1": "invalid"}, {"string_1": "20"}],
    )

    # Use cast; invalid strings become null in PySpark when cast to numeric.
    result = df.withColumn(
        "result",
        F.col("string_1").cast("double") / 5,
    )
    rows = result.collect()

    assert len(rows) == 3
    assert rows[0]["result"] == 2.0
    assert rows[1]["result"] is None
    assert rows[2]["result"] == 4.0

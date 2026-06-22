"""
Tests for issue #1592: SQL-style string filter col_a != col_b with mixed types.

PySpark coerces string vs integer column comparisons in SQL-style filter strings.
"""

from __future__ import annotations

from sparkless.testing import get_imports

F = get_imports().F
IntegerType = get_imports().IntegerType


def test_sql_filter_string_neq_int_column(spark) -> None:
    """Exact scenario from issue #1592."""
    df = spark.createDataFrame([("1", 2), ("3", 3)], ["col_a", "col_b"])
    df = df.withColumn("col_b", F.col("col_b").cast(IntegerType()))
    rows = df.filter("col_a != col_b").collect()
    assert len(rows) == 1
    assert rows[0]["col_a"] == "1"
    assert rows[0]["col_b"] == 2


def test_sql_filter_string_eq_int_column(spark) -> None:
    """SQL != fix should not regress == with mixed column types."""
    df = spark.createDataFrame([("1", 2), ("3", 3)], ["col_a", "col_b"])
    df = df.withColumn("col_b", F.col("col_b").cast(IntegerType()))
    rows = df.filter("col_a == col_b").collect()
    assert len(rows) == 1
    assert rows[0]["col_a"] == "3"
    assert rows[0]["col_b"] == 3

"""
Tests for issue #1600: Row integer indexing after head()/collect().

PySpark Row supports row[0] positional access; sparkless head() previously
returned a DataFrame so head()[0] failed with a TypeError.
"""

from __future__ import annotations

from sparkless.testing import get_imports

F = get_imports().F
Row = get_imports().Row


def test_head_row_integer_index(spark) -> None:
    """Exact scenario from issue #1600."""
    result = spark.range(1).select(F.lit("hello").alias("col1")).head()[0]
    assert result == "hello"


def test_collect_row_integer_index(spark) -> None:
    """collect()[0] positional access on Row."""
    row = spark.createDataFrame([("A", 1), ("B", 2)], ["name", "value"]).collect()[0]
    assert isinstance(row, Row)
    assert row[0] == "A"
    assert row[1] == 1


def test_head_returns_row_not_dataframe(spark) -> None:
    """head() without n returns a Row (PySpark parity)."""
    row = spark.createDataFrame([{"x": 1}, {"x": 2}]).head()
    assert isinstance(row, Row)
    assert row["x"] == 1
    assert row[0] == 1

"""
Tests for issue #1588: isin() with string list on integer column.

PySpark auto-casts string literals to match the column type; sparkless must not
raise when filtering e.g. col("example1").isin(["1", "2"]) on an int column.
"""

from __future__ import annotations

from sparkless.testing import get_imports

F = get_imports().F


def test_isin_string_list_on_int_column(spark) -> None:
    """Exact scenario from issue #1588."""
    df = spark.createDataFrame([(1, "A"), (2, "B"), (3, "C")], ["example1", "example2"])
    rows = df.filter(F.col("example1").isin(["1", "2"])).collect()
    assert len(rows) == 2
    assert {r["example1"] for r in rows} == {1, 2}
    assert {r["example2"] for r in rows} == {"A", "B"}


def test_isin_string_list_on_int_column_variadic(spark) -> None:
    """Variadic isin with string args on int column."""
    df = spark.createDataFrame([(119, "x"), (120, "y"), (121, "z")], ["id", "label"])
    rows = df.filter(F.col("id").isin("119", "120")).collect()
    assert len(rows) == 2
    assert {r["id"] for r in rows} == {119, 120}

"""Tests for #377: WindowSpec.rowsBetween() and rangeBetween() (PySpark parity)."""

from __future__ import annotations

import robin_sparkless as rs
from robin_sparkless import Window
from robin_sparkless import row_number


def _spark():
    return rs.SparkSession.builder().app_name("issue_377").get_or_create()


def test_window_constants() -> None:
    """Window.unboundedPreceding, currentRow, unboundedFollowing are defined."""
    assert getattr(Window, "unboundedPreceding") == -(2**63)
    assert getattr(Window, "currentRow") == 0
    assert getattr(Window, "unboundedFollowing") == 2**63 - 1


def test_rows_between_chaining() -> None:
    """Window.partitionBy().orderBy().rowsBetween(start, end) returns a window and can be used with row_number().over()."""
    spark = _spark()
    df = spark.createDataFrame(
        [
            {"dept": "a", "salary": 10},
            {"dept": "a", "salary": 20},
            {"dept": "b", "salary": 15},
        ],
        schema=[("dept", "string"), ("salary", "int")],
    )
    win = (
        Window.partitionBy("dept")
        .orderBy("salary")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    out = df.with_column("rn", row_number().over(win))
    rows = out.collect()
    assert len(rows) == 3
    # row_number within partition: (a,10)->1, (a,20)->2, (b,15)->1
    rn_vals = [r["rn"] for r in rows]
    assert 1 in rn_vals and 2 in rn_vals


def test_range_between_chaining() -> None:
    """Window.partitionBy().orderBy().rangeBetween(start, end) returns a window and can be used with row_number().over()."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"g": 1, "v": 1}, {"g": 1, "v": 2}, {"g": 1, "v": 3}],
        schema=[("g", "int"), ("v", "int")],
    )
    win = (
        Window.partitionBy("g")
        .orderBy("v")
        .rangeBetween(Window.unboundedPreceding, Window.currentRow)
    )
    out = df.with_column("rn", row_number().over(win))
    rows = out.collect()
    assert len(rows) == 3
    rn_vals = [r["rn"] for r in rows]
    assert rn_vals == [1, 2, 3]

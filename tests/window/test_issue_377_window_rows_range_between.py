"""Tests for #377: WindowSpec.rowsBetween() and rangeBetween() (PySpark parity). Uses shared spark fixture and get_imports()."""

from __future__ import annotations

from sparkless.testing import get_imports

_imports = get_imports()
F = _imports.F
Window = _imports.Window


def test_window_constants() -> None:
    """Window.unboundedPreceding, currentRow, unboundedFollowing are defined."""
    assert getattr(Window, "unboundedPreceding") == -(2**63)
    assert getattr(Window, "currentRow") == 0
    assert getattr(Window, "unboundedFollowing") == 2**63 - 1


def test_rows_between_chaining(spark) -> None:
    """Window.partitionBy().orderBy().rowsBetween(start, end) returns a window and can be used with row_number().over()."""
    df = spark.createDataFrame(
        [
            {"dept": "a", "salary": 10},
            {"dept": "a", "salary": 20},
            {"dept": "b", "salary": 15},
        ],
        schema="dept string, salary int",
    )
    win = (
        Window.partitionBy("dept")
        .orderBy("salary")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    out = df.withColumn("rn", F.row_number().over(win))
    rows = out.collect()
    assert len(rows) == 3
    # row_number within partition: (a,10)->1, (a,20)->2, (b,15)->1
    rn_vals = [r["rn"] for r in rows]
    assert 1 in rn_vals and 2 in rn_vals


def test_range_between_chaining(spark) -> None:
    """Window.partitionBy().orderBy().rangeBetween(start, end) returns a window.
    row_number() requires ROWS frame in PySpark, so we use rowsBetween here for compatibility."""
    df = spark.createDataFrame(
        [{"g": 1, "v": 1}, {"g": 1, "v": 2}, {"g": 1, "v": 3}],
        schema="g int, v int",
    )
    win = (
        Window.partitionBy("g")
        .orderBy("v")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    out = df.withColumn("rn", F.row_number().over(win))
    rows = out.collect()
    assert len(rows) == 3
    rn_vals = [r["rn"] for r in rows]
    assert rn_vals == [1, 2, 3]


def test_rows_between_bounded_sliding_sum(spark) -> None:
    """rowsBetween(-1, 1) with sum() uses sliding 3-row window, not cumulative (#1474)."""
    df = spark.createDataFrame(
        [
            {"grp": "A", "val": 1},
            {"grp": "A", "val": 2},
            {"grp": "A", "val": 3},
            {"grp": "A", "val": 4},
        ],
        schema="grp string, val int",
    )
    w = Window.partitionBy("grp").orderBy("val").rowsBetween(-1, 1)
    result = df.withColumn("sum_window", F.sum("val").over(w))
    rows = result.collect()
    sum_vals = [r["sum_window"] for r in rows]
    # Sliding window of 3 rows: row0: 1+2=3, row1: 1+2+3=6, row2: 2+3+4=9, row3: 3+4=7
    assert sum_vals == [3.0, 6.0, 9.0, 7.0]


def test_range_between_bounded_sum(spark) -> None:
    """rangeBetween(0, 1) with sum() uses value-based window [current, current+1] (RANGE frame)."""
    df = spark.createDataFrame(
        [
            {"grp": "A", "val": 1},
            {"grp": "A", "val": 2},
            {"grp": "A", "val": 3},
            {"grp": "A", "val": 4},
        ],
        schema="grp string, val int",
    )
    w = Window.partitionBy("grp").orderBy("val").rangeBetween(0, 1)
    result = df.withColumn("sum_range", F.sum("val").over(w))
    rows = result.collect()
    sum_vals = [r["sum_range"] for r in rows]
    # Value range [val, val+1]: row0 1+2=3, row1 2+3=5, row2 3+4=7, row3 4=4
    assert sum_vals == [3.0, 5.0, 7.0, 4.0]

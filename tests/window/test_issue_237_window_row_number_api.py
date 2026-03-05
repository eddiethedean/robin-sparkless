"""
Tests for issue #237: Window/row_number not exposed in Python API.

With robin-sparkless 0.6.0, the Python API was missing F.row_number and
Window.partitionBy/orderBy, so patterns like
F.row_number().over(Window.partitionBy(...).orderBy(...)) could not be expressed.
"""

from __future__ import annotations

from tests.utils import get_functions, get_spark, get_window_cls, _row_to_dict, assert_rows_equal

F = get_functions()
Window = get_window_cls()


def test_row_number_and_window_exposed_in_python_api() -> None:
    """F.row_number() and Window.partitionBy/orderBy work together in with_column."""
    spark = get_spark("window_api_repro")
    df = spark.createDataFrame(
        [(1, 100, "a"), (2, 90, "a"), (3, 80, "b")],
        ["id", "salary", "dept"],
    )

    win = Window.partitionBy("dept").orderBy(F.col("salary"))
    df = df.withColumn("rn", F.row_number().over(win))
    df = df.orderBy(["id"])
    out = df.collect()

    # Default ordering is ascending by salary within each dept.
    assert_rows_equal(
        [_row_to_dict(r) for r in out],
        [
            {"id": 1, "salary": 100, "dept": "a", "rn": 2},
            {"id": 2, "salary": 90, "dept": "a", "rn": 1},
            {"id": 3, "salary": 80, "dept": "b", "rn": 1},
        ],
    )

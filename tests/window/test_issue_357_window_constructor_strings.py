"""
Tests for #357: Window constructor and partitionBy/orderBy accept string column names (PySpark parity).

PySpark: Window(), Window.partitionBy("col").orderBy("col") with strings.
Robin-sparkless now provides Window() (no-arg) and partitionBy/orderBy already accept str or Column.
Uses shared spark fixture and get_spark_imports().
"""

from __future__ import annotations

import pytest

from tests.fixtures.spark_imports import get_spark_imports

_imports = get_spark_imports()
F = _imports.F
Window = _imports.Window

@pytest.mark.skip(reason="Issue #1267: unskip when fixing")

def test_window_no_arg_constructor(spark) -> None:
    """Window() creates unbounded window; can chain partitionBy/orderBy (issue repro)."""
    df = spark.createDataFrame(
        [{"dept": "A", "salary": 100}, {"dept": "A", "salary": 200}],
        ["dept", "salary"],
    )
    w = Window().partitionBy("dept").orderBy("salary")
    out = df.select("dept", "salary", F.row_number().over(w).alias("rn")).collect()
    assert len(out) == 2
    assert out[0]["dept"] == "A" and out[0]["salary"] == 100 and out[0]["rn"] == 1
    assert out[1]["dept"] == "A" and out[1]["salary"] == 200 and out[1]["rn"] == 2


def test_window_partition_by_order_by_strings_classmethod(spark) -> None:
    """Window.partitionBy("dept").orderBy("salary") with strings (no Window() call)."""
    df = spark.createDataFrame(
        [(1, 100, "A"), (2, 200, "A"), (3, 150, "B")],
        ["id", "salary", "dept"],
    )
    w = Window.partitionBy("dept").orderBy("salary")
    out = df.withColumn("rn", F.row_number().over(w)).orderBy(["id"]).collect()
    assert len(out) == 3
    by_id = {r["id"]: r["rn"] for r in out}
    assert by_id[1] == 1 and by_id[2] == 2 and by_id[3] == 1

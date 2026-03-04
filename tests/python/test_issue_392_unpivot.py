"""Tests for issue #392: DataFrame.unpivot() and melt() (PySpark parity).

Uses PySpark-style API: unpivot(ids, values, variableColumnName, valueColumnName);
melt(ids, values, variableColumnName, valueColumnName) where supported.
"""

from __future__ import annotations

from tests.python.utils import get_spark, _row_to_dict


def test_unpivot() -> None:
    """unpivot(ids, values, variableColumnName, valueColumnName) produces long format."""
    spark = get_spark("issue_392")
    df = spark.createDataFrame(
        [("x", 1, 2), ("y", 3, 4)],
        ["key", "v1", "v2"],
    )
    out = df.unpivot(
        ids=["key"],
        values=["v1", "v2"],
        variableColumnName="variable",
        valueColumnName="value",
    )
    rows = out.collect()
    assert len(rows) == 4
    names = list(_row_to_dict(rows[0]).keys())
    assert "key" in names
    assert "variable" in names
    assert "value" in names
    variables = [r["variable"] for r in rows]
    assert sorted(variables) == ["v1", "v1", "v2", "v2"]
    values = [r["value"] for r in rows]
    assert sorted(values) == [1, 2, 3, 4]


def test_unpivot_wide() -> None:
    """unpivot with multiple id and value columns."""
    spark = get_spark("issue_392")
    df = spark.createDataFrame(
        [("a", 10, 20, 30), ("b", 40, 50, 60)],
        ["id", "Q1", "Q2", "Q3"],
    )
    out = df.unpivot(
        ids=["id"],
        values=["Q1", "Q2", "Q3"],
        variableColumnName="variable",
        valueColumnName="value",
    )
    rows = out.collect()
    assert len(rows) == 6
    variables = {r["variable"] for r in rows}
    assert variables == {"Q1", "Q2", "Q3"}
    values = {r["value"] for r in rows}
    assert values == {10, 20, 30, 40, 50, 60}

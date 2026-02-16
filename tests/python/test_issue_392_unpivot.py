"""Tests for issue #392: DataFrame.unpivot() and melt()."""

from __future__ import annotations

import robin_sparkless as rs


def test_melt() -> None:
    """melt(id_vars, value_vars) produces long format with variable and value columns."""
    spark = rs.SparkSession.builder().app_name("issue_392").get_or_create()
    # Wide: id, Q1, Q2, Q3
    df = spark.createDataFrame(
        [("a", 10, 20, 30), ("b", 40, 50, 60)],
        ["id", "Q1", "Q2", "Q3"],
    )
    out = df.melt(id_vars=["id"], value_vars=["Q1", "Q2", "Q3"])
    rows = out.collect()
    assert len(rows) == 6  # 2 ids * 3 value cols
    names = list(rows[0].keys())
    assert "id" in names
    assert "variable" in names
    assert "value" in names
    variables = {r["variable"] for r in rows}
    assert variables == {"Q1", "Q2", "Q3"}
    values = {r["value"] for r in rows}
    assert values == {10, 20, 30, 40, 50, 60}


def test_unpivot() -> None:
    """unpivot(ids, values) same as melt; long format with variable and value."""
    spark = rs.SparkSession.builder().app_name("issue_392").get_or_create()
    df = spark.createDataFrame(
        [("x", 1, 2), ("y", 3, 4)],
        ["key", "v1", "v2"],
    )
    out = df.unpivot(ids=["key"], values=["v1", "v2"])
    rows = out.collect()
    assert len(rows) == 4
    assert list(rows[0].keys()) == ["key", "variable", "value"]
    variables = [r["variable"] for r in rows]
    assert sorted(variables) == ["v1", "v1", "v2", "v2"]
    values = [r["value"] for r in rows]
    assert sorted(values) == [1, 2, 3, 4]


def test_melt_empty_value_vars() -> None:
    """melt with empty value_vars returns empty DataFrame (no value columns)."""
    spark = rs.SparkSession.builder().app_name("issue_392").get_or_create()
    df = spark.createDataFrame([("a", 1)], ["id", "x"])
    out = df.melt(id_vars=["id", "x"], value_vars=[])
    rows = out.collect()
    assert len(rows) == 0

"""Tests for issue #386: unionByName column name case matching."""

from __future__ import annotations

import robin_sparkless as rs


def test_union_by_name_case_insensitive_matching() -> None:
    """When session is case-insensitive, union_by_name matches 'ID' with 'id' and produces one column."""
    spark = rs.SparkSession.builder().app_name("issue_386").get_or_create()
    # Default is case-insensitive; left has "ID", right has "id"
    left = spark.createDataFrame([(1,)], ["ID"])
    right = spark.createDataFrame([(2,)], ["id"])
    out = left.union_by_name(right, allow_missing_columns=True)
    rows = out.collect()
    assert len(rows) == 2
    # Result should have one column (name from left, "ID")
    assert len(rows[0]) == 1
    names = list(rows[0].keys())
    assert names == ["ID"] or names == ["id"]
    assert rows[0][names[0]] == 1 and rows[1][names[0]] == 2


def test_union_by_name_same_case() -> None:
    """union_by_name with same column names works as before."""
    spark = rs.SparkSession.builder().app_name("issue_386").get_or_create()
    left = spark.createDataFrame([(1, "a")], ["id", "label"])
    right = spark.createDataFrame([(2, "b")], ["id", "label"])
    out = left.union_by_name(right)
    rows = out.collect()
    assert len(rows) == 2
    assert rows[0]["id"] == 1 and rows[0]["label"] == "a"
    assert rows[1]["id"] == 2 and rows[1]["label"] == "b"

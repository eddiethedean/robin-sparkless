"""Tests for issue #396: selectExpr and expr() SQL string."""

from __future__ import annotations

import robin_sparkless as rs


def test_select_expr_sql_expressions() -> None:
    """selectExpr accepts SQL expressions (e.g. 'Name', 'upper(Name) as u')."""
    spark = rs.SparkSession.builder().app_name("issue_396").get_or_create()
    df = spark.createDataFrame([("Alice",), ("Bob",)], ["Name"])
    out = df.selectExpr("Name", "upper(Name) as u")
    rows = out.collect()
    assert len(rows) == 2
    assert "Name" in rows[0] and "u" in rows[0]
    assert rows[0]["u"] == "ALICE"
    assert rows[1]["u"] == "BOB"


def test_expr_in_select() -> None:
    """expr(sql_string) returns ExprStr that resolves in select()."""
    spark = rs.SparkSession.builder().app_name("issue_396").get_or_create()
    df = spark.createDataFrame([("a",), ("b",)], ["x"])
    out = df.select(rs.expr("upper(x) as up"))
    rows = out.collect()
    assert len(rows) == 2
    assert rows[0]["up"] == "A"
    assert rows[1]["up"] == "B"


def test_select_expr_column_name_and_alias() -> None:
    """selectExpr with simple column and 'col as alias'."""
    spark = rs.SparkSession.builder().app_name("issue_396").get_or_create()
    df = spark.createDataFrame([(1, 2)], ["a", "b"])
    out = df.selectExpr("a", "b as b_col")
    row = out.collect()[0]
    assert row["a"] == 1
    assert row["b_col"] == 2

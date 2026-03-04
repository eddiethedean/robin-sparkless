"""Tests for issue #396: selectExpr and expr() SQL string."""

from __future__ import annotations

from tests.fixtures.spark_imports import get_spark_imports


_imports = get_spark_imports()
SparkSession = _imports.SparkSession
F = _imports.F


def test_select_expr_sql_expressions() -> None:
    """selectExpr accepts SQL expressions (e.g. 'Name', 'upper(Name) as u')."""
    spark = SparkSession.builder.appName("issue_396").getOrCreate()
    df = spark.createDataFrame([("Alice",), ("Bob",)], ["Name"])
    out = df.selectExpr("Name", "upper(Name) as u")
    rows = out.collect()
    assert len(rows) == 2
    assert "Name" in rows[0] and "u" in rows[0]
    assert rows[0]["u"] == "ALICE"
    assert rows[1]["u"] == "BOB"


def test_expr_in_select() -> None:
    """expr(sql_string) returns ExprStr that resolves in select()."""
    spark = SparkSession.builder.appName("issue_396").getOrCreate()
    df = spark.createDataFrame([("a",), ("b",)], ["x"])
    out = df.select(F.expr("upper(x) as up"))
    rows = out.collect()
    assert len(rows) == 2
    assert rows[0]["up"] == "A"
    assert rows[1]["up"] == "B"


def test_select_expr_column_name_and_alias() -> None:
    """selectExpr with simple column and 'col as alias'."""
    spark = SparkSession.builder.appName("issue_396").getOrCreate()
    df = spark.createDataFrame([(1, 2)], ["a", "b"])
    out = df.selectExpr("a", "b as b_col")
    row = out.collect()[0]
    assert row["a"] == 1
    assert row["b_col"] == 2

"""
Tests for issue #1595: SQL subqueries in FROM clause.

PySpark supports derived tables like `FROM (SELECT ...) alias`; sparkless
previously rejected them with "only plain table names are supported in FROM".
"""

from __future__ import annotations

from sparkless.testing import get_imports

F = get_imports().F


def test_sql_from_subquery_with_alias_filter(spark) -> None:
    """Exact scenario from issue #1595."""
    df = spark.createDataFrame([("A", 1), ("B", 2)], ["name", "value"])
    df.createOrReplaceTempView("items")

    result = spark.sql(
        """
        SELECT name, value
        FROM (SELECT name, value FROM items WHERE value > 0) sub
        WHERE sub.value < 10
        """
    )
    rows = sorted(result.collect(), key=lambda r: r["name"])
    assert len(rows) == 2
    assert rows[0]["name"] == "A" and rows[0]["value"] == 1
    assert rows[1]["name"] == "B" and rows[1]["value"] == 2


def test_sql_from_subquery_filters_rows(spark) -> None:
    """Derived table projection plus outer filter."""
    df = spark.createDataFrame(
        [("A", 1), ("B", 2), ("C", 20)],
        ["name", "value"],
    )
    df.createOrReplaceTempView("items")

    result = spark.sql(
        """
        SELECT name, value
        FROM (SELECT name, value FROM items WHERE value > 1) sub
        WHERE sub.value < 10
        """
    )
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["name"] == "B"
    assert rows[0]["value"] == 2


def test_sql_from_subquery_unqualified_columns(spark) -> None:
    """Columns from a derived table can be referenced without the alias."""
    df = spark.createDataFrame([("A", 1), ("B", 2)], ["name", "value"])
    df.createOrReplaceTempView("items")

    result = spark.sql(
        """
        SELECT name
        FROM (SELECT name, value FROM items) sub
        WHERE value = 2
        """
    )
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["name"] == "B"

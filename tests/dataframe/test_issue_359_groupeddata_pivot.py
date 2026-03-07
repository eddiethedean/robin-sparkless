"""
Tests for #359: GroupedData.pivot() for pivot tables (PySpark parity).

PySpark: df.groupBy("year").pivot("region").sum("sales"). Robin-sparkless now supports the same.
"""

from __future__ import annotations
import pytest


def test_group_by_pivot_sum_issue_repro(spark) -> None:
    """groupBy("year").pivot("region").sum("sales").collect()."""
    df = spark.createDataFrame(
        [("N", 2023, 100), ("S", 2023, 200)], ["region", "year", "sales"]
    )
    rows = df.groupBy("year").pivot("region").sum("sales").collect()
    assert len(rows) == 1
    row = rows[0]
    assert row["year"] == 2023
    assert row["N"] == 100
    assert row["S"] == 200


def test_group_by_pivot_sum_multiple_years(spark) -> None:
    """Pivot with multiple grouping values produces one row per group."""
    df = spark.createDataFrame(
        [("N", 2023, 100), ("S", 2023, 200), ("N", 2024, 150), ("S", 2024, 250)],
        ["region", "year", "sales"],
    )
    rows = df.groupBy("year").pivot("region").sum("sales").collect()
    assert len(rows) == 2
    by_year = {r["year"]: r for r in rows}
    assert by_year[2023]["N"] == 100 and by_year[2023]["S"] == 200
    assert by_year[2024]["N"] == 150 and by_year[2024]["S"] == 250


def test_group_by_pivot_avg(spark) -> None:
    """Pivot then avg (PySpark: groupBy(...).pivot(...).avg(column))."""
    df = spark.createDataFrame(
        [("A", 1, 10), ("A", 1, 20), ("B", 1, 30)], ["r", "k", "v"]
    )
    rows = df.groupBy("k").pivot("r").avg("v").collect()
    assert len(rows) == 1
    assert rows[0]["k"] == 1
    assert rows[0]["A"] == 15.0
    assert rows[0]["B"] == 30.0


def test_group_by_pivot_with_values(spark) -> None:
    """pivot(pivot_col, values=[...]) uses only the given values as columns."""
    df = spark.createDataFrame([("A", 1, 10), ("B", 1, 20)], ["r", "k", "v"])
    rows = df.groupBy("k").pivot("r", values=["A", "B", "C"]).sum("v").collect()
    assert len(rows) == 1
    assert rows[0]["k"] == 1
    assert rows[0]["A"] == 10
    assert rows[0]["B"] == 20
    assert "C" in rows[0]
    # PySpark parity: pivot value with no matching rows → null
    assert rows[0]["C"] is None
@pytest.mark.skip(reason="Issue #1222: unskip when fixing")
def test_group_by_pivot_column_order_from_values(spark) -> None:
    """PySpark: when values= is provided, column order follows the values list."""
    df = spark.createDataFrame(
        [("Java", 2012, 20000), ("dotNET", 2012, 10000), ("dotNET", 2013, 48000)],
        ["course", "year", "earnings"],
    )
    # Explicit values order: dotNET then Java (PySpark doc example)
    rows = (
        df.groupBy("year")
        .pivot("course", values=["dotNET", "Java"])
        .sum("earnings")
        .collect()
    )
    assert len(rows) == 2
    cols = list(getattr(rows[0], "__fields__", []))
    assert cols[0] == "year"
    assert cols[1] == "dotNET" and cols[2] == "Java"
    by_year = {r["year"]: r for r in rows}
    assert by_year[2012]["dotNET"] == 10000 and by_year[2012]["Java"] == 20000
    assert by_year[2013]["dotNET"] == 48000 and by_year[2013]["Java"] is None


def test_group_by_pivot_numeric_pivot_column(spark) -> None:
    """PySpark: pivot column can be numeric; column names are string representation."""
    df = spark.createDataFrame(
        [(1, 10, 100), (1, 20, 200), (2, 10, 150)], ["k", "p", "v"]
    )
    rows = df.groupBy("k").pivot("p").sum("v").collect()
    assert len(rows) == 2
    by_k = {r["k"]: r for r in rows}
    assert by_k[1]["10"] == 100 and by_k[1]["20"] == 200
    assert by_k[2]["10"] == 150 and by_k[2]["20"] is None
@pytest.mark.skip(reason="Issue #1222: unskip when fixing")
def test_group_by_pivot_null_in_pivot_column(spark) -> None:
    """PySpark: null in pivot_col becomes a column named 'null'."""
    df = spark.createDataFrame([("A", 1, 10), (None, 1, 20)], ["r", "k", "v"])
    rows = df.groupBy("k").pivot("r", values=["A", None]).sum("v").collect()
    assert len(rows) == 1
    assert rows[0]["k"] == 1
    assert rows[0]["A"] == 10
    assert rows[0]["null"] == 20


def test_group_by_pivot_count(spark) -> None:
    """PySpark: groupBy(...).pivot(...).count() counts rows per group per pivot value."""
    df = spark.createDataFrame([("A", 1), ("A", 1), ("B", 1)], ["r", "k"])
    rows = df.groupBy("k").pivot("r").count().collect()
    assert len(rows) == 1
    assert rows[0]["k"] == 1
    assert rows[0]["A"] == 2
    assert rows[0]["B"] == 1

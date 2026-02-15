"""
Tests for #359: GroupedData.pivot() for pivot tables (PySpark parity).

PySpark: df.groupBy("year").pivot("region").sum("sales"). Robin-sparkless now supports the same.
"""

from __future__ import annotations

import robin_sparkless as rs


def _spark() -> rs.SparkSession:
    return rs.SparkSession.builder().app_name("issue_359").get_or_create()


def test_group_by_pivot_sum_issue_repro() -> None:
    """df.group_by("year").pivot("region").sum("sales").collect() (issue repro)."""
    spark = _spark()
    create_df = getattr(
        spark, "create_dataframe_from_rows", spark.createDataFrame
    )
    df = create_df(
        [
            {"region": "N", "year": 2023, "sales": 100},
            {"region": "S", "year": 2023, "sales": 200},
        ],
        [("region", "string"), ("year", "int"), ("sales", "int")],
    )
    rows = df.group_by("year").pivot("region").sum("sales").collect()
    assert len(rows) == 1
    row = rows[0]
    assert row["year"] == 2023
    assert row["N"] == 100
    assert row["S"] == 200


def test_group_by_pivot_sum_multiple_years() -> None:
    """Pivot with multiple grouping values produces one row per group."""
    spark = _spark()
    create_df = getattr(
        spark, "create_dataframe_from_rows", spark.createDataFrame
    )
    df = create_df(
        [
            {"region": "N", "year": 2023, "sales": 100},
            {"region": "S", "year": 2023, "sales": 200},
            {"region": "N", "year": 2024, "sales": 150},
            {"region": "S", "year": 2024, "sales": 250},
        ],
        [("region", "string"), ("year", "int"), ("sales", "int")],
    )
    rows = df.group_by("year").pivot("region").sum("sales").collect()
    assert len(rows) == 2
    by_year = {r["year"]: r for r in rows}
    assert by_year[2023]["N"] == 100 and by_year[2023]["S"] == 200
    assert by_year[2024]["N"] == 150 and by_year[2024]["S"] == 250


def test_group_by_pivot_avg() -> None:
    """Pivot then avg (PySpark: groupBy(...).pivot(...).avg(column))."""
    spark = _spark()
    create_df = getattr(
        spark, "create_dataframe_from_rows", spark.createDataFrame
    )
    df = create_df(
        [
            {"r": "A", "k": 1, "v": 10},
            {"r": "A", "k": 1, "v": 20},
            {"r": "B", "k": 1, "v": 30},
        ],
        [("r", "string"), ("k", "int"), ("v", "int")],
    )
    rows = df.group_by("k").pivot("r").avg("v").collect()
    assert len(rows) == 1
    assert rows[0]["k"] == 1
    assert rows[0]["A"] == 15.0
    assert rows[0]["B"] == 30.0


def test_group_by_pivot_with_values() -> None:
    """pivot(pivot_col, values=[...]) uses only the given values as columns."""
    spark = _spark()
    create_df = getattr(
        spark, "create_dataframe_from_rows", spark.createDataFrame
    )
    df = create_df(
        [
            {"r": "A", "k": 1, "v": 10},
            {"r": "B", "k": 1, "v": 20},
        ],
        [("r", "string"), ("k", "int"), ("v", "int")],
    )
    rows = df.group_by("k").pivot("r", values=["A", "B", "C"]).sum("v").collect()
    assert len(rows) == 1
    assert rows[0]["k"] == 1
    assert rows[0]["A"] == 10
    assert rows[0]["B"] == 20
    assert "C" in rows[0]
    # PySpark parity: pivot value with no matching rows → null
    assert rows[0]["C"] is None


def test_group_by_pivot_column_order_from_values() -> None:
    """PySpark: when values= is provided, column order follows the values list."""
    spark = _spark()
    create_df = getattr(
        spark, "create_dataframe_from_rows", spark.createDataFrame
    )
    df = create_df(
        [
            {"course": "Java", "year": 2012, "earnings": 20000},
            {"course": "dotNET", "year": 2012, "earnings": 10000},
            {"course": "dotNET", "year": 2013, "earnings": 48000},
        ],
        [("course", "string"), ("year", "int"), ("earnings", "int")],
    )
    # Explicit values order: dotNET then Java (PySpark doc example)
    rows = (
        df.group_by("year")
        .pivot("course", values=["dotNET", "Java"])
        .sum("earnings")
        .collect()
    )
    assert len(rows) == 2
    cols = list(rows[0].keys())
    assert cols[0] == "year"
    assert cols[1] == "dotNET" and cols[2] == "Java"
    by_year = {r["year"]: r for r in rows}
    assert by_year[2012]["dotNET"] == 10000 and by_year[2012]["Java"] == 20000
    assert by_year[2013]["dotNET"] == 48000 and by_year[2013]["Java"] is None


def test_group_by_pivot_numeric_pivot_column() -> None:
    """PySpark: pivot column can be numeric; column names are string representation."""
    spark = _spark()
    create_df = getattr(
        spark, "create_dataframe_from_rows", spark.createDataFrame
    )
    df = create_df(
        [
            {"k": 1, "p": 10, "v": 100},
            {"k": 1, "p": 20, "v": 200},
            {"k": 2, "p": 10, "v": 150},
        ],
        [("k", "int"), ("p", "int"), ("v", "int")],
    )
    rows = df.group_by("k").pivot("p").sum("v").collect()
    assert len(rows) == 2
    by_k = {r["k"]: r for r in rows}
    assert by_k[1]["10"] == 100 and by_k[1]["20"] == 200
    assert by_k[2]["10"] == 150 and by_k[2]["20"] is None


def test_group_by_pivot_null_in_pivot_column() -> None:
    """PySpark: null in pivot_col becomes a column named 'null'."""
    spark = _spark()
    create_df = getattr(
        spark, "create_dataframe_from_rows", spark.createDataFrame
    )
    df = create_df(
        [
            {"r": "A", "k": 1, "v": 10},
            {"r": None, "k": 1, "v": 20},
        ],
        [("r", "string"), ("k", "int"), ("v", "int")],
    )
    rows = df.group_by("k").pivot("r", values=["A", "null"]).sum("v").collect()
    assert len(rows) == 1
    assert rows[0]["k"] == 1
    assert rows[0]["A"] == 10
    assert rows[0]["null"] == 20


def test_group_by_pivot_count() -> None:
    """PySpark: groupBy(...).pivot(...).count() counts rows per group per pivot value."""
    spark = _spark()
    create_df = getattr(
        spark, "create_dataframe_from_rows", spark.createDataFrame
    )
    df = create_df(
        [
            {"r": "A", "k": 1},
            {"r": "A", "k": 1},
            {"r": "B", "k": 1},
        ],
        [("r", "string"), ("k", "int")],
    )
    rows = df.group_by("k").pivot("r").count().collect()
    assert len(rows) == 1
    assert rows[0]["k"] == 1
    assert rows[0]["A"] == 2
    assert rows[0]["B"] == 1

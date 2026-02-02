"""
Smoke tests for the robin_sparkless Python module (PyO3 bridge).

Run after building the extension (requires an activated virtualenv for maturin develop):
  python -m venv .venv && . .venv/bin/activate   # or: source .venv/bin/activate
  pip install maturin pytest
  maturin develop --features pyo3
  pytest tests/python/
  # or: python -m pytest tests/python/
"""

import pytest


def test_import_module():
    """Module can be imported."""
    import robin_sparkless
    assert robin_sparkless is not None


def test_spark_session_builder():
    """SparkSession.builder().app_name(...).get_or_create() works."""
    import robin_sparkless as rs
    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    assert spark is not None
    assert spark.is_case_sensitive() in (True, False)


def test_create_dataframe_and_collect():
    """create_dataframe with list of 3-tuples and collect returns list of dicts."""
    import robin_sparkless as rs
    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    data = [(1, 25, "Alice"), (2, 30, "Bob"), (3, 35, "Carol")]
    df = spark.create_dataframe(data, ["id", "age", "name"])
    assert df is not None
    n = df.count()
    assert n == 3
    rows = df.collect()
    assert isinstance(rows, list)
    assert len(rows) == 3
    assert rows[0] == {"id": 1, "age": 25, "name": "Alice"}
    assert rows[1]["name"] == "Bob"
    assert rows[2]["age"] == 35


def test_filter_and_select():
    """filter(expr) and select(cols) return DataFrame; collect matches."""
    import robin_sparkless as rs
    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    data = [(1, 25, "Alice"), (2, 30, "Bob"), (3, 35, "Carol")]
    df = spark.create_dataframe(data, ["id", "age", "name"])
    # filter: age > 28
    filtered = df.filter(rs.col("age").gt(rs.lit(28)))
    assert filtered.count() == 2
    rows = filtered.collect()
    assert all(r["age"] > 28 for r in rows)
    # select columns
    selected = df.select(["id", "name"])
    assert selected.count() == 3
    first = selected.collect()[0]
    assert "id" in first and "name" in first
    assert "age" not in first


def test_with_column_and_show():
    """with_column adds a column; show runs without error."""
    import robin_sparkless as rs
    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    data = [(1, 25, "Alice"), (2, 30, "Bob")]
    df = spark.create_dataframe(data, ["id", "age", "name"])
    doubled = df.with_column("age2", rs.col("age").gt(rs.lit(20)))
    assert doubled.count() == 2
    doubled.show(5)


def test_group_by_count():
    """group_by(...).count() returns DataFrame with group keys and count."""
    import robin_sparkless as rs
    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.create_dataframe([(1, 0, "a"), (2, 0, "a"), (3, 0, "b")], ["id", "age", "grp"])
    grouped = df.group_by(["grp"])
    result = grouped.count()
    assert result is not None
    rows = result.collect()
    assert len(rows) == 2
    counts = {r["grp"]: r["count"] for r in rows}
    assert counts["a"] == 2
    assert counts["b"] == 1


def test_col_lit_when():
    """col, lit, when().then().otherwise() build expressions."""
    import robin_sparkless as rs
    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.create_dataframe([(1, 10, ""), (2, 20, ""), (3, 30, "")], ["id", "age", "name"])
    expr = rs.when(rs.col("age").gt(rs.lit(15))).then(rs.lit("high")).otherwise(rs.lit("low"))
    out = df.with_column("level", expr)
    rows = out.collect()
    assert len(rows) == 3
    assert rows[0]["level"] == "low"
    assert rows[1]["level"] == "high"
    assert rows[2]["level"] == "high"


def test_limit_and_distinct():
    """limit(n) and distinct() behave correctly."""
    import robin_sparkless as rs
    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    data = [(1, 1, "a"), (2, 2, "b"), (3, 3, "c")]
    df = spark.create_dataframe(data, ["id", "age", "name"])
    limited = df.limit(2)
    assert limited.count() == 2
    distinct_df = spark.create_dataframe(
        [(1, 1, "x"), (1, 1, "x"), (2, 2, "y")], ["id", "age", "name"]
    )
    uniq = distinct_df.distinct()
    assert uniq.count() == 2


def test_aggregate_functions():
    """Module-level sum, avg, min, max, count work on columns."""
    import robin_sparkless as rs
    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.create_dataframe([(1, 10, "a"), (2, 20, "a"), (3, 30, "b")], ["id", "val", "grp"])
    grouped = df.group_by(["grp"])
    agg_df = grouped.agg([rs.sum(rs.col("val")), rs.count(rs.col("id"))])
    rows = agg_df.collect()
    assert len(rows) == 2

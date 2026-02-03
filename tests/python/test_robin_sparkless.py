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


def test_stat_cov_corr():
    """df.stat().cov(col1, col2) and .corr(col1, col2) return float."""
    import robin_sparkless as rs
    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.create_dataframe([(1, 10, "a"), (2, 20, "b"), (3, 30, "c")], ["id", "x", "name"])
    stat = df.stat()
    c = stat.cov("id", "x")
    assert isinstance(c, (int, float))
    r = stat.corr("id", "x")
    assert isinstance(r, (int, float))
    assert -1.0 - 1e-9 <= r <= 1.0 + 1e-9 or (r != r)  # NaN


def test_na_fill_drop():
    """df.na().fill(value) and df.na().drop() exist and run."""
    import robin_sparkless as rs
    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.create_dataframe([(1, 10, "a"), (2, 20, "b"), (3, 30, "c")], ["id", "age", "name"])
    filled = df.na().fill(rs.lit(0))
    assert filled.count() == 3
    dropped = df.na().drop()
    assert dropped.count() == 3


def test_with_columns_and_renamed():
    """with_columns and with_columns_renamed work (dict or list of tuples)."""
    import robin_sparkless as rs
    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.create_dataframe([(1, 25, "Alice")], ["id", "age", "name"])
    out = df.with_columns({"extra": rs.lit(42)})
    rows = out.collect()
    assert rows[0]["extra"] == 42
    renamed = df.with_columns_renamed({"name": "full_name"})
    row = renamed.collect()[0]
    assert "full_name" in row and "name" not in row


def test_to_pandas():
    """to_pandas returns list of dicts (same as collect)."""
    import robin_sparkless as rs
    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.create_dataframe([(1, 25, "Alice")], ["id", "age", "name"])
    result = df.to_pandas()
    assert result is not None
    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0]["id"] == 1 and result[0]["age"] == 25 and result[0]["name"] == "Alice"


def test_phase13_ascii_base64():
    """Phase 13: ascii(column) and base64(column) exist and run."""
    import robin_sparkless as rs
    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.create_dataframe([(1, 65, "A")], ["id", "code", "name"])
    out = df.with_column("ascii_val", rs.ascii(rs.col("name")))
    assert out.count() == 1
    df2 = spark.create_dataframe([(1, 2, "hello")], ["id", "x", "msg"])
    out2 = df2.with_column("enc", rs.base64(rs.col("msg")))
    assert out2.count() == 1


def test_filter_nonexistent_column_raises():
    """Filter with non-existent column raises an error."""
    import robin_sparkless as rs
    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.create_dataframe([(1, 25, "Alice")], ["id", "age", "name"])
    with pytest.raises(Exception):
        df.filter(rs.col("nonexistent").gt(rs.lit(0)))


def test_select_nonexistent_column_raises():
    """Select with non-existent column raises an error."""
    import robin_sparkless as rs
    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.create_dataframe([(1, 25, "Alice")], ["id", "age", "name"])
    with pytest.raises(Exception):
        df.select(["id", "nonexistent"])

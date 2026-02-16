"""Tests for issue #368: duplicate column names in groupBy().agg() result (PySpark parity)."""

import robin_sparkless as rs


def test_group_by_agg_sum_avg_same_column_no_duplicate_error() -> None:
    """groupBy('g').agg(sum('value'), avg('value')) no longer raises duplicate column name."""
    spark = rs.SparkSession.builder().app_name("repro").get_or_create()
    # PySpark-style: createDataFrame with list of dicts + schema, or list of rows + names
    df = spark.createDataFrame(
        [("a", 10), ("a", 20)],
        ["g", "value"],
    )
    result = df.group_by("g").agg([
        rs.sum(rs.col("value")),
        rs.avg(rs.col("value")),
    ]).collect()
    assert len(result) == 1
    row = result[0]
    assert row["g"] == "a"
    # Disambiguation: one column keeps name, duplicate gets _1 (or sum(value)/avg(value) if aliased)
    assert "value" in row or "sum(value)" in row
    assert "value_1" in row or "avg(value)" in row or "value" in row
    # Values: sum(10,20)=30, avg(10,20)=15
    if "sum(value)" in row and "avg(value)" in row:
        assert row["sum(value)"] == 30
        assert row["avg(value)"] == 15.0
    else:
        vals = [v for k, v in row.items() if k != "g" and v is not None]
        assert 30 in vals
        assert 15.0 in vals or 15 in vals


def test_global_agg_duplicate_names() -> None:
    """df.agg(sum('x'), avg('x')) with duplicate output names is disambiguated."""
    spark = rs.SparkSession.builder().app_name("repro").get_or_create()
    df = spark.createDataFrame([(10,), (20,)], ["x"])
    result = df.agg([rs.sum(rs.col("x")), rs.avg(rs.col("x"))]).collect()
    assert len(result) == 1
    row = result[0]
    assert 30 in row.values()  # sum
    assert 15.0 in row.values() or 15 in row.values()  # avg

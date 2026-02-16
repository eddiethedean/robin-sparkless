"""Tests for issue #369: DataFrame.alias() for subqueries (PySpark parity)."""

import robin_sparkless as rs


def test_dataframe_alias_returns_dataframe() -> None:
    """df.alias('t') returns a DataFrame (no AttributeError)."""
    spark = rs.SparkSession.builder().app_name("repro").get_or_create()
    df = spark.createDataFrame([(1, 10)], ["id", "v"])
    aliased = df.alias("t")
    assert aliased is not None
    # Aliased DataFrame should behave like the original for collect
    rows = aliased.collect()
    assert len(rows) == 1
    assert rows[0]["id"] == 1
    assert rows[0]["v"] == 10


def test_dataframe_alias_chaining() -> None:
    """df.alias('t').filter(...) works (alias is preserved/cloned)."""
    spark = rs.SparkSession.builder().app_name("repro").get_or_create()
    df = spark.createDataFrame([(1, 10), (2, 20)], ["id", "v"])
    filtered = df.alias("t").filter(rs.col("id") > 1)
    rows = filtered.collect()
    assert len(rows) == 1
    assert rows[0]["id"] == 2

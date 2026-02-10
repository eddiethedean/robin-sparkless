"""
Tests for issue #200: substr/substring with alias — expression name not found.

Select with col("name").substr(1, 3).alias("partial") must not raise
RuntimeError: not found: partial. The alias name is an output column name,
not an input column to resolve.
"""

from __future__ import annotations


def test_substr_alias_select_collect() -> None:
    """select(col('name').substr(1, 3).alias('partial')) returns column 'partial' (Sparkless parity)."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [{"name": "hello"}],
        [("name", "string")],
    )
    result = df.select(rs.col("name").substr(1, 3).alias("partial"))
    rows = result.collect()
    assert result.columns() == ["partial"]
    assert rows == [{"partial": "hel"}]


def test_substr_alias_multiple_rows() -> None:
    """substr with alias over multiple rows."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [{"name": "abc"}, {"name": "xyz"}, {"name": "hi"}],
        [("name", "string")],
    )
    result = df.select(rs.col("name").substr(1, 2).alias("partial")).collect()
    assert result == [{"partial": "ab"}, {"partial": "xy"}, {"partial": "hi"}]


def test_substr_alias_chained_with_other_expr() -> None:
    """select with substr alias and another column (chained operations)."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [{"s": "hello", "n": 1}],
        [("s", "string"), ("n", "int")],
    )
    result = df.select(
        rs.col("s").substr(1, 3).alias("partial"),
        rs.col("n"),
    ).collect()
    assert result == [{"partial": "hel", "n": 1}]

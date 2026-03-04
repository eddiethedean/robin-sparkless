"""Tests for issue #212: Expression/alias 'not found' in select.

Alias output names (when().otherwise().alias('result'), rank().over([]).alias('rank'),
etc.) must not be resolved as input columns; they are preserved by resolve_expr_column_names (see #200).
"""

from tests.fixtures.spark_imports import get_spark_imports


_imports = get_spark_imports()
SparkSession = _imports.SparkSession
F = _imports.F
Window = _imports.Window


def test_select_when_otherwise_alias() -> None:
    """Select with when().then().otherwise().alias('result') should not raise 'not found: result'."""
    spark = SparkSession.builder.appName("test_212").getOrCreate()
    df = spark.createDataFrame(
        [{"x": 1}, {"x": 2}],
        [("x", "bigint")],
    )
    result = df.select(
        F.when(F.col("x") > F.lit(1))
        .then(F.lit("yes"))
        .otherwise(F.lit("no"))
        .alias("result")
    )
    rows = result.collect()
    assert len(rows) == 2
    assert rows[0]["result"] == "no"
    assert rows[1]["result"] == "yes"


def test_select_window_rank_alias() -> None:
    """Select with rank().over(Window.partitionBy('x')).alias('rank') should not raise 'not found: rank'."""
    spark = SparkSession.builder.appName("test_212").getOrCreate()
    df = spark.createDataFrame(
        [{"x": 10}, {"x": 20}, {"x": 20}],
        [("x", "bigint")],
    )
    # Partition by x so over() has at least one key; alias "rank" must not be resolved as input column.
    window = Window.partitionBy("x")
    result = df.select(F.rank().over(window).alias("rank"))
    rows = result.collect()
    assert len(rows) == 3
    assert all("rank" in r for r in rows)
    assert all(isinstance(r["rank"], int) for r in rows)

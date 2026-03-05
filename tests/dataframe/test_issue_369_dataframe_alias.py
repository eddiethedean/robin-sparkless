from tests.fixtures.spark_imports import get_spark_imports

_imports = get_spark_imports()
F = _imports.F

"""Tests for issue #369: DataFrame.alias() for subqueries (PySpark parity)."""


def test_dataframe_alias_returns_dataframe(spark) -> None:
    """df.alias('t') returns a DataFrame (no AttributeError)."""
    df = spark.createDataFrame([(1, 10)], ["id", "v"])
    aliased = df.alias("t")
    assert aliased is not None
    # Aliased DataFrame should behave like the original for collect
    rows = aliased.collect()
    assert len(rows) == 1
    assert rows[0]["id"] == 1
    assert rows[0]["v"] == 10


def test_dataframe_alias_chaining(spark) -> None:
    """df.alias('t').filter(...) works (alias is preserved/cloned)."""
    df = spark.createDataFrame([(1, 10), (2, 20)], ["id", "v"])
    filtered = df.alias("t").filter(F.col("id") > 1)
    rows = filtered.collect()
    assert len(rows) == 1
    assert rows[0]["id"] == 2

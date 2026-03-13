from sparkless.testing import get_imports

_imports = get_imports()
F = _imports.F

"""Repro for issue #211: astype/cast returns None instead of expected value (PySpark parity)."""


def test_cast_int_to_string_in_with_column(spark) -> None:
    """Basic cast: int to string in with_column; collected value should be '1', not None."""
    df = spark.createDataFrame([{"num": 1}], schema=["num"])
    result = df.withColumn("num_str", F.col("num").cast("string"))
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["num_str"] == "1", f"expected '1', got {rows[0]['num_str']!r}"


def test_cast_int_to_string_in_select(spark) -> None:
    """Cast in select; collected value should be '1', not None."""
    df = spark.createDataFrame([{"num": 1}], schema=["num"])
    result = df.select(F.col("num").cast("string").alias("num_str"))
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["num_str"] == "1", f"expected '1', got {rows[0]['num_str']!r}"

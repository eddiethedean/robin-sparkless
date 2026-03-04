"""Tests for #379: Column string replace (PySpark parity).

PySpark uses regexp_replace for string replacement; sparkless may have replace(old, new).
"""

from __future__ import annotations

from tests.python.utils import get_functions, get_spark

F = get_functions()


def _spark():
    return get_spark("issue_379")


def _replace(col, old: str, new: str):
    """Use regexp_replace (PySpark API)."""
    return F.regexp_replace(col, old, new)


def test_replace_single_pair() -> None:
    """replace(search, replacement) or regexp_replace works."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"x": "a-b-c"}],
        schema=["x"],
    )
    out = df.withColumn("y", _replace(F.col("x"), "-", "_"))
    rows = out.collect()
    assert rows[0]["y"] == "a_b_c"


def test_replace_chained() -> None:
    """Multiple replacements via chained regexp_replace (PySpark-supported)."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"x": "hello world"}],
        schema=["x"],
    )
    col = F.regexp_replace(F.col("x"), "hello", "hi")
    col = F.regexp_replace(col, "world", "earth")
    out = df.withColumn("y", col)
    rows = out.collect()
    assert rows[0]["y"] == "hi earth"

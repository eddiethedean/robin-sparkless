"""Tests for #379: Column string replace (PySpark parity).

PySpark uses regexp_replace for string replacement; sparkless may have replace(old, new).
"""

from __future__ import annotations
import pytest

from tests.fixtures.spark_imports import get_spark_imports

_imports = get_spark_imports()
F = _imports.F


def _replace(col, old: str, new: str):
    """Use regexp_replace (PySpark API)."""
    return F.regexp_replace(col, old, new)


@pytest.mark.skip(reason="Issue #1232: unskip when fixing")
def test_replace_single_pair(spark) -> None:
    """replace(search, replacement) or regexp_replace works."""
    df = spark.createDataFrame(
        [{"x": "a-b-c"}],
        schema=["x"],
    )
    out = df.withColumn("y", _replace(F.col("x"), "-", "_"))
    rows = out.collect()
    assert rows[0]["y"] == "a_b_c"


@pytest.mark.skip(reason="Issue #1232: unskip when fixing")
def test_replace_chained(spark) -> None:
    """Multiple replacements via chained regexp_replace (PySpark-supported)."""
    df = spark.createDataFrame(
        [{"x": "hello world"}],
        schema=["x"],
    )
    col = F.regexp_replace(F.col("x"), "hello", "hi")
    col = F.regexp_replace(col, "world", "earth")
    out = df.withColumn("y", col)
    rows = out.collect()
    assert rows[0]["y"] == "hi earth"

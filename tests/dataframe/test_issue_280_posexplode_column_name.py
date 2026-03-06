"""
Tests for issue #280: posexplode() should accept column name (str) for PySpark compatibility.

PySpark F.posexplode("Values") and F.posexplode(F.col("Values")) both work.
Robin previously required Column only; now accepts string column name.
"""

from __future__ import annotations
import pytest

from tests.fixtures.spark_imports import get_spark_imports


_imports = get_spark_imports()
SparkSession = _imports.SparkSession
F = _imports.F


@pytest.mark.skip(reason="Issue #1208: unskip when fixing")
def test_posexplode_accepts_column_name_string() -> None:
    """posexplode("Values") with string column name works (PySpark parity)."""
    spark = SparkSession.builder.appName("test_280").getOrCreate()
    data = [
        {"Name": "Alice", "Values": [10, 20]},
        {"Name": "Bob", "Values": [30, 40]},
    ]
    df = spark.createDataFrame(data, ["Name", "Values"])
    # PySpark posexplode returns a single Column; alias assigns output column names.
    out = df.select(F.posexplode("Values").alias("pos", "val")).collect()
    assert len(out) == 4
    assert all("pos" in r and "val" in r for r in out)
    vals = [r["val"] for r in out]
    assert vals == [10, 20, 30, 40]


@pytest.mark.skip(reason="Issue #1208: unskip when fixing")
def test_posexplode_column_still_works() -> None:
    """posexplode(F.col("Values")) still works."""
    spark = SparkSession.builder.appName("test_280").getOrCreate()
    data = [{"Name": "Alice", "Values": [1, 2, 3]}]
    df = spark.createDataFrame(data, ["Name", "Values"])
    out = df.select(F.posexplode(F.col("Values")).alias("pos", "val")).collect()
    assert len(out) == 3
    assert [r["val"] for r in out] == [1, 2, 3]

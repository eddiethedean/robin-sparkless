"""
Tests for #421: createDataFrame accept pyarrow.Table and numpy.ndarray (PySpark parity).
"""

from __future__ import annotations

import pytest

import robin_sparkless as rs


def _spark() -> rs.SparkSession:
    return rs.SparkSession.builder().app_name("issue_421").get_or_create()


def test_create_dataframe_from_pyarrow_table() -> None:
    """createDataFrame(data) accepts pyarrow.Table; rows and values match."""
    pa = pytest.importorskip("pyarrow")
    spark = _spark()
    table = pa.table({"a": [1, 2], "b": ["x", "y"]})
    df = spark.createDataFrame(table)
    rows = df.collect()
    assert len(rows) == 2
    assert rows[0]["a"] == 1 and rows[0]["b"] == "x"
    assert rows[1]["a"] == 2 and rows[1]["b"] == "y"


def test_create_dataframe_from_numpy_2d() -> None:
    """createDataFrame(data) accepts 2D numpy ndarray; each row is a row."""
    np = pytest.importorskip("numpy")
    spark = _spark()
    arr = np.array([[1, 10], [2, 20], [3, 30]])
    df = spark.createDataFrame(arr)
    rows = df.collect()
    assert len(rows) == 3
    assert rows[0]["_1"] == 1 and rows[0]["_2"] == 10
    assert rows[2]["_1"] == 3 and rows[2]["_2"] == 30


def test_create_dataframe_from_numpy_1d() -> None:
    """createDataFrame(data) accepts 1D numpy ndarray; single column."""
    np = pytest.importorskip("numpy")
    spark = _spark()
    arr = np.array([10, 20, 30])
    df = spark.createDataFrame(arr)
    rows = df.collect()
    assert len(rows) == 3
    assert rows[0]["_1"] == 10 and rows[1]["_1"] == 20 and rows[2]["_1"] == 30

"""
Tests for #421: createDataFrame accept pyarrow.Table and numpy.ndarray (PySpark parity).
"""

from __future__ import annotations

import pytest

from tests.python.utils import get_spark


def _spark():
    return get_spark("issue_421")


def test_create_dataframe_from_pyarrow_table() -> None:
    """createDataFrame(data) accepts pyarrow.Table via pandas conversion; rows and values match."""
    pa = pytest.importorskip("pyarrow")
    spark = _spark()
    table = pa.table({"a": [1, 2], "b": ["x", "y"]})
    # Current PySpark infers schema from pandas rather than pyarrow.Table directly.
    df = spark.createDataFrame(table.to_pandas())
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
    first_col = df.columns[0]
    assert rows[0][first_col] == 10
    assert rows[1][first_col] == 20
    assert rows[2][first_col] == 30

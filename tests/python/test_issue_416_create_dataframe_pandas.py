"""
Tests for #416: createDataFrame accept pandas.DataFrame (PySpark parity).

PySpark createDataFrame accepts pandas.DataFrame; Robin-sparkless converts via to_dict("records").
"""

from __future__ import annotations

import pytest

import robin_sparkless as rs

pandas = pytest.importorskip("pandas")


def _spark() -> rs.SparkSession:
    return rs.SparkSession.builder().app_name("issue_416").get_or_create()


def test_create_dataframe_from_pandas() -> None:
    """spark.createDataFrame(pandas.DataFrame) works."""
    spark = _spark()
    pdf = pandas.DataFrame({"a": [1, 2], "b": [3, 4]})
    df = spark.createDataFrame(pdf)
    out = df.collect()
    assert len(out) == 2
    assert out[0] == {"a": 1, "b": 3}
    assert out[1] == {"a": 2, "b": 4}

"""
Tests for #416: createDataFrame accept pandas.DataFrame (PySpark parity).

PySpark createDataFrame accepts pandas.DataFrame; Robin-sparkless converts via to_dict("records").
"""

from __future__ import annotations

import pytest

import robin_sparkless as rs

pandas = pytest.importorskip("pandas")


def _spark() -> rs.SparkSession:
    try:
        return rs.SparkSession("issue_416")
    except TypeError:
        pass
    builder = rs.SparkSession.builder
    if callable(builder):
        builder = builder()
    return builder.app_name("issue_416").get_or_create()


def test_create_dataframe_from_pandas() -> None:
    """spark.createDataFrame(pandas.DataFrame) works."""
    spark = _spark()
    pdf = pandas.DataFrame({"a": [1, 2], "b": [3, 4]})
    df = spark.createDataFrame(pdf)
    out = df.collect()
    assert len(out) == 2
    # Row or dict both acceptable (sparkless returns Row, native returns dict)
    def v(r, k):
        return r[k] if isinstance(r, dict) else getattr(r, k)
    assert v(out[0], "a") == 1 and v(out[0], "b") == 3
    assert v(out[1], "a") == 2 and v(out[1], "b") == 4

"""
Tests for #416: createDataFrame accept pandas.DataFrame (PySpark parity).

PySpark createDataFrame accepts pandas.DataFrame; Robin-sparkless converts via to_dict("records").
"""

from __future__ import annotations

import pytest

from tests.utils import get_spark

pandas = pytest.importorskip("pandas")


def _spark():
    return get_spark("issue_416")


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

"""
Tests for issue #245: Column.desc_nulls_last() and nulls ordering methods not found.

With robin-sparkless 0.7.0, col("value").desc_nulls_last() and similar methods
(desc_nulls_first, asc_nulls_last, asc_nulls_first) were not available.
PySpark supports these for controlling null placement in orderBy.
"""

from __future__ import annotations
import pytest

from tests.fixtures.spark_imports import get_spark_imports

_imports = get_spark_imports()
F = _imports.F


def test_column_has_desc_nulls_last(spark) -> None:
    """Column has desc_nulls_last method (PySpark parity)."""
    c = F.col("value")
    assert hasattr(c, "desc_nulls_last")
    assert hasattr(c, "desc_nulls_first")
    assert hasattr(c, "asc_nulls_last")
    assert hasattr(c, "asc_nulls_first")
def test_order_by_desc_nulls_last(spark) -> None:
    """order_by_exprs with col().desc_nulls_last() puts nulls last."""
    data = [{"value": "A"}, {"value": "B"}, {"value": None}, {"value": "C"}]
    df = spark.createDataFrame(data, ["value"])
    out = df.orderBy(F.col("value").desc_nulls_last()).collect()
    assert len(out) == 4
    # Desc with nulls last: C, B, A, null
    values = [r["value"] for r in out]
    assert values[0] == "C"
    assert values[1] == "B"
    assert values[2] == "A"
    assert values[3] is None
def test_order_by_asc_nulls_first(spark) -> None:
    """order_by_exprs with col().asc_nulls_first() puts nulls first."""
    data = [{"value": "A"}, {"value": "B"}, {"value": None}, {"value": "C"}]
    df = spark.createDataFrame(data, ["value"])
    out = df.orderBy(F.col("value").asc_nulls_first()).collect()
    assert len(out) == 4
    # Asc with nulls first: null, A, B, C
    values = [r["value"] for r in out]
    assert values[0] is None
    assert values[1] == "A"
    assert values[2] == "B"
    assert values[3] == "C"

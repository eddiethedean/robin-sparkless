"""
Tests for issue #244: Column.isin() not found.

With robin-sparkless 0.7.0, col("id").isin([]) raised AttributeError.
PySpark supports col.isin([]) (empty list yields 0 rows). We now expose isin on Column.
"""

from __future__ import annotations

from tests.python.utils import get_spark, get_functions


def test_column_isin_empty_list_returns_zero_rows() -> None:
    """col("id").isin([]) filters to 0 rows (PySpark parity)."""
    spark = get_spark("issue_244_isin_empty")
    F = get_functions()
    data = [{"id": 1}, {"id": 2}, {"id": 3}]
    df = spark.createDataFrame(data, ["id"])
    out = df.filter(F.col("id").isin([])).collect()
    assert len(out) == 0


def test_column_isin_non_empty_int_list() -> None:
    """col("id").isin([1, 3]) keeps matching rows."""
    spark = get_spark("issue_244_isin_int")
    F = get_functions()
    data = [{"id": 1}, {"id": 2}, {"id": 3}]
    df = spark.createDataFrame(data, ["id"])
    out = df.filter(F.col("id").isin([1, 3])).collect()
    assert len(out) == 2
    ids = {r["id"] for r in out}
    assert ids == {1, 3}


def test_column_isin_string_list() -> None:
    """col("name").isin(list of str) works."""
    spark = get_spark("issue_244_isin_str")
    F = get_functions()
    data = [{"name": "a"}, {"name": "b"}, {"name": "c"}]
    df = spark.createDataFrame(data, ["name"])
    out = df.filter(F.col("name").isin(["a", "c"])).collect()
    assert len(out) == 2
    names = {r["name"] for r in out}
    assert names == {"a", "c"}

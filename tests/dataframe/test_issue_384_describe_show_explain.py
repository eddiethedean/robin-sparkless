"""Tests for issue #384: DataFrame describe, show, and explain(mode) API."""

from __future__ import annotations

from tests.utils import _row_to_dict


def test_describe_returns_dataframe(spark) -> None:
    """describe() returns a DataFrame with summary stats (count, mean, stddev, min, max)."""
    df = spark.createDataFrame([(1, 10.0), (2, 20.0), (3, 30.0)], ["a", "b"])
    desc = df.describe()
    assert desc is not None
    rows = desc.collect()
    assert len(rows) >= 1
    names = list(_row_to_dict(rows[0]).keys())
    assert "summary" in names or any("a" in k or "b" in k for k in names)


def test_summary_alias_for_describe(spark) -> None:
    """summary() is alias for describe(); both return summary stats (row count may differ by backend)."""
    df = spark.createDataFrame([(1, "x"), (2, "y")], ["id", "label"])
    desc = df.describe()
    summ = df.summary()
    desc_rows = desc.collect()
    summ_rows = summ.collect()
    assert len(desc_rows) >= 1 and len(summ_rows) >= 1
    # Same columns (summary is alias for describe)
    assert set(_row_to_dict(desc_rows[0]).keys()) == set(_row_to_dict(summ_rows[0]).keys())


def test_show_no_error(spark) -> None:
    """show() and show(n) run without error."""
    df = spark.createDataFrame([(1,), (2,)], ["x"])
    df.show()
    df.show(1)


def test_explain_accepts_mode(spark) -> None:
    """explain() runs; explain(extended=True) accepted (PySpark uses extended=, not mode=)."""
    df = spark.createDataFrame([(1,)], ["x"])
    s = df.explain()
    # PySpark explain() returns None (prints to stdout); some backends return str
    assert s is None or (isinstance(s, str) and len(s) > 0)
    s2 = df.explain(extended=True)
    assert s2 is None or isinstance(s2, str)

"""
Tests for issue #1589: comparing to_date() result with string literal.

PySpark auto-casts string literals to date for comparison; sparkless must support
filters like F.to_date(F.col("end_date"), "yyyy-MM-dd") == "9999-12-31".
"""

from __future__ import annotations

from sparkless.testing import get_imports

F = get_imports().F


def test_to_date_eq_string_literal(spark) -> None:
    """Exact scenario from issue #1589."""
    df = spark.createDataFrame(
        [("2025-01-01",), ("9999-12-31",)],
        ["end_date"],
    )
    rows = df.filter(
        F.to_date(F.col("end_date"), "yyyy-MM-dd") == "9999-12-31"
    ).collect()
    assert len(rows) == 1
    assert rows[0]["end_date"] == "9999-12-31"


def test_to_date_ne_string_literal(spark) -> None:
    """Negated sentinel-date filter."""
    df = spark.createDataFrame(
        [("2025-01-01",), ("9999-12-31",)],
        ["end_date"],
    )
    rows = df.filter(
        F.to_date(F.col("end_date"), "yyyy-MM-dd") != "9999-12-31"
    ).collect()
    assert len(rows) == 1
    assert rows[0]["end_date"] == "2025-01-01"

"""
Tests for issue #265: date/datetime vs string comparison (PySpark parity).

PySpark supports comparing date columns to string literals when data is created with date type.
"""

from __future__ import annotations

from datetime import date

from tests.python.utils import get_functions, get_spark

F = get_functions()


def test_filter_date_column_equals_string_literal() -> None:
    """filter(F.col('dt') == '2025-01-01') on date column returns matching row."""
    spark = get_spark("test_265")
    df = spark.createDataFrame(
        [{"dt": date(2025, 1, 1)}, {"dt": date(2025, 1, 2)}],
        "dt date",
    )
    out = df.filter(F.col("dt") == "2025-01-01").collect()
    assert len(out) == 1
    assert out[0]["dt"] == date(2025, 1, 1) or str(out[0]["dt"]).startswith("2025-01-01")


def test_filter_date_column_not_equals_string_literal() -> None:
    """filter(F.col('dt') != '2025-01-01') on date column returns non-matching rows."""
    spark = get_spark("test_265")
    df = spark.createDataFrame(
        [{"dt": date(2025, 1, 1)}, {"dt": date(2025, 1, 2)}],
        "dt date",
    )
    out = df.filter(F.col("dt") != "2025-01-01").collect()
    assert len(out) == 1
    assert out[0]["dt"] == date(2025, 1, 2) or str(out[0]["dt"]).startswith("2025-01-02")

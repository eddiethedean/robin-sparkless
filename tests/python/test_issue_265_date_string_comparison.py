"""
Tests for issue #265: date/datetime vs string comparison (PySpark parity).

PySpark supports comparing date/datetime columns to string literals (implicit cast).
Robin previously raised RuntimeError: cannot compare 'date/datetime/time' to a string value.
"""

from __future__ import annotations

import robin_sparkless as rs

F = rs


def test_filter_date_column_equals_string_literal() -> None:
    """filter(F.col('dt') == '2025-01-01') on date column returns matching row."""
    spark = F.SparkSession.builder().app_name("test_265").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    df = create_df(
        [{"dt": "2025-01-01"}, {"dt": "2025-01-02"}],
        [("dt", "date")],
    )
    out = df.filter(F.col("dt") == "2025-01-01").collect()
    assert len(out) == 1
    # Row may have dt as date or ISO string depending on binding
    assert out[0]["dt"] == "2025-01-01" or str(out[0]["dt"]).startswith("2025-01-01")


def test_filter_date_column_not_equals_string_literal() -> None:
    """filter(F.col('dt') != '2025-01-01') on date column returns non-matching rows."""
    spark = F.SparkSession.builder().app_name("test_265").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    df = create_df(
        [{"dt": "2025-01-01"}, {"dt": "2025-01-02"}],
        [("dt", "date")],
    )
    out = df.filter(F.col("dt") != "2025-01-01").collect()
    assert len(out) == 1
    assert out[0]["dt"] == "2025-01-02" or str(out[0]["dt"]).startswith("2025-01-02")

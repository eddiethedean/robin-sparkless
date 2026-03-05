"""
Tests for issue #265: date/datetime vs string comparison (PySpark parity).

PySpark supports comparing date columns to string literals when data is created with date type.
"""

from __future__ import annotations

from tests.fixtures.spark_imports import get_spark_imports

_imports = get_spark_imports()
F = _imports.F

from datetime import date


def test_filter_date_column_equals_string_literal(spark) -> None:
    """filter(F.col('dt') == '2025-01-01') on date column returns matching row."""
    df = spark.createDataFrame(
        [{"dt": date(2025, 1, 1)}, {"dt": date(2025, 1, 2)}],
        "dt date",
    )
    out = df.filter(F.col("dt") == "2025-01-01").collect()
    assert len(out) == 1
    assert out[0]["dt"] == date(2025, 1, 1) or str(out[0]["dt"]).startswith(
        "2025-01-01"
    )


def test_filter_date_column_not_equals_string_literal(spark) -> None:
    """filter(F.col('dt') != '2025-01-01') on date column returns non-matching rows."""
    df = spark.createDataFrame(
        [{"dt": date(2025, 1, 1)}, {"dt": date(2025, 1, 2)}],
        "dt date",
    )
    out = df.filter(F.col("dt") != "2025-01-01").collect()
    assert len(out) == 1
    assert out[0]["dt"] == date(2025, 1, 2) or str(out[0]["dt"]).startswith(
        "2025-01-02"
    )

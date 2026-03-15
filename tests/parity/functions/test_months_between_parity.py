"""
Tests for months_between() PySpark parity.

Issue #1481: months_between() was using total_days/31 instead of PySpark's
formula: (year1-year2)*12 + (month1-month2) + (day1-day2)/31.
"""

from sparkless.testing import get_imports

F = get_imports().F


def test_months_between_issue_1481(spark):
    """Exact reproduction from issue #1481: 2024-01-15 to 2024-06-20."""
    df = spark.createDataFrame([{"start": "2024-01-15", "end": "2024-06-20"}])
    result = df.select(
        F.months_between(F.to_date("end"), F.to_date("start")).alias("months")
    )
    got = result.collect()[0]["months"]
    # PySpark: (6-1) + (20-15)/31 = 5.16129032
    expected = 5.16129032
    assert abs(got - expected) < 1e-6, f"expected {expected}, got {got}"


def test_months_between_same_day(spark):
    """Same day of month -> integer result."""
    df = spark.createDataFrame([{"start": "2024-01-15", "end": "2024-04-15"}])
    result = df.select(
        F.months_between(F.to_date("end"), F.to_date("start")).alias("months")
    )
    got = result.collect()[0]["months"]
    assert got == 3.0


def test_months_between_fractional(spark):
    """Fractional part uses (day1-day2)/31."""
    df = spark.createDataFrame([{"start": "2024-01-01", "end": "2024-02-11"}])
    result = df.select(
        F.months_between(F.to_date("end"), F.to_date("start")).alias("months")
    )
    got = result.collect()[0]["months"]
    # 1 month + (11-1)/31 = 1 + 10/31 ≈ 1.32258065
    expected = 1.0 + 10.0 / 31.0
    assert abs(got - expected) < 1e-6, f"expected {expected}, got {got}"

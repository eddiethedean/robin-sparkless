"""Regression tests for issue #1569: date_add(unit, quantity, start) in spark.sql."""

from __future__ import annotations

from datetime import date

from sparkless.testing import get_imports

_imports = get_imports()
F = _imports.F


def test_sql_date_add_month_unit_with_column(spark) -> None:
    """date_add(MONTH, n, date_col) must not treat MONTH as a column name."""
    df = spark.createDataFrame([("2024-06-15",)], ["d"])
    df.createOrReplaceTempView("dates")
    rows = spark.sql(
        "SELECT date_add(MONTH, -1, to_date(d, 'yyyy-MM-dd')) AS result FROM dates"
    ).collect()
    assert len(rows) == 1
    val = rows[0]["result"]
    assert val is not None
    assert str(val)[:10].startswith("2024-05")


def test_sql_date_add_day_two_arg(spark) -> None:
    """Legacy date_add(start, days) still works in SQL."""
    df = spark.createDataFrame([("2024-01-01",)], ["d"])
    df.createOrReplaceTempView("dates")
    rows = spark.sql(
        "SELECT date_add(to_date(d, 'yyyy-MM-dd'), 7) AS result FROM dates"
    ).collect()
    assert len(rows) == 1
    assert str(rows[0]["result"])[:10] == "2024-01-08"


def test_sql_date_add_month_current_date(spark) -> None:
    """PySpark overload: date_add(MONTH, -13, current_date()) runs without unresolved MONTH."""
    rows = spark.sql(
        "SELECT date_add(MONTH, -13, current_date()) AS result"
    ).collect()
    assert len(rows) == 1
    assert rows[0]["result"] is not None


def test_sql_date_add_month_matches_add_months(spark) -> None:
    """Three-arg MONTH form matches add_months on the same anchor date."""
    anchor = date(2024, 6, 4)
    df = spark.createDataFrame([(anchor.isoformat(),)], ["d"])
    df.createOrReplaceTempView("t")
    sql_val = spark.sql(
        "SELECT date_add(MONTH, -2, to_date(d, 'yyyy-MM-dd')) AS result FROM t"
    ).collect()[0]["result"]
    api_val = df.select(
        F.add_months(F.to_date("d", "yyyy-MM-dd"), -2).alias("result")
    ).collect()[0]["result"]
    assert str(sql_val)[:10] == str(api_val)[:10] == "2024-04-04"

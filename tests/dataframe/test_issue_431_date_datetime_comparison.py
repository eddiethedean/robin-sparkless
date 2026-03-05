"""Test issue #431: date vs datetime comparison must not raise TypeError.

PySpark allows comparing date and datetime columns. Sparkless must coerce
date to datetime (at midnight) for comparison instead of raising
TypeError: can't compare datetime.datetime to datetime.date.

Run in PySpark mode first, then mock mode:
  MOCK_SPARK_TEST_BACKEND=pyspark pytest tests/test_issue_431_date_datetime_comparison.py -v
  pytest tests/test_issue_431_date_datetime_comparison.py -v

https://github.com/eddiethedean/sparkless/issues/431
"""

import datetime

from tests.fixtures.spark_imports import get_spark_imports


def test_date_less_than_datetime(spark, spark_backend):
    """F.col('Date') < F.col('DateTime') must filter correctly (#431)."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [
            {
                "Name": "Alice",
                "DateTime": datetime.datetime(2023, 1, 1, 12, 0, 0),
                "Date": datetime.date(2024, 1, 1),
            },
            {
                "Name": "Bob",
                "DateTime": datetime.datetime(2024, 1, 1, 12, 0, 0),
                "Date": datetime.date(2024, 1, 1),
            },
        ]
    )
    result = df.filter(F_backend.col("Date") < F_backend.col("DateTime"))
    rows = result.collect()

    # Alice: Date 2024-01-01 < DateTime 2023-01-01 12:00 -> False (filtered out)
    # Bob: Date 2024-01-01 < DateTime 2024-01-01 12:00 -> True (kept)
    assert len(rows) == 1
    assert rows[0]["Name"] == "Bob"
    assert rows[0]["Date"] == datetime.date(2024, 1, 1)
    assert rows[0]["DateTime"] == datetime.datetime(2024, 1, 1, 12, 0, 0)


def test_datetime_greater_than_date(spark, spark_backend):
    """F.col('DateTime') > F.col('Date') must work."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [
            {
                "dt": datetime.datetime(2024, 6, 15, 10, 0, 0),
                "d": datetime.date(2024, 1, 1),
            },
        ]
    )
    result = df.filter(F_backend.col("dt") > F_backend.col("d"))
    rows = result.collect()
    assert len(rows) == 1


def test_date_eq_datetime(spark, spark_backend):
    """Date equals datetime at midnight."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [
            {
                "d": datetime.date(2024, 1, 1),
                "dt": datetime.datetime(2024, 1, 1, 0, 0, 0),
            },
            {
                "d": datetime.date(2024, 1, 1),
                "dt": datetime.datetime(2024, 1, 1, 12, 0, 0),
            },
        ]
    )
    result = df.filter(F_backend.col("d") == F_backend.col("dt"))
    rows = result.collect()
    # Date 2024-01-01 == datetime 2024-01-01 00:00:00 -> True
    # Date 2024-01-01 == datetime 2024-01-01 12:00:00 -> False
    assert len(rows) == 1
    assert rows[0]["dt"] == datetime.datetime(2024, 1, 1, 0, 0, 0)


def test_date_lte_datetime(spark, spark_backend):
    """F.col('Date') <= F.col('DateTime') must work."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [
            {
                "d": datetime.date(2024, 1, 1),
                "dt": datetime.datetime(2024, 1, 1, 0, 0, 0),
            },
            {
                "d": datetime.date(2024, 1, 1),
                "dt": datetime.datetime(2023, 12, 31, 23, 59, 0),
            },
        ]
    )
    result = df.filter(F_backend.col("d") <= F_backend.col("dt"))
    rows = result.collect()
    # Date 2024-01-01 <= datetime 2024-01-01 00:00:00 -> True (equal when coerced)
    # Date 2024-01-01 <= datetime 2023-12-31 23:59 -> False
    assert len(rows) == 1
    assert rows[0]["dt"] == datetime.datetime(2024, 1, 1, 0, 0, 0)


def test_date_gte_datetime(spark, spark_backend):
    """F.col('Date') >= F.col('DateTime') must work."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [
            {
                "d": datetime.date(2024, 6, 15),
                "dt": datetime.datetime(2024, 1, 1, 12, 0, 0),
            },
            {
                "d": datetime.date(2023, 12, 1),
                "dt": datetime.datetime(2024, 1, 1, 12, 0, 0),
            },
        ]
    )
    result = df.filter(F_backend.col("d") >= F_backend.col("dt"))
    rows = result.collect()
    # Date 2024-06-15 >= datetime 2024-01-01 12:00 -> True
    # Date 2023-12-01 >= datetime 2024-01-01 12:00 -> False
    assert len(rows) == 1
    assert rows[0]["d"] == datetime.date(2024, 6, 15)


def test_datetime_less_than_date(spark, spark_backend):
    """F.col('DateTime') < F.col('Date') - datetime on left, date on right."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [
            {
                "dt": datetime.datetime(2023, 6, 15, 10, 0, 0),
                "d": datetime.date(2024, 1, 1),
            },
            {
                "dt": datetime.datetime(2024, 6, 15, 10, 0, 0),
                "d": datetime.date(2024, 1, 1),
            },
        ]
    )
    result = df.filter(F_backend.col("dt") < F_backend.col("d"))
    rows = result.collect()
    # dt 2023-06-15 < d 2024-01-01 -> True
    # dt 2024-06-15 < d 2024-01-01 -> False
    assert len(rows) == 1
    assert rows[0]["dt"] == datetime.datetime(2023, 6, 15, 10, 0, 0)


def test_date_ne_datetime(spark, spark_backend):
    """F.col('Date') != F.col('DateTime') must work."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [
            {
                "d": datetime.date(2024, 1, 1),
                "dt": datetime.datetime(2024, 1, 1, 12, 0, 0),
            },
            {
                "d": datetime.date(2024, 1, 1),
                "dt": datetime.datetime(2024, 1, 1, 0, 0, 0),
            },
        ]
    )
    result = df.filter(F_backend.col("d") != F_backend.col("dt"))
    rows = result.collect()
    # Date 2024-01-01 != datetime 2024-01-01 12:00 -> True
    # Date 2024-01-01 != datetime 2024-01-01 00:00 -> False (equal when coerced)
    assert len(rows) == 1
    assert rows[0]["dt"] == datetime.datetime(2024, 1, 1, 12, 0, 0)


def test_date_datetime_chained_filter(spark, spark_backend):
    """Chained filter with date/datetime comparison."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [
            {
                "id": 1,
                "d": datetime.date(2024, 1, 15),
                "dt": datetime.datetime(2024, 1, 20, 10, 0, 0),
            },
            {
                "id": 2,
                "d": datetime.date(2024, 1, 15),
                "dt": datetime.datetime(2024, 1, 10, 10, 0, 0),
            },
            {
                "id": 3,
                "d": datetime.date(2024, 1, 5),
                "dt": datetime.datetime(2024, 1, 20, 10, 0, 0),
            },
        ]
    )
    result = df.filter(F_backend.col("d") < F_backend.col("dt")).filter(
        F_backend.col("id") >= 2
    )
    rows = result.collect()
    # id=1: d < dt (15 < 20) True, id>=2 False -> filtered
    # id=2: d < dt (15 < 10) False -> filtered
    # id=3: d < dt (5 < 20) True, id>=2 True -> kept
    assert len(rows) == 1
    assert rows[0]["id"] == 3


def test_date_datetime_with_and(spark, spark_backend):
    """Date/datetime comparison combined with AND."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [
            {
                "name": "A",
                "d": datetime.date(2024, 1, 1),
                "dt": datetime.datetime(2024, 1, 1, 12, 0, 0),
            },
            {
                "name": "B",
                "d": datetime.date(2024, 1, 1),
                "dt": datetime.datetime(2023, 1, 1, 12, 0, 0),
            },
        ]
    )
    result = df.filter(
        (F_backend.col("d") < F_backend.col("dt")) & (F_backend.col("name") == "A")
    )
    rows = result.collect()
    # A: d < dt True, name=="A" True -> kept
    # B: d < dt False -> filtered
    assert len(rows) == 1
    assert rows[0]["name"] == "A"


def test_date_datetime_orderby(spark, spark_backend):
    """orderBy after date/datetime filter."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [
            {
                "id": 1,
                "d": datetime.date(2024, 3, 1),
                "dt": datetime.datetime(2024, 3, 15, 0, 0, 0),
            },
            {
                "id": 2,
                "d": datetime.date(2024, 1, 1),
                "dt": datetime.datetime(2024, 1, 15, 0, 0, 0),
            },
            {
                "id": 3,
                "d": datetime.date(2024, 2, 1),
                "dt": datetime.datetime(2024, 2, 15, 0, 0, 0),
            },
        ]
    )
    result = df.filter(F_backend.col("d") < F_backend.col("dt")).orderBy("id")
    rows = result.collect()
    assert len(rows) == 3
    assert [r["id"] for r in rows] == [1, 2, 3]


def test_exact_scenario_from_issue_431(spark, spark_backend):
    """Exact scenario from issue #431 - must not raise and return Bob row."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [
            {
                "Name": "Alice",
                "DateTime": datetime.datetime(2023, 1, 1, 12, 0, 0),
                "Date": datetime.date(2024, 1, 1),
            },
            {
                "Name": "Bob",
                "DateTime": datetime.datetime(2024, 1, 1, 12, 0, 0),
                "Date": datetime.date(2024, 1, 1),
            },
        ]
    )
    result = df.filter(F_backend.col("Date") < F_backend.col("DateTime"))
    result.show()  # Previously failed here with TypeError
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["Name"] == "Bob"

"""Test issue #432: cast('timestamp') and cast('date') on already-typed columns.

PySpark allows .cast('timestamp') on datetime and .cast('date') on date as no-ops.
Sparkless must not raise SchemaError: expected String, got datetime[μs].

https://github.com/eddiethedean/sparkless/issues/432
"""

import datetime

from tests.fixtures.spark_imports import get_spark_imports


def test_cast_datetime_to_timestamp_noop(spark, spark_backend):
    """F.col('DateTime').cast('timestamp') on datetime column must not raise (#432)."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [
            {"Name": "Alice", "DateTime": datetime.datetime(2023, 1, 1, 12, 0, 0)},
            {"Name": "Bob", "DateTime": datetime.datetime(2024, 1, 1, 12, 0, 0)},
        ]
    )
    result = df.withColumn("DateTime", F_backend.col("DateTime").cast("timestamp"))
    rows = result.collect()
    assert len(rows) == 2
    assert rows[0]["DateTime"] == datetime.datetime(2023, 1, 1, 12, 0, 0)
    assert rows[1]["DateTime"] == datetime.datetime(2024, 1, 1, 12, 0, 0)


def test_cast_date_to_date_noop(spark, spark_backend):
    """F.col('Date').cast('date') on date column must not raise (#432)."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [
            {"Name": "Alice", "Date": datetime.date(2024, 1, 1)},
            {"Name": "Bob", "Date": datetime.date(2024, 6, 15)},
        ]
    )
    result = df.withColumn("Date", F_backend.col("Date").cast("date"))
    rows = result.collect()
    assert len(rows) == 2
    assert rows[0]["Date"] == datetime.date(2024, 1, 1)
    assert rows[1]["Date"] == datetime.date(2024, 6, 15)


def test_exact_scenario_from_issue_432(spark, spark_backend):
    """Exact scenario from issue #432 - must not raise and show correctly."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [
            {
                "Name": "Alice",
                "DateTime": datetime.datetime(2023, 1, 1, 12, 0, 0),
                "Date": datetime.date(2024, 1, 1),
                "DateTimeString": "2023-01-01 12:00:00",
                "DateString": "2024-01-01",
            },
            {
                "Name": "Bob",
                "DateTime": datetime.datetime(2024, 1, 1, 12, 0, 0),
                "Date": datetime.date(2024, 1, 1),
                "DateTimeString": "2023-01-01 12:00:00",
                "DateString": "2024-01-01",
            },
        ]
    )
    df = df.withColumn("DateTime", F_backend.col("DateTime").cast("timestamp"))
    df = df.withColumn("Date", F_backend.col("Date").cast("date"))
    df = df.withColumn(
        "DateTimeString", F_backend.col("DateTimeString").cast("timestamp")
    )
    df = df.withColumn("DateString", F_backend.col("DateString").cast("date"))
    df.show()  # Previously failed here with SchemaError
    rows = df.collect()
    assert len(rows) == 2
    assert rows[0]["Name"] == "Alice"
    assert rows[1]["Name"] == "Bob"


def test_cast_string_to_timestamp_still_works(spark, spark_backend):
    """String->timestamp cast must still work."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame([{"s": "2024-01-15 10:30:00"}])
    result = df.withColumn("s", F_backend.col("s").cast("timestamp"))
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["s"] == datetime.datetime(2024, 1, 15, 10, 30, 0)


def test_cast_string_to_date_still_works(spark, spark_backend):
    """String->date cast must still work."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame([{"s": "2024-01-15"}])
    result = df.withColumn("s", F_backend.col("s").cast("date"))
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["s"] == datetime.date(2024, 1, 15)


# --- Robust edge-case tests ---


def test_cast_datetime_to_timestamp_with_nulls(spark, spark_backend):
    """datetime.cast('timestamp') with None values must not raise and preserve nulls."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [
            {"id": 1, "dt": datetime.datetime(2023, 1, 1, 12, 0, 0)},
            {"id": 2, "dt": None},
            {"id": 3, "dt": datetime.datetime(2024, 6, 15, 0, 0, 0)},
        ]
    )
    result = df.withColumn("dt", F_backend.col("dt").cast("timestamp"))
    rows = result.collect()
    assert len(rows) == 3
    assert rows[0]["dt"] == datetime.datetime(2023, 1, 1, 12, 0, 0)
    assert rows[1]["dt"] is None
    assert rows[2]["dt"] == datetime.datetime(2024, 6, 15, 0, 0, 0)


def test_cast_date_to_date_with_nulls(spark, spark_backend):
    """date.cast('date') with None values must not raise and preserve nulls."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [
            {"id": 1, "d": datetime.date(2024, 1, 1)},
            {"id": 2, "d": None},
            {"id": 3, "d": datetime.date(2024, 12, 31)},
        ]
    )
    result = df.withColumn("d", F_backend.col("d").cast("date"))
    rows = result.collect()
    assert len(rows) == 3
    assert rows[0]["d"] == datetime.date(2024, 1, 1)
    assert rows[1]["d"] is None
    assert rows[2]["d"] == datetime.date(2024, 12, 31)


def test_cast_date_to_timestamp_midnight(spark, spark_backend):
    """date.cast('timestamp') must become datetime at midnight."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame([{"d": datetime.date(2024, 3, 15)}])
    result = df.withColumn("ts", F_backend.col("d").cast("timestamp"))
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["ts"] == datetime.datetime(2024, 3, 15, 0, 0, 0)


def test_cast_datetime_to_date_truncates_time(spark, spark_backend):
    """datetime.cast('date') must keep only the date part."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame([{"dt": datetime.datetime(2024, 5, 10, 14, 30, 45)}])
    result = df.withColumn("d", F_backend.col("dt").cast("date"))
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["d"] == datetime.date(2024, 5, 10)


def test_cast_date_only_string_to_timestamp(spark, spark_backend):
    """'YYYY-MM-DD' string cast to timestamp must become midnight."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame([{"s": "2024-01-15"}])
    result = df.withColumn("ts", F_backend.col("s").cast("timestamp"))
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["ts"] == datetime.datetime(2024, 1, 15, 0, 0, 0)


def test_cast_select_with_cast(spark, spark_backend):
    """select(F.col('DateTime').cast('timestamp')) must work on datetime column."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [{"Name": "A", "DateTime": datetime.datetime(2023, 1, 1, 12, 0, 0)}]
    )
    result = df.select(
        F_backend.col("Name"),
        F_backend.col("DateTime").cast("timestamp").alias("DateTime"),
    )
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["DateTime"] == datetime.datetime(2023, 1, 1, 12, 0, 0)


def test_cast_filter_after_cast(spark, spark_backend):
    """withColumn(cast) then filter must work."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [
            {"id": 1, "dt": datetime.datetime(2023, 1, 1, 12, 0, 0)},
            {"id": 2, "dt": datetime.datetime(2024, 6, 1, 12, 0, 0)},
        ]
    )
    result = df.withColumn("dt", F_backend.col("dt").cast("timestamp")).filter(
        F_backend.col("dt") >= F_backend.lit(datetime.datetime(2024, 1, 1))
    )
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["id"] == 2


def test_cast_with_datatype_objects(spark, spark_backend):
    """cast(TimestampType()) and cast(DateType()) on typed columns must work."""
    imports = get_spark_imports(spark_backend)
    F_backend = imports.F
    TimestampType = imports.TimestampType
    DateType = imports.DateType
    df = spark.createDataFrame(
        [
            {
                "dt": datetime.datetime(2023, 1, 1, 12, 0, 0),
                "d": datetime.date(2024, 1, 1),
            }
        ]
    )
    result = df.withColumn("dt", F_backend.col("dt").cast(TimestampType())).withColumn(
        "d", F_backend.col("d").cast(DateType())
    )
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["dt"] == datetime.datetime(2023, 1, 1, 12, 0, 0)
    assert rows[0]["d"] == datetime.date(2024, 1, 1)


def test_cast_leap_day(spark, spark_backend):
    """Leap day 2024-02-29 must cast correctly."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [
            {"d": datetime.date(2024, 2, 29)},
        ]
    )
    result = df.withColumn("d", F_backend.col("d").cast("date")).withColumn(
        "ts", F_backend.col("d").cast("timestamp")
    )
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["d"] == datetime.date(2024, 2, 29)
    assert rows[0]["ts"] == datetime.datetime(2024, 2, 29, 0, 0, 0)

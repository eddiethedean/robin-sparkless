"""
Tests for issue #1596: join on TimestampType vs DateType.

PySpark coerces date join keys to timestamp; sparkless previously raised
"cannot find common type for Datetime and Date".
"""

from __future__ import annotations

from datetime import date, datetime

from sparkless.testing import get_imports

DateType = get_imports().DateType
IntegerType = get_imports().IntegerType
StructField = get_imports().StructField
StructType = get_imports().StructType
TimestampType = get_imports().TimestampType


def test_join_timestamp_left_date_right(spark) -> None:
    """Exact scenario from issue #1596."""
    schema_ts = StructType([StructField("col1", TimestampType())])
    schema_dt = StructType(
        [
            StructField("col1", DateType()),
            StructField("col2", IntegerType()),
        ]
    )

    df_a = spark.createDataFrame(
        [(datetime(2025, 1, 1),), (datetime(2025, 1, 2),)],
        schema_ts,
    )
    df_b = spark.createDataFrame([(date(2025, 1, 1), 100)], schema_dt)

    rows = sorted(
        df_a.join(df_b, "col1", "left").collect(),
        key=lambda r: r["col1"],
    )
    assert len(rows) == 2

    matched = rows[0]
    assert matched["col1"] == datetime(2025, 1, 1)
    assert matched["col2"] == 100

    unmatched = rows[1]
    assert unmatched["col1"] == datetime(2025, 1, 2)
    assert unmatched["col2"] is None


def test_join_date_left_timestamp_right(spark) -> None:
    """Join with date on the left and timestamp on the right."""
    schema_dt = StructType([StructField("col1", DateType())])
    schema_ts = StructType(
        [
            StructField("col1", TimestampType()),
            StructField("col2", IntegerType()),
        ]
    )

    df_a = spark.createDataFrame([(date(2025, 1, 1),)], schema_dt)
    df_b = spark.createDataFrame(
        [(datetime(2025, 1, 1), 42), (datetime(2025, 1, 2), 99)],
        schema_ts,
    )

    row = df_a.join(df_b, "col1", "inner").collect()[0]
    assert row["col1"] in (date(2025, 1, 1), datetime(2025, 1, 1))
    assert row["col2"] == 42

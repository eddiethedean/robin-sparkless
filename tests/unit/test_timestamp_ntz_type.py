from datetime import datetime

import pytest
from sparkless.testing import get_imports

pytestmark = pytest.mark.sparkless_only
T = get_imports()


def test_timestamp_ntz_schema_and_values_remain_timezone_free(spark):
    value = datetime(2026, 10, 20, 12, 34, 56)
    frame = spark.createDataFrame(
        [(value,)], T.StructType([T.StructField("event_time", T.TimestampNTZType())])
    )

    assert frame.dtypes == [("event_time", "timestamp_ntz")]
    assert type(frame.schema["event_time"].dataType) is T.TimestampNTZType
    collected = frame.collect()[0]["event_time"]
    assert collected == value
    assert collected.tzinfo is None


def test_timestamp_ntz_string_schema_is_not_downgraded_to_string(spark):
    value = datetime(2026, 10, 20, 12, 34, 56)
    frame = spark.createDataFrame([(value,)], [("event_time", "timestamp_ntz")])

    assert frame.dtypes == [("event_time", "timestamp_ntz")]
    assert type(frame.schema["event_time"].dataType) is T.TimestampNTZType
    assert frame.collect()[0]["event_time"] == value


def test_to_timestamp_and_to_timestamp_ntz_keep_distinct_logical_types(spark):
    frame = spark.createDataFrame([("2026-10-20 12:34:56",)], ["raw"])
    instant = frame.select(T.F.to_timestamp("raw").alias("event_time"))
    local = frame.select(T.F.to_timestamp_ntz("raw").alias("event_time"))

    assert type(instant.schema["event_time"].dataType) is T.TimestampType
    assert type(local.schema["event_time"].dataType) is T.TimestampNTZType
    assert instant.collect()[0]["event_time"].tzinfo is None
    assert local.collect()[0]["event_time"].tzinfo is None

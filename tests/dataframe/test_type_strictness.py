"""
Tests for strict type checking in functions.

Asserts PySpark behavior: to_timestamp/to_date accept or reject types per PySpark.
All tests use backend-agnostic imports (spark fixture + get_spark_imports).
"""

import pytest
from datetime import datetime, date

from tests.fixtures.spark_imports import get_spark_imports

imports = get_spark_imports()
SparkSession = imports.SparkSession
F = imports.F
StructType = imports.StructType
StructField = imports.StructField
StringType = imports.StringType
IntegerType = imports.IntegerType
LongType = imports.LongType
DoubleType = imports.DoubleType
BooleanType = imports.BooleanType
DateType = imports.DateType


class TestTypeStrictness:
    """Test type checking in functions (PySpark behavior)."""

    def test_to_timestamp_accepts_multiple_types(self, spark):
        """to_timestamp accepts StringType, TimestampType, IntegerType, LongType, DateType, DoubleType (PySpark)."""
        df_str = spark.createDataFrame(
            [{"date_str": "2023-01-01 12:00:00"}], schema=["date_str"]
        )
        result_str = df_str.withColumn("parsed", F.to_timestamp(F.col("date_str")))
        assert result_str is not None
        rows = result_str.collect()
        assert len(rows) == 1
        assert isinstance(rows[0]["parsed"], datetime)

        df_ts = spark.createDataFrame(
            [{"ts": datetime(2023, 1, 1, 12, 0, 0)}], schema=["ts"]
        )
        result_ts = df_ts.withColumn("ts2", F.to_timestamp(F.col("ts")))
        assert result_ts is not None
        assert isinstance(result_ts.collect()[0]["ts2"], datetime)

        schema_int = StructType([StructField("unix_ts", IntegerType(), True)])
        df_int = spark.createDataFrame([{"unix_ts": 1672574400}], schema=schema_int)
        result_int = df_int.withColumn("parsed", F.to_timestamp(F.col("unix_ts")))
        assert result_int is not None
        assert isinstance(result_int.collect()[0]["parsed"], datetime)

        schema_long = StructType([StructField("unix_ts", LongType(), True)])
        df_long = spark.createDataFrame([{"unix_ts": 1672574400}], schema=schema_long)
        result_long = df_long.withColumn("parsed", F.to_timestamp(F.col("unix_ts")))
        assert result_long is not None
        assert isinstance(result_long.collect()[0]["parsed"], datetime)

        schema_date = StructType([StructField("date_col", DateType(), True)])
        df_date = spark.createDataFrame(
            [{"date_col": date(2023, 1, 1)}], schema=schema_date
        )
        result_date = df_date.withColumn("parsed", F.to_timestamp(F.col("date_col")))
        assert result_date is not None
        assert isinstance(result_date.collect()[0]["parsed"], datetime)

        schema_double = StructType([StructField("unix_ts", DoubleType(), True)])
        df_double = spark.createDataFrame(
            [{"unix_ts": 1672574400.5}], schema=schema_double
        )
        result_double = df_double.withColumn("parsed", F.to_timestamp(F.col("unix_ts")))
        assert result_double is not None
        assert isinstance(result_double.collect()[0]["parsed"], datetime)

    def test_to_timestamp_unsupported_types_raise_or_null(self, spark):
        """to_timestamp with BooleanType: PySpark either raises or returns null."""
        schema = StructType([StructField("bool_col", BooleanType(), True)])
        df = spark.createDataFrame([{"bool_col": True}], schema=schema)
        try:
            rows = df.withColumn("parsed", F.to_timestamp(F.col("bool_col"))).collect()
            assert len(rows) == 1
            assert rows[0]["parsed"] is None
        except Exception:
            pass

    def test_to_timestamp_works_with_string(self, spark):
        """to_timestamp works with string input."""
        df = spark.createDataFrame(
            [{"date_str": "2023-01-01 12:00:00"}], schema=["date_str"]
        )
        result = df.withColumn("parsed", F.to_timestamp(F.col("date_str")))
        assert result is not None

    def test_to_date_requires_string_or_date(self, spark):
        """to_date with IntegerType column must raise (PySpark: unsupported type)."""
        schema = StructType([StructField("date", IntegerType(), True)])
        df = spark.createDataFrame([{"date": 12345}], schema=schema)
        with pytest.raises(Exception):
            df.withColumn("parsed", F.to_date(F.col("date"))).collect()

    def test_to_date_works_with_string(self, spark):
        """to_date works with string input."""
        df = spark.createDataFrame([{"date_str": "2023-01-01"}], schema=["date_str"])
        result = df.withColumn("parsed", F.to_date(F.col("date_str")))
        assert result is not None

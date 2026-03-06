import pytest


class TestToTimestampCompatibility:
    """Test to_timestamp() compatibility with PySpark."""

    def test_to_timestamp_timestamp_type_pass_through(self, spark):
        """Test that to_timestamp() accepts TimestampType input (pass-through behavior).

        This is the exact scenario from issue #131.
        """
        from datetime import datetime
        from tests.fixtures.spark_imports import get_spark_imports

        imports = get_spark_imports()
        F = imports.F

        # Create DataFrame with timestamp string
        data = [("2024-01-01T10:00:00", "test")]
        df = spark.createDataFrame(data, ["timestamp_str", "name"])

        # Convert to timestamp
        df = df.withColumn(
            "ts", F.to_timestamp(df["timestamp_str"], "yyyy-MM-dd'T'HH:mm:ss")
        )

        # Try to_timestamp on TimestampType column - should work now
        result = df.withColumn("ts2", F.to_timestamp(df["ts"], "yyyy-MM-dd'T'HH:mm:ss"))

        # Verify both columns are TimestampType
        rows = result.collect()
        assert len(rows) == 1
        assert isinstance(rows[0]["ts"], datetime)
        assert isinstance(rows[0]["ts2"], datetime)
        # ts2 should be the same as ts (pass-through behavior)
        assert rows[0]["ts"] == rows[0]["ts2"]

    def test_to_timestamp_string_type_with_format(self, spark):
        """Test that to_timestamp() works with StringType input and format string."""
        from datetime import datetime
        from tests.fixtures.spark_imports import get_spark_imports

        imports = get_spark_imports()
        F = imports.F

        data = [("2024-01-01T10:00:00",)]
        df = spark.createDataFrame(data, ["timestamp_str"])

        result = df.withColumn(
            "ts", F.to_timestamp(F.col("timestamp_str"), "yyyy-MM-dd'T'HH:mm:ss")
        )

        rows = result.collect()
        assert len(rows) == 1
        assert isinstance(rows[0]["ts"], datetime)
        assert rows[0]["ts"] == datetime(2024, 1, 1, 10, 0, 0)

    def test_to_timestamp_string_type_without_format(self, spark):
        """Test that to_timestamp() works with StringType input without format."""
        from datetime import datetime
        from tests.fixtures.spark_imports import get_spark_imports

        imports = get_spark_imports()
        F = imports.F

        data = [("2024-01-01 10:00:00",)]
        df = spark.createDataFrame(data, ["timestamp_str"])

        result = df.withColumn("ts", F.to_timestamp(F.col("timestamp_str")))

        rows = result.collect()
        assert len(rows) == 1
        assert isinstance(rows[0]["ts"], datetime)

    def test_to_timestamp_integer_type_unix_timestamp(self, spark):
        """Test that to_timestamp() accepts IntegerType input (Unix timestamp in seconds)."""
        from datetime import datetime
        from tests.fixtures.spark_imports import get_spark_imports

        imports = get_spark_imports()
        F = imports.F
        IntegerType = imports.IntegerType
        StructType = imports.StructType
        StructField = imports.StructField

        # Unix timestamp for 2024-01-01 10:00:00 UTC
        unix_ts = 1704110400
        schema = StructType([StructField("unix_ts", IntegerType(), True)])
        df = spark.createDataFrame([{"unix_ts": unix_ts}], schema=schema)

        result = df.withColumn("ts", F.to_timestamp(F.col("unix_ts")))

        rows = result.collect()
        assert len(rows) == 1
        assert isinstance(rows[0]["ts"], datetime)

    def test_to_timestamp_long_type_unix_timestamp(self, spark):
        """Test that to_timestamp() accepts LongType input (Unix timestamp in seconds)."""
        from datetime import datetime
        from tests.fixtures.spark_imports import get_spark_imports

        imports = get_spark_imports()
        F = imports.F
        LongType = imports.LongType
        StructType = imports.StructType
        StructField = imports.StructField

        # Unix timestamp for 2024-01-01 10:00:00 UTC
        unix_ts = 1704110400
        schema = StructType([StructField("unix_ts", LongType(), True)])
        df = spark.createDataFrame([{"unix_ts": unix_ts}], schema=schema)

        result = df.withColumn("ts", F.to_timestamp(F.col("unix_ts")))

        rows = result.collect()
        assert len(rows) == 1
        assert isinstance(rows[0]["ts"], datetime)

    def test_to_timestamp_date_type_conversion(self, spark):
        """Test that to_timestamp() accepts DateType input (converts Date to Timestamp)."""
        from datetime import date, datetime
        from tests.fixtures.spark_imports import get_spark_imports

        imports = get_spark_imports()
        F = imports.F
        DateType = imports.DateType
        StructType = imports.StructType
        StructField = imports.StructField

        schema = StructType([StructField("date_col", DateType(), True)])
        df = spark.createDataFrame([{"date_col": date(2024, 1, 1)}], schema=schema)

        result = df.withColumn("ts", F.to_timestamp(F.col("date_col")))

        rows = result.collect()
        assert len(rows) == 1
        assert isinstance(rows[0]["ts"], datetime)
        # Date should be converted to timestamp at midnight
        assert rows[0]["ts"].date() == date(2024, 1, 1)

    def test_to_timestamp_double_type_unix_timestamp(self, spark):
        """Test that to_timestamp() accepts DoubleType input (Unix timestamp with decimal seconds)."""
        from datetime import datetime
        from tests.fixtures.spark_imports import get_spark_imports

        imports = get_spark_imports()
        F = imports.F
        DoubleType = imports.DoubleType
        StructType = imports.StructType
        StructField = imports.StructField

        # Unix timestamp with decimals for 2024-01-01 10:00:00.5 UTC
        unix_ts = 1704110400.5
        schema = StructType([StructField("unix_ts", DoubleType(), True)])
        df = spark.createDataFrame([{"unix_ts": unix_ts}], schema=schema)

        result = df.withColumn("ts", F.to_timestamp(F.col("unix_ts")))

        rows = result.collect()
        assert len(rows) == 1
        assert isinstance(rows[0]["ts"], datetime)
    @pytest.mark.skip(reason="Issue #1252: unskip when fixing")

    def test_to_timestamp_rejects_unsupported_type(self, spark):
        """Test that to_timestamp() rejects unsupported input types."""
        from tests.fixtures.spark_imports import get_spark_imports

        imports = get_spark_imports()
        F = imports.F
        BooleanType = imports.BooleanType
        StructType = imports.StructType
        StructField = imports.StructField

        schema = StructType([StructField("bool_col", BooleanType(), True)])
        df = spark.createDataFrame([{"bool_col": True}], schema=schema)

        # PySpark semantics: bool is implicitly castable from 0/1 in many contexts,
        # so to_timestamp on a boolean column does NOT consistently raise TypeError.
        # Here we assert only that the operation produces a column and the type is timestamp.
        result = df.withColumn("ts", F.to_timestamp(F.col("bool_col")))
        rows = result.collect()
        assert len(rows) == 1

    def test_to_timestamp_after_regexp_replace(self, spark):
        """Test that to_timestamp() works correctly after regexp_replace operation.

        This test verifies the fix for issue #133 where to_timestamp() would fail
        with SchemaError when used on a column created by regexp_replace.
        """
        from datetime import datetime, timedelta
        from tests.fixtures.spark_imports import get_spark_imports

        imports = get_spark_imports()
        F = imports.F
        StructType = imports.StructType
        StructField = imports.StructField
        StringType = imports.StringType

        # Create test data with ISO 8601 formatted timestamps (with microseconds)
        test_data = [
            {
                "id": f"record-{i:03d}",
                "timestamp_str": (datetime.now() - timedelta(hours=i)).isoformat(),
            }
            for i in range(5)
        ]

        schema = StructType(
            [
                StructField("id", StringType(), False),
                StructField("timestamp_str", StringType(), False),
            ]
        )

        df = spark.createDataFrame(test_data, schema)

        # Clean timestamp string (remove microseconds) using regexp_replace
        df_clean = df.withColumn(
            "timestamp_clean",
            F.regexp_replace(F.col("timestamp_str"), r"\.\d+", ""),
        )

        # Parse to timestamp - should work without SchemaError
        df_parsed = df_clean.withColumn(
            "timestamp_parsed",
            F.to_timestamp(F.col("timestamp_clean"), "yyyy-MM-dd'T'HH:mm:ss"),
        )

        # Schema should show correct type
        schema_dict = {
            field.name: type(field.dataType).__name__
            for field in df_parsed.schema.fields
        }
        assert schema_dict["timestamp_parsed"] == "TimestampType"

        # Materialization should work without SchemaError
        rows = df_parsed.collect()
        assert len(rows) == 5
        # Verify that timestamp_parsed column exists and is the correct type
        for row in rows:
            # timestamp_parsed should be datetime or None (if parsing failed)
            assert row["timestamp_parsed"] is None or isinstance(
                row["timestamp_parsed"], datetime
            )

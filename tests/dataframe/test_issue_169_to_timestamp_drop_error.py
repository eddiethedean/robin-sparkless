"""
Test for issue #169: to_timestamp() + drop + materialize.

Uses get_spark_imports from fixture only.
"""

from datetime import datetime, timedelta

from tests.fixtures.spark_imports import get_spark_imports

_imports = get_spark_imports()
SparkSession = _imports.SparkSession
F = _imports.F


class TestIssue169ToTimestampDropError:
    """Test fix for issue #169: to_timestamp() + drop() + materialize() error."""

    def test_to_timestamp_drop_materialize_basic(self):
        """Test the basic reproduction case from issue #169."""
        spark = SparkSession.builder.appName("test_issue_169").getOrCreate()
        try:
            # Create test data with timestamp strings
            data = []
            for i in range(150):
                data.append(
                    {
                        "lab_id": f"LAB-{i:08d}",
                        "test_date": (
                            datetime.now() - timedelta(days=i % 365)
                        ).isoformat(),
                    }
                )

            bronze_df = spark.createDataFrame(data, ["lab_id", "test_date"])

            # Transform with to_timestamp() - this is the exact scenario from issue #169
            silver_df = (
                bronze_df.withColumn(
                    "test_date_clean",
                    F.regexp_replace(F.col("test_date"), r"\.\d+", ""),
                )
                .withColumn(
                    "test_date_parsed",
                    F.to_timestamp(F.col("test_date_clean"), "yyyy-MM-dd'T'HH:mm:ss"),
                )
                .drop("test_date_clean")
                .select("lab_id", "test_date_parsed")
            )

            # Materialize (THIS WAS FAILING BEFORE THE FIX)
            count = silver_df.count()
            assert count == 150

            # Verify the data is correct
            rows = silver_df.collect()
            assert len(rows) == 150
            for row in rows:
                assert "lab_id" in row
                assert "test_date_parsed" in row
                assert isinstance(row["test_date_parsed"], datetime)
        finally:
            spark.stop()

    def test_to_timestamp_drop_multiple_columns(self):
        """Test to_timestamp() followed by dropping multiple columns."""
        spark = SparkSession.builder.appName("test_issue_169_multiple_drops").getOrCreate()
        try:
            data = [
                {
                    "id": 1,
                    "timestamp_str": "2024-01-01T10:00:00",
                    "extra_col": "test",
                },
                {
                    "id": 2,
                    "timestamp_str": "2024-01-02T11:00:00",
                    "extra_col": "test2",
                },
            ]

            df = spark.createDataFrame(data)

            result = (
                df.withColumn(
                    "ts_clean",
                    F.regexp_replace(F.col("timestamp_str"), r"\.\d+", ""),
                )
                .withColumn(
                    "timestamp",
                    F.to_timestamp(F.col("ts_clean"), "yyyy-MM-dd'T'HH:mm:ss"),
                )
                .drop("ts_clean", "extra_col")
                .select("id", "timestamp")
            )

            # Materialize - should work now
            count = result.count()
            assert count == 2

            rows = result.collect()
            assert len(rows) == 2
            for row in rows:
                assert isinstance(row["timestamp"], datetime)
        finally:
            spark.stop()

    def test_to_timestamp_drop_with_select(self):
        """Test to_timestamp() + drop() + select() chain."""
        spark = SparkSession.builder.appName("test_issue_169_select").getOrCreate()
        try:
            data = [
                {"id": 1, "ts_str": "2024-01-01T10:00:00"},
                {"id": 2, "ts_str": "2024-01-02T11:00:00"},
            ]

            df = spark.createDataFrame(data)

            result = (
                df.withColumn(
                    "ts_clean", F.regexp_replace(F.col("ts_str"), r"\.\d+", "")
                )
                .withColumn(
                    "ts", F.to_timestamp(F.col("ts_clean"), "yyyy-MM-dd'T'HH:mm:ss")
                )
                .drop("ts_clean")
                .select("id", "ts")
            )

            # Materialize - should work
            rows = result.collect()
            assert len(rows) == 2
        finally:
            spark.stop()

    def test_to_timestamp_drop_with_filter(self):
        """Test to_timestamp() + drop() + filter() chain."""
        spark = SparkSession.builder.appName("test_issue_169_filter").getOrCreate()
        try:
            data = [
                {"id": 1, "ts_str": "2024-01-01T10:00:00"},
                {"id": 2, "ts_str": "2024-01-02T11:00:00"},
            ]

            df = spark.createDataFrame(data)

            result = (
                df.withColumn(
                    "ts_clean", F.regexp_replace(F.col("ts_str"), r"\.\d+", "")
                )
                .withColumn(
                    "ts", F.to_timestamp(F.col("ts_clean"), "yyyy-MM-dd'T'HH:mm:ss")
                )
                .drop("ts_clean")
                .filter(F.col("id") > 1)
            )

            # Materialize - should work
            count = result.count()
            assert count == 1
        finally:
            spark.stop()

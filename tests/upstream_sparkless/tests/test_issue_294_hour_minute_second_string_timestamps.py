"""Test issue #294: hour/minute/second functions with string timestamps.

This test verifies that F.hour(), F.minute(), and F.second() correctly
extract time components from string columns containing timestamp values.
"""

from sparkless.sql import SparkSession
import sparkless.sql.functions as F


class TestIssue294HourMinuteSecondStringTimestamps:
    """Test hour/minute/second functions with string timestamps."""

    def _get_unique_app_name(self, test_name: str) -> str:
        """Generate unique app name for parallel test execution."""
        import os
        import threading

        thread_id = threading.current_thread().ident
        process_id = os.getpid()
        return f"{test_name}_{process_id}_{thread_id}"

    def test_hour_minute_second_from_string_timestamps(self):
        """Test hour/minute/second extraction from string timestamps (issue example)."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            # Create dataframe with timestamp strings (exact format from issue)
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Timestamp": "2023-02-07T04:00:01.730+0000"},
                    {"Name": "Bob", "Timestamp": "2023-02-07T12:30:32.730+0000"},
                    {"Name": "Charlie", "Timestamp": "2023-02-07T23:59:59.730+0000"},
                ]
            )

            # Extract the time components from the timestamp
            df = (
                df.withColumn("hour", F.hour(F.col("Timestamp")))
                .withColumn("minute", F.minute(F.col("Timestamp")))
                .withColumn("second", F.second(F.col("Timestamp")))
            )

            rows = df.collect()
            assert len(rows) == 3

            # Find rows by Name to avoid order dependency
            row_alice = [r for r in rows if r["Name"] == "Alice"][0]
            row_bob = [r for r in rows if r["Name"] == "Bob"][0]
            row_charlie = [r for r in rows if r["Name"] == "Charlie"][0]

            # Verify Alice: 2023-02-07T04:00:01.730+0000 -> hour=4, minute=0, second=1
            assert row_alice["hour"] == 4, f"Expected hour=4, got {row_alice['hour']}"
            assert row_alice["minute"] == 0, (
                f"Expected minute=0, got {row_alice['minute']}"
            )
            assert row_alice["second"] == 1, (
                f"Expected second=1, got {row_alice['second']}"
            )

            # Verify Bob: 2023-02-07T12:30:32.730+0000 -> hour=12, minute=30, second=32
            assert row_bob["hour"] == 12, f"Expected hour=12, got {row_bob['hour']}"
            assert row_bob["minute"] == 30, (
                f"Expected minute=30, got {row_bob['minute']}"
            )
            assert row_bob["second"] == 32, (
                f"Expected second=32, got {row_bob['second']}"
            )

            # Verify Charlie: 2023-02-07T23:59:59.730+0000 -> hour=23, minute=59, second=59
            assert row_charlie["hour"] == 23, (
                f"Expected hour=23, got {row_charlie['hour']}"
            )
            assert row_charlie["minute"] == 59, (
                f"Expected minute=59, got {row_charlie['minute']}"
            )
            assert row_charlie["second"] == 59, (
                f"Expected second=59, got {row_charlie['second']}"
            )
        finally:
            spark.stop()

    def test_hour_minute_second_with_different_timezone_formats(self):
        """Test hour/minute/second with various timezone formats."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            # Test various timezone formats
            df = spark.createDataFrame(
                [
                    {"Name": "UTC", "Timestamp": "2023-02-07T04:00:01.730+0000"},
                    {"Name": "EST", "Timestamp": "2023-02-07T04:00:01.730-0500"},
                    {"Name": "PST", "Timestamp": "2023-02-07T04:00:01.730-0800"},
                    {"Name": "NoTZ", "Timestamp": "2023-02-07T04:00:01.730"},
                    {"Name": "ZFormat", "Timestamp": "2023-02-07T04:00:01.730Z"},
                ]
            )

            df = (
                df.withColumn("hour", F.hour(F.col("Timestamp")))
                .withColumn("minute", F.minute(F.col("Timestamp")))
                .withColumn("second", F.second(F.col("Timestamp")))
            )

            rows = df.collect()
            assert len(rows) == 5

            # All should have hour=4, minute=0, second=1 (timezone doesn't affect extraction)
            for row in rows:
                assert row["hour"] == 4, (
                    f"Expected hour=4 for {row['Name']}, got {row['hour']}"
                )
                assert row["minute"] == 0, (
                    f"Expected minute=0 for {row['Name']}, got {row['minute']}"
                )
                assert row["second"] == 1, (
                    f"Expected second=1 for {row['Name']}, got {row['second']}"
                )
        finally:
            spark.stop()

    def test_hour_minute_second_with_different_formats(self):
        """Test hour/minute/second with different timestamp string formats."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "ISO", "Timestamp": "2023-02-07T04:00:01.730+0000"},
                    {"Name": "Space", "Timestamp": "2023-02-07 04:00:01.730"},
                    {"Name": "NoMicro", "Timestamp": "2023-02-07T04:00:01+0000"},
                    {"Name": "DateOnly", "Timestamp": "2023-02-07"},
                ]
            )

            df = (
                df.withColumn("hour", F.hour(F.col("Timestamp")))
                .withColumn("minute", F.minute(F.col("Timestamp")))
                .withColumn("second", F.second(F.col("Timestamp")))
            )

            rows = df.collect()
            assert len(rows) == 4

            # Find rows by Name
            row_iso = [r for r in rows if r["Name"] == "ISO"][0]
            row_space = [r for r in rows if r["Name"] == "Space"][0]
            row_nomicro = [r for r in rows if r["Name"] == "NoMicro"][0]
            row_dateonly = [r for r in rows if r["Name"] == "DateOnly"][0]

            # ISO format: hour=4, minute=0, second=1
            assert row_iso["hour"] == 4
            assert row_iso["minute"] == 0
            assert row_iso["second"] == 1

            # Space format: hour=4, minute=0, second=1
            assert row_space["hour"] == 4
            assert row_space["minute"] == 0
            assert row_space["second"] == 1

            # No microseconds: hour=4, minute=0, second=1
            assert row_nomicro["hour"] == 4
            assert row_nomicro["minute"] == 0
            assert row_nomicro["second"] == 1

            # Date only: hour=0, minute=0, second=0 (defaults for date)
            assert row_dateonly["hour"] == 0
            assert row_dateonly["minute"] == 0
            assert row_dateonly["second"] == 0
        finally:
            spark.stop()

    def test_hour_minute_second_in_select(self):
        """Test hour/minute/second in select context."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Timestamp": "2023-02-07T04:00:01.730+0000"},
                    {"Name": "Bob", "Timestamp": "2023-02-07T12:30:32.730+0000"},
                ]
            )

            df = df.select(
                "Name",
                "Timestamp",
                F.hour("Timestamp").alias("hour"),
                F.minute("Timestamp").alias("minute"),
                F.second("Timestamp").alias("second"),
            )

            rows = df.collect()
            assert len(rows) == 2

            row_alice = [r for r in rows if r["Name"] == "Alice"][0]
            row_bob = [r for r in rows if r["Name"] == "Bob"][0]

            assert row_alice["hour"] == 4
            assert row_alice["minute"] == 0
            assert row_alice["second"] == 1

            assert row_bob["hour"] == 12
            assert row_bob["minute"] == 30
            assert row_bob["second"] == 32
        finally:
            spark.stop()

    def test_hour_minute_second_with_filter(self):
        """Test hour/minute/second with filter operations."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Timestamp": "2023-02-07T04:00:01.730+0000"},
                    {"Name": "Bob", "Timestamp": "2023-02-07T12:30:32.730+0000"},
                    {"Name": "Charlie", "Timestamp": "2023-02-07T23:59:59.730+0000"},
                ]
            )

            df = (
                df.withColumn("hour", F.hour("Timestamp"))
                .withColumn("minute", F.minute("Timestamp"))
                .withColumn("second", F.second("Timestamp"))
                .filter(F.col("hour") >= 12)
            )

            rows = df.collect()
            assert len(rows) == 2

            # Should only have Bob (hour=12) and Charlie (hour=23)
            names = {r["Name"] for r in rows}
            assert "Bob" in names
            assert "Charlie" in names
            assert "Alice" not in names
        finally:
            spark.stop()

    def test_hour_minute_second_with_null_values(self):
        """Test hour/minute/second with null timestamp values."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Timestamp": "2023-02-07T04:00:01.730+0000"},
                    {"Name": "Bob", "Timestamp": None},
                    {"Name": "Charlie", "Timestamp": "2023-02-07T23:59:59.730+0000"},
                ]
            )

            df = (
                df.withColumn("hour", F.hour("Timestamp"))
                .withColumn("minute", F.minute("Timestamp"))
                .withColumn("second", F.second("Timestamp"))
            )

            rows = df.collect()
            assert len(rows) == 3

            row_alice = [r for r in rows if r["Name"] == "Alice"][0]
            row_bob = [r for r in rows if r["Name"] == "Bob"][0]
            row_charlie = [r for r in rows if r["Name"] == "Charlie"][0]

            assert row_alice["hour"] == 4
            assert row_alice["minute"] == 0
            assert row_alice["second"] == 1

            # Null timestamp should result in None for all parts
            assert row_bob["hour"] is None
            assert row_bob["minute"] is None
            assert row_bob["second"] is None

            assert row_charlie["hour"] == 23
            assert row_charlie["minute"] == 59
            assert row_charlie["second"] == 59
        finally:
            spark.stop()

    def test_hour_minute_second_in_groupby_agg(self):
        """Test hour/minute/second in groupBy aggregation context."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Category": "A", "Timestamp": "2023-02-07T04:00:01.730+0000"},
                    {"Category": "A", "Timestamp": "2023-02-07T05:00:01.730+0000"},
                    {"Category": "B", "Timestamp": "2023-02-07T12:30:32.730+0000"},
                    {"Category": "B", "Timestamp": "2023-02-07T13:30:32.730+0000"},
                ]
            )

            df = (
                df.withColumn("hour", F.hour("Timestamp"))
                .groupBy("Category")
                .agg(F.avg("hour").alias("avg_hour"))
            )

            rows = df.collect()
            assert len(rows) == 2

            # Find rows by Category
            row_a = [r for r in rows if r["Category"] == "A"][0]
            row_b = [r for r in rows if r["Category"] == "B"][0]

            # Category A: avg of 4 and 5 = 4.5
            assert row_a["avg_hour"] == 4.5

            # Category B: avg of 12 and 13 = 12.5
            assert row_b["avg_hour"] == 12.5
        finally:
            spark.stop()

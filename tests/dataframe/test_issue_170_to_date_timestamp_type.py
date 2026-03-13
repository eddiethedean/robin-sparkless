class TestIssue170ToDateTimestampType:
    """Test fix for issue #170: to_date() on TimestampType + materialize() error."""

    def test_to_date_on_timestamp_type_basic(self, spark):
        """Test the basic reproduction case from issue #170."""
        from datetime import datetime
        from sparkless.testing import get_imports

        imports = get_imports()
        F = imports.F

        # Create test data
        data = []
        for i in range(100):
            data.append(
                {
                    "event_id": f"EVT-{i:08d}",
                    "event_timestamp": datetime.now().isoformat(),
                }
            )

        bronze_df = spark.createDataFrame(data, ["event_id", "event_timestamp"])

        # PySpark 3.x requires a policy for parsing ISO8601 strings with microseconds.
        # Align with the new parser semantics by treating unparseable strings as invalid.
        spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")

        # Parse timestamp (creates TimestampType column)
        silver_df = bronze_df.withColumn(
            "event_timestamp_parsed",
            F.to_timestamp(
                F.col("event_timestamp").cast("string"), "yyyy-MM-dd'T'HH:mm:ss"
            ),
        ).select("event_id", "event_timestamp_parsed")

        # Apply to_date() on TimestampType (THIS WAS FAILING BEFORE THE FIX)
        gold_df = (
            silver_df.withColumn(
                "metric_date",
                F.to_date(F.col("event_timestamp_parsed")),  # Should work now
            )
            .groupBy("metric_date")
            .agg(F.count("*").alias("total_events"))
        )

        # Materialize (THIS WAS FAILING BEFORE THE FIX)
        result = gold_df.collect()
        assert len(result) > 0

        # Verify the data is correct
        for row in result:
            assert "metric_date" in row
            assert "total_events" in row
            assert row["total_events"] > 0

    def test_to_date_on_timestamp_type_with_drop(self, spark):
        """Test to_date() on TimestampType followed by drop()."""
        from sparkless.testing import get_imports

        imports = get_imports()
        F = imports.F

        data = [
            {"id": 1, "ts_str": "2024-01-01T10:00:00"},
            {"id": 2, "ts_str": "2024-01-02T11:00:00"},
        ]

        df = spark.createDataFrame(data)

        result = (
            df.withColumn(
                "ts",
                F.to_timestamp(F.col("ts_str").cast("string"), "yyyy-MM-dd'T'HH:mm:ss"),
            )
            .withColumn("date", F.to_date(F.col("ts")))
            .drop("ts_str")
            .select("id", "date")
        )

        # Materialize - should work now
        rows = result.collect()
        assert len(rows) == 2
        for row in rows:
            assert "date" in row

    def test_to_date_on_timestamp_type_with_select(self, spark):
        """Test to_date() on TimestampType + select() chain."""
        from sparkless.testing import get_imports

        imports = get_imports()
        F = imports.F

        data = [
            {"id": 1, "ts_str": "2024-01-01T10:00:00"},
            {"id": 2, "ts_str": "2024-01-02T11:00:00"},
        ]

        df = spark.createDataFrame(data)

        result = (
            df.withColumn(
                "ts",
                F.to_timestamp(F.col("ts_str").cast("string"), "yyyy-MM-dd'T'HH:mm:ss"),
            )
            .withColumn("date", F.to_date(F.col("ts")))
            .select("id", "date")
        )

        # Materialize - should work
        rows = result.collect()
        assert len(rows) == 2

    def test_to_date_on_timestamp_type_with_filter(self, spark):
        """Test to_date() on TimestampType + filter() chain."""
        from sparkless.testing import get_imports

        imports = get_imports()
        F = imports.F

        data = [
            {"id": 1, "ts_str": "2024-01-01T10:00:00"},
            {"id": 2, "ts_str": "2024-01-02T11:00:00"},
        ]

        df = spark.createDataFrame(data)

        result = (
            df.withColumn(
                "ts",
                F.to_timestamp(F.col("ts_str").cast("string"), "yyyy-MM-dd'T'HH:mm:ss"),
            )
            .withColumn("date", F.to_date(F.col("ts")))
            .filter(F.col("id") > 1)
        )

        # Materialize - should work
        count = result.count()
        assert count == 1

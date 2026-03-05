"""
Test for issue #153: to_timestamp() returns None for all values in 3.18.2

Issue #153 reports that F.to_timestamp() returns TimestampType in the schema
(appearing correct), but all actual values are None. This causes validation
to fail silently (0% valid, 0 rows processed) because all rows are None.

This test verifies that to_timestamp() actually parses string dates into
timestamp values, not just returns None.
"""

from tests.fixtures.spark_imports import get_spark_imports

imports = get_spark_imports()
F = imports.F


class TestIssue153ToTimestampReturnsNone:
    """Test cases for issue #153: to_timestamp() returning None values."""

    def test_to_timestamp_returns_actual_values(self, spark):
        """Test that to_timestamp() returns actual datetime values, not None.

        This test verifies the fix for issue #153 where to_timestamp() was
        returning None for all values even though the schema showed TimestampType.
        """
        data = [
            ("imp_001", "2024-01-15T10:30:45.123456"),
            ("imp_002", "2024-01-16T14:20:30.789012"),
            ("imp_003", "2024-01-17T09:15:22.456789"),
        ]
        df = spark.createDataFrame(data, ["id", "date_string"])

        df_transformed = df.withColumn(
            "date_parsed",
            F.to_timestamp(
                F.regexp_replace(F.col("date_string"), r"\.\d+", "").cast("string"),
                "yyyy-MM-dd'T'HH:mm:ss",
            ),
        )

        # Verify schema shows TimestampType
        schema = df_transformed.schema
        date_parsed_field = next(f for f in schema.fields if f.name == "date_parsed")
        assert date_parsed_field.dataType.__class__.__name__ == "TimestampType"

        # Verify actual values are datetime objects, not None
        rows = df_transformed.select("date_parsed").collect()
        assert len(rows) == 3

        for i, row in enumerate(rows):
            assert row["date_parsed"] is not None, f"Row {i + 1} should not be None"
            assert isinstance(row["date_parsed"], type(rows[0]["date_parsed"])), (
                f"Row {i + 1} should be datetime object"
            )

        # Verify validation works (isNotNull filter should return all rows)
        valid_rows = df_transformed.filter(F.col("date_parsed").isNotNull()).count()
        assert valid_rows == 3, f"Expected 3 valid rows, got {valid_rows}"

    def test_to_timestamp_with_clean_string(self, spark):
        """Test that to_timestamp() works with already-clean strings."""
        data = [
            ("imp_001", "2024-01-15T10:30:45"),
            ("imp_002", "2024-01-16T14:20:30"),
        ]
        df = spark.createDataFrame(data, ["id", "date_string"])

        df_transformed = df.withColumn(
            "date_parsed",
            F.to_timestamp(F.col("date_string"), "yyyy-MM-dd'T'HH:mm:ss"),
        )

        rows = df_transformed.select("date_parsed").collect()
        assert len(rows) == 2

        for row in rows:
            assert row["date_parsed"] is not None
            # Verify it's a datetime object
            assert hasattr(row["date_parsed"], "year")
            assert hasattr(row["date_parsed"], "month")
            assert hasattr(row["date_parsed"], "day")

        valid_rows = df_transformed.filter(F.col("date_parsed").isNotNull()).count()
        assert valid_rows == 2

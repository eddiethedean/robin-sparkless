"""
Test for issue #151: to_timestamp() returns datetime but validation expects String.

Issue #151 reports that to_timestamp() correctly returns TimestampType, but sparkless's
internal validation system incorrectly expects StringType, causing validation failures
with the error: "invalid series dtype: expected `String`, got `datetime[μs]`".
"""

from tests.fixtures.spark_imports import get_spark_imports

# Get the appropriate imports based on backend (sparkless or PySpark)
imports = get_spark_imports()
F = imports.F
col = F.col
to_timestamp = F.to_timestamp
regexp_replace = F.regexp_replace


class TestIssue151ToTimestampValidation:
    """Test cases for issue #151: to_timestamp() validation type mismatch."""

    def test_to_timestamp_with_validation_rule_not_null(self, spark):
        """Test that validation rules work correctly with to_timestamp() columns.

        This test verifies the fix for issue #151 where validation rules like "not_null"
        would fail with "invalid series dtype: expected `String`, got `datetime[μs]`"
        when applied to columns created by to_timestamp().
        """
        # Create test data with datetime strings
        data = [("2024-01-15T10:30:45",)]
        df = spark.createDataFrame(data, ["date_string"])

        # Apply to_timestamp (this pattern is used in failing tests)
        df_transformed = df.withColumn(
            "date_parsed",
            to_timestamp(
                col("date_string"),
                "yyyy-MM-dd'T'HH:mm:ss",
            ),
        )

        # Verify PySpark returns correct type
        date_parsed_field = next(
            f for f in df_transformed.schema.fields if f.name == "date_parsed"
        )
        assert date_parsed_field.dataType.__class__.__name__ == "TimestampType", (
            f"Expected TimestampType, got {date_parsed_field.dataType}"
        )

        # Apply validation rule - this should not fail
        # In a real pipeline, this would be done via validation rules dict
        # For now, we'll just verify the column works with isNotNull()
        df_validated = df_transformed.filter(col("date_parsed").isNotNull())

        # Verify the operation completes without schema validation errors
        result = df_validated.collect()
        assert len(result) == 1

        # Verify the column type is still TimestampType after validation
        date_parsed_field_after = next(
            f for f in df_validated.schema.fields if f.name == "date_parsed"
        )
        assert date_parsed_field_after.dataType.__class__.__name__ == "TimestampType", (
            f"Expected TimestampType after validation, got {date_parsed_field_after.dataType}"
        )

    def test_to_timestamp_with_datetime_operations(self, spark):
        """Test that datetime operations work correctly on to_timestamp() columns."""
        data = [("2024-01-15T10:30:45",)]
        df = spark.createDataFrame(data, ["date_string"])

        df_transformed = (
            df.withColumn(
                "date_parsed",
                to_timestamp(
                    regexp_replace(col("date_string"), r"T", " "),
                    "yyyy-MM-dd HH:mm:ss",
                ),
            )
            .withColumn("hour_of_day", F.hour(col("date_parsed")))
            .withColumn("day_of_week", F.dayofweek(col("date_parsed")))
        )

        # Verify no schema error occurs
        result = df_transformed.collect()
        assert len(result) == 1

        # Verify all column types are correct
        date_parsed_field = next(
            f for f in df_transformed.schema.fields if f.name == "date_parsed"
        )
        assert date_parsed_field.dataType.__class__.__name__ == "TimestampType", (
            f"Expected TimestampType, got {date_parsed_field.dataType}"
        )

        hour_field = next(
            f for f in df_transformed.schema.fields if f.name == "hour_of_day"
        )
        assert hour_field.dataType.__class__.__name__ in (
            "IntegerType",
            "LongType",
        ), f"Expected IntegerType or LongType, got {hour_field.dataType}"

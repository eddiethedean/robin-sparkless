"""
PySpark parity tests for DataFrame filter operations.

Tests validate that Sparkless filter operations behave identically to PySpark.
"""

from tests.fixtures.parity_base import ParityTestBase
from tests.fixtures.spark_imports import get_spark_imports


class TestFilterParity(ParityTestBase):
    """Test DataFrame filter operations parity with PySpark."""

    def test_filter_operations(self, spark):
        """Test filter matches PySpark behavior."""
        expected = self.load_expected("dataframe_operations", "filter_operations")

        df = spark.createDataFrame(expected["input_data"])
        result = df.filter(df.age > 30)

        self.assert_parity(result, expected)

    def test_filter_with_boolean(self, spark):
        """Test filter with boolean matches PySpark behavior."""
        expected = self.load_expected("dataframe_operations", "filter_with_boolean")

        df = spark.createDataFrame(expected["input_data"])
        result = df.filter(df.salary > 60000)

        self.assert_parity(result, expected)

    def test_filter_with_and_operator(self, spark):
        """Test filter with combined expressions using & (AND) operator.

        Tests fix for issue #108: Combined ColumnOperation expressions with & operator
        should be properly translated, not treated as column names.
        """
        imports = get_spark_imports()
        F = imports.F

        df = spark.createDataFrame(
            [{"a": 1, "b": 2}, {"a": 2, "b": 3}, {"a": 3, "b": 1}], "a int, b int"
        )

        expr1 = F.col("a") > 1
        expr2 = F.col("b") > 1
        combined = expr1 & expr2

        # Should not raise ColumnNotFoundError
        result = df.filter(combined)
        assert result.count() == 1, "Should return 1 row where both conditions are true"
        rows = result.collect()
        assert rows[0]["a"] == 2
        assert rows[0]["b"] == 3

    def test_filter_with_or_operator(self, spark):
        """Test filter with combined expressions using | (OR) operator.

        Tests fix for issue #109: Combined ColumnOperation expressions with | operator
        should be properly translated, not treated as column names.
        """
        imports = get_spark_imports()
        F = imports.F

        df = spark.createDataFrame(
            [{"a": 1, "b": 2}, {"a": 2, "b": 3}, {"a": 3, "b": 1}], "a int, b int"
        )

        expr1 = F.col("a") > 1
        expr2 = F.col("b") > 1
        combined = expr1 | expr2

        # Should not raise ColumnNotFoundError
        result = df.filter(combined)
        assert result.count() == 3, (
            "Should return 3 rows where at least one condition is true"
        )

    def test_filter_on_table_with_complex_schema(self, spark):
        """Test filter on DataFrames read from tables with complex schemas.

        Tests fix for issue #122: DataFrame.filter() returns 0 rows on tables
        created with saveAsTable() using complex schemas (29+ fields).

        The bug was that ColumnOperation is a subclass of Column, so isinstance()
        checks for Column before ColumnOperation incorrectly handled comparison
        operations as simple column existence checks.
        """
        from datetime import datetime

        imports = get_spark_imports()
        F = imports.F
        StructType = imports.StructType
        StructField = imports.StructField
        StringType = imports.StringType
        TimestampType = imports.TimestampType
        FloatType = imports.FloatType
        IntegerType = imports.IntegerType

        # Create schema
        schema_name = "test_schema"
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

        # Create DataFrame with complex schema (29 fields)
        # Use backend-specific types (PySpark or Sparkless) so createDataFrame accepts the schema
        complex_schema = StructType(
            [
                StructField("run_id", StringType(), False),
                StructField("run_mode", StringType(), False),
                StructField("run_started_at", TimestampType(), True),
                StructField("run_ended_at", TimestampType(), True),
                StructField("execution_id", StringType(), False),
                StructField("pipeline_id", StringType(), False),
                StructField("schema", StringType(), False),
                StructField("phase", StringType(), False),
                StructField("step_name", StringType(), False),
                StructField("step_type", StringType(), False),
                StructField("start_time", TimestampType(), True),
                StructField("end_time", TimestampType(), True),
                StructField("duration_secs", FloatType(), False),
                StructField("table_fqn", StringType(), True),
                StructField("write_mode", StringType(), True),
                StructField("input_rows", IntegerType(), True),
                StructField("output_rows", IntegerType(), True),
                StructField("rows_written", IntegerType(), True),
                StructField("rows_processed", IntegerType(), True),
                StructField("table_total_rows", IntegerType(), True),
                StructField("valid_rows", IntegerType(), True),
                StructField("invalid_rows", IntegerType(), True),
                StructField("validation_rate", FloatType(), True),
                StructField("success", StringType(), False),
                StructField("error_message", StringType(), True),
                StructField("memory_usage_mb", FloatType(), True),
                StructField("cpu_usage_percent", FloatType(), True),
                StructField("metadata", StringType(), True),
                StructField("created_at", StringType(), True),
            ]
        )

        current_time = datetime.now()
        data = [
            (
                "initial_run_1",
                "initial",
                current_time,
                current_time,
                "exec_1",
                "pipeline_1",
                schema_name,
                "bronze",
                "step_1",
                "transform",
                current_time,
                current_time,
                30.0,
                None,
                None,
                100,
                100,
                100,
                100,
                None,
                100,
                0,
                100.0,
                "true",
                None,
                None,
                None,
                "{}",
                current_time.isoformat(),
            ),
            (
                "initial_run_1",
                "initial",
                current_time,
                current_time,
                "exec_2",
                "pipeline_1",
                schema_name,
                "silver",
                "step_2",
                "transform",
                current_time,
                current_time,
                25.0,
                None,
                None,
                100,
                100,
                100,
                100,
                None,
                100,
                0,
                100.0,
                "true",
                None,
                None,
                None,
                "{}",
                current_time.isoformat(),
            ),
            (
                "other_run",
                "initial",
                current_time,
                current_time,
                "exec_3",
                "pipeline_1",
                schema_name,
                "gold",
                "step_3",
                "transform",
                current_time,
                current_time,
                20.0,
                None,
                None,
                100,
                100,
                100,
                100,
                None,
                100,
                0,
                100.0,
                "true",
                None,
                None,
                None,
                "{}",
                current_time.isoformat(),
            ),
        ]

        df = spark.createDataFrame(data, complex_schema)
        assert df.count() == 3, "Original DataFrame should have 3 rows"

        # Write to table (Delta when available, else Parquet for PySpark without Delta)
        table_fqn = f"{schema_name}.test_table"
        try:
            df.write.format("delta").mode("overwrite").option(
                "overwriteSchema", "true"
            ).saveAsTable(table_fqn)
        except Exception:
            df.write.format("parquet").mode("overwrite").saveAsTable(table_fqn)

        # Read back
        read_df = spark.table(table_fqn)
        assert read_df.count() == 3, "Table read should return 3 rows"

        # Try to filter - this was failing before the fix
        filtered = read_df.filter(read_df.run_id == "initial_run_1")
        assert filtered.count() == 2, (
            "Filter should return 2 rows with run_id='initial_run_1'"
        )

        # Also test with F.col()
        filtered2 = read_df.filter(F.col("run_id") == "initial_run_1")
        assert filtered2.count() == 2, "Filter with F.col() should return 2 rows"

        # Verify the filtered rows have the correct run_id
        filtered_rows = filtered.collect()
        for row in filtered_rows:
            assert row.run_id == "initial_run_1", (
                "All filtered rows should have run_id='initial_run_1'"
            )

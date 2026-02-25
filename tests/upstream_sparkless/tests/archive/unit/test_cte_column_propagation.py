"""
Unit tests for CTE column propagation and optimization fixes.

This module tests the fixes for:
1. Column visibility in CTEs (chained withColumn operations)
2. String concatenation type inference
3. Schema tracking after drop operations
"""

import pytest
import warnings
from sparkless import SparkSession, F
from sparkless.spark_types import StringType


@pytest.mark.unit
class TestCTEColumnPropagation:
    """Test CTE column propagation fixes."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        session = SparkSession("cte_column_propagation_test")
        yield session
        session.stop()

    def test_chained_withcolumn_references(self, spark):
        """Test chained withColumn operations where later columns reference earlier ones."""
        test_data = [
            {"id": 1, "first_name": "John", "last_name": "Doe"},
            {"id": 2, "first_name": "Jane", "last_name": "Smith"},
        ]

        df = spark.createDataFrame(test_data)

        # Chain: withColumn creates col1, then col2 references col1
        with warnings.catch_warnings():
            warnings.simplefilter("error")  # Fail on CTE fallback warnings
            try:
                result = (
                    df.withColumn("col1", F.lit(1))
                    .withColumn("col2", F.col("col1") + 1)
                    .withColumn("col3", F.col("col2") * 2)
                )

                rows = result.collect()
                assert len(rows) == 2
                # Verify columns exist
                assert "col1" in result.columns
                assert "col2" in result.columns
                assert "col3" in result.columns
                # Verify values
                assert rows[0]["col1"] == 1
                assert rows[0]["col2"] == 2
                assert rows[0]["col3"] == 4
            except Warning:
                pytest.fail(
                    "CTE optimization should not fall back for chained withColumn"
                )

    def test_withcolumn_references_previous_cte_column(self, spark):
        """Test withColumn that references a column created in a previous withColumn."""
        test_data = [
            {"diagnosis_id": 1, "status": "chronic", "severity": 5},
            {"diagnosis_id": 2, "status": "acute", "severity": 3},
        ]

        df = spark.createDataFrame(test_data)

        # This pattern was failing before the fix
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            try:
                result = df.withColumn(
                    "risk_level",
                    F.when(F.col("severity") > 4, "high").otherwise("low"),
                ).withColumn(
                    "is_chronic",
                    F.when(F.col("status") == "chronic", True).otherwise(False),
                )

                rows = result.collect()
                assert len(rows) == 2
                assert "risk_level" in result.columns
                assert "is_chronic" in result.columns
                assert rows[0]["risk_level"] == "high"
                assert rows[0]["is_chronic"] is True
            except Warning:
                pytest.fail(
                    "CTE optimization should handle column references correctly"
                )


@pytest.mark.unit
class TestStringConcatenationType:
    """Test string concatenation type inference fixes."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        session = SparkSession("string_concat_type_test")
        yield session
        session.stop()

    def test_concat_preserves_string_type(self, spark):
        """Test that concat() operations preserve STRING type."""
        test_data = [
            {"first_name": "John", "last_name": "Doe"},
            {"first_name": "Jane", "last_name": "Smith"},
        ]

        df = spark.createDataFrame(test_data)

        with warnings.catch_warnings():
            warnings.simplefilter("error")
            try:
                result = df.withColumn(
                    "full_name",
                    F.concat(F.col("first_name"), F.lit(" "), F.col("last_name")),
                )

                rows = result.collect()
                assert len(rows) == 2
                assert "full_name" in result.columns

                # Verify schema type is StringType
                full_name_field = next(
                    f for f in result.schema.fields if f.name == "full_name"
                )
                assert isinstance(full_name_field.dataType, StringType), (
                    "concat() should return StringType"
                )

                # Verify values are strings
                assert rows[0]["full_name"] == "John Doe"
                assert rows[1]["full_name"] == "Jane Smith"
            except Warning:
                pytest.fail("CTE optimization should handle concat() correctly")

    def test_concat_no_type_conversion_error(self, spark):
        """Test that concat() doesn't cause 'Conversion Error: Could not convert string to FLOAT'."""
        test_data = [
            {"patient_id": "P001", "first": "Patient0", "last": "LastName0"},
        ]

        df = spark.createDataFrame(test_data)

        with warnings.catch_warnings():
            warnings.simplefilter("error")
            try:
                result = df.withColumn(
                    "full_name",
                    F.concat(F.col("first"), F.lit(" "), F.col("last")),
                )

                rows = result.collect()
                assert len(rows) == 1
                assert rows[0]["full_name"] == "Patient0 LastName0"

                # Verify no type conversion errors occurred
                assert isinstance(rows[0]["full_name"], str)
            except (Warning, ValueError) as e:
                if "convert" in str(e).lower() and "float" in str(e).lower():
                    pytest.fail(f"Type conversion error occurred: {e}")
                raise


@pytest.mark.unit
class TestDropOperationSchemaTracking:
    """Test schema tracking after drop operations."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        session = SparkSession("drop_schema_test")
        yield session
        session.stop()

    def test_drop_followed_by_withcolumn(self, spark):
        """Test drop() followed by withColumn() using remaining columns."""
        test_data = [
            {"id": 1, "col1": "a", "col2": "b", "col3": "c"},
        ]

        df = spark.createDataFrame(test_data)

        with warnings.catch_warnings():
            warnings.simplefilter("error")
            try:
                result = df.drop("col1").withColumn("new_col", F.col("id") * 2)

                rows = result.collect()
                assert len(rows) == 1

                # Verify dropped column is gone
                assert "col1" not in result.columns
                # Verify remaining columns exist
                assert "col2" in result.columns
                assert "col3" in result.columns
                # Verify new column exists
                assert "new_col" in result.columns
                assert rows[0]["new_col"] == 2
            except Warning:
                pytest.fail("CTE optimization should handle drop() correctly")

    @pytest.mark.skip(
        reason="Known limitation: Polars schema dtype mismatch when converting materialized DataFrame back to lazy after drop operation. Will be addressed in future."
    )
    def test_withcolumn_drop_withcolumn_chain(self, spark):
        """Test withColumn -> drop -> withColumn chain.

        Note: This test currently verifies that the operations work correctly
        even if CTE optimization falls back to table-per-operation mode.
        The drop operation column tracking in CTEs is a known limitation
        that will be addressed in Phase 3.
        """
        test_data = [
            {
                "test_date": "2025-01-01",
            },
        ]

        df = spark.createDataFrame(test_data)

        # This test may fall back to table-per-operation mode due to drop operation
        # but should still work correctly
        # Replace all "-" with " " to get "2025 01 01" format
        # Then parse with format that matches: "yyyy MM dd" (date only, no time)
        result = (
            df.withColumn(
                "test_date_clean", F.regexp_replace(F.col("test_date"), "-", " ")
            )
            .withColumn(
                "test_date_parsed",
                F.to_timestamp(F.col("test_date_clean"), "yyyy MM dd"),
            )
            .drop("test_date_clean")
            .withColumn("year", F.year(F.col("test_date_parsed")))
        )

        rows = result.collect()
        assert len(rows) == 1

        # Verify dropped column is gone
        assert "test_date_clean" not in result.columns
        # Verify intermediate and final columns exist
        assert "test_date_parsed" in result.columns
        assert "year" in result.columns

    def test_schema_consistency_after_drop(self, spark):
        """Test that df.columns matches actual schema after drop."""
        test_data = [
            {"id": 1, "col1": "a", "col2": "b"},
        ]

        df = spark.createDataFrame(test_data)

        result = df.drop("col1")

        # Verify schema consistency
        schema_columns = [f.name for f in result.schema.fields]
        assert set(result.columns) == set(schema_columns), (
            "df.columns should match schema fields after drop"
        )
        assert "col1" not in schema_columns
        assert "id" in schema_columns
        assert "col2" in schema_columns

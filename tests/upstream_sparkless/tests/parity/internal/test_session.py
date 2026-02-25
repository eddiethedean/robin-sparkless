"""
PySpark parity tests for SparkSession operations.

Tests validate that Sparkless SparkSession operations behave identically to PySpark.
"""

from tests.fixtures.parity_base import ParityTestBase
from sparkless.spark_types import StructType, StructField, StringType, IntegerType


class TestSessionParity(ParityTestBase):
    """Test SparkSession operations parity with PySpark."""

    def test_createDataFrame_from_list_of_dicts(self, spark):
        """Test createDataFrame from list of dicts matches PySpark behavior.

        Note: This is a foundational operation. PySpark behavior is verified
        indirectly through all other tests that use createDataFrame.
        For direct parity testing, we verify the basic structure matches.
        """
        data = [
            {"name": "Alice", "age": 25},
            {"name": "Bob", "age": 30},
        ]
        df = spark.createDataFrame(data)

        # Verify structure matches PySpark expectations
        assert df.count() == 2
        assert "name" in df.columns
        assert "age" in df.columns
        rows = df.collect()
        assert rows[0].name == "Alice"
        assert rows[0].age == 25

    def test_createDataFrame_with_explicit_schema(self, spark):
        """Test createDataFrame with explicit schema matches PySpark behavior."""
        data = [
            {"name": "Alice", "age": 25},
            {"name": "Bob", "age": 30},
        ]
        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
            ]
        )
        df = spark.createDataFrame(data, schema)

        # Verify schema is applied correctly
        assert df.count() == 2
        assert df.schema.fieldNames() == ["name", "age"]
        assert df.schema.fields[0].dataType == StringType()
        assert df.schema.fields[1].dataType == IntegerType()

    def test_createDataFrame_empty(self, spark):
        """Test createDataFrame with empty data matches PySpark behavior."""
        df = spark.createDataFrame([], StructType([]))

        assert df.count() == 0
        assert len(df.columns) == 0

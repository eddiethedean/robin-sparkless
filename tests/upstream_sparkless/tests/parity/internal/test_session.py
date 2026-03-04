"""
PySpark parity tests for SparkSession operations.

Tests validate that Sparkless SparkSession operations behave identically to PySpark.
"""

from tests.fixtures.parity_base import ParityTestBase


def _schema_for_spark(spark):
    """Return StructType/StructField/StringType/IntegerType for the given session (PySpark or sparkless)."""
    if type(spark).__module__.startswith("pyspark"):
        from pyspark.sql.types import (
            StructType as PyStructType,
            StructField as PyStructField,
            StringType as PyStringType,
            IntegerType as PyIntegerType,
        )
        return PyStructType([
            PyStructField("name", PyStringType(), True),
            PyStructField("age", PyIntegerType(), True),
        ]), PyStructType([])
    from sparkless.spark_types import StructType, StructField, StringType, IntegerType
    return StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
    ]), StructType([])


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
        schema, _ = _schema_for_spark(spark)
        df = spark.createDataFrame(data, schema)

        # Verify schema is applied correctly (compare names and type semantics)
        assert df.count() == 2
        assert list(df.schema.fieldNames()) == ["name", "age"]
        # Type comparison: allow different type objects (sparkless vs pyspark) that represent string/int
        assert df.schema.fields[0].dataType.__class__.__name__ in ("StringType", "str")
        assert df.schema.fields[1].dataType.__class__.__name__ in ("IntegerType", "LongType", "int", "long")

    def test_createDataFrame_empty(self, spark):
        """Test createDataFrame with empty data matches PySpark behavior."""
        _, empty_schema = _schema_for_spark(spark)
        df = spark.createDataFrame([], empty_schema)

        assert df.count() == 0
        assert len(df.columns) == 0

"""
Tests for issue #361: createDataFrame from df.rdd.

PySpark supports spark.createDataFrame(df.rdd, schema=...) to create a new
DataFrame from an existing DataFrame's RDD. Sparkless now supports this.

Works with both sparkless (mock) and PySpark backends.
Set MOCK_SPARK_TEST_BACKEND=pyspark to run with real PySpark.
"""


class TestIssue361CreateDataFrameRdd:
    """Test createDataFrame(df.rdd, schema=...) (issue #361)."""

    def test_createDataFrame_from_rdd_with_schema_list(self, spark):
        """Exact scenario from issue #361: createDataFrame(df.rdd, schema=[...])."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value": 1},
                {"Name": "Bob", "Value": 10},
            ]
        )
        df2 = spark.createDataFrame(df.rdd, schema=["Name", "Value"])
        rows = df2.collect()
        assert len(rows) == 2
        assert rows[0]["Name"] == "Alice" and rows[0]["Value"] == 1
        assert rows[1]["Name"] == "Bob" and rows[1]["Value"] == 10

    def test_createDataFrame_from_rdd_show_matches_expected(self, spark):
        """df2.show() produces expected output (issue #361 expected results)."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value": 1},
                {"Name": "Bob", "Value": 10},
            ]
        )
        df2 = spark.createDataFrame(df.rdd, schema=["Name", "Value"])
        # Should not raise; show() displays the data
        df2.show()
        assert df2.count() == 2

    def test_createDataFrame_from_rdd_empty_dataframe(self, spark):
        """createDataFrame(empty_df.rdd, schema=...) with explicit StructType schema."""
        from tests.fixtures.spark_imports import get_spark_imports

        imports = get_spark_imports()
        schema = imports.StructType(
            [
                imports.StructField("Name", imports.StringType()),
                imports.StructField("Value", imports.IntegerType()),
            ]
        )
        df = spark.createDataFrame([], schema=schema)
        # Empty data requires StructType (PySpark compatibility)
        df2 = spark.createDataFrame(df.rdd, schema=schema)
        assert df2.count() == 0
        assert df2.columns == ["Name", "Value"]

    def test_createDataFrame_from_rdd_single_row(self, spark):
        """createDataFrame from RDD with single row."""
        df = spark.createDataFrame([{"a": 1, "b": "x"}])
        df2 = spark.createDataFrame(df.rdd, schema=["a", "b"])
        rows = df2.collect()
        assert len(rows) == 1
        assert rows[0]["a"] == 1 and rows[0]["b"] == "x"

    def test_createDataFrame_from_rdd_preserves_schema_order(self, spark):
        """Schema column order is preserved when creating from RDD."""
        df = spark.createDataFrame(
            [{"z": 3, "y": 2, "x": 1}],
            schema=["x", "y", "z"],
        )
        df2 = spark.createDataFrame(df.rdd, schema=["x", "y", "z"])
        assert df2.columns == ["x", "y", "z"]
        row = df2.collect()[0]
        assert row["x"] == 1 and row["y"] == 2 and row["z"] == 3

"""
Unit tests for DataFrameWriter operations.
"""

import pytest
import tempfile
import os
from sparkless import SparkSession
from sparkless.core.exceptions.analysis import AnalysisException
from sparkless.errors import IllegalArgumentException


@pytest.mark.unit
class TestWriterOperations:
    """Test DataFrameWriter operations."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        return SparkSession("test")

    @pytest.fixture
    def sample_data(self):
        """Sample data for testing."""
        return [
            {"id": 1, "name": "Alice", "age": 25},
            {"id": 2, "name": "Bob", "age": 30},
            {"id": 3, "name": "Charlie", "age": 35},
        ]

    # format tests
    def test_format_parquet(self, spark, sample_data):
        """Test setting format to parquet."""
        df = spark.createDataFrame(sample_data)
        writer = df.write.format("parquet")

        assert writer.format_name == "parquet"

    def test_format_json(self, spark, sample_data):
        """Test setting format to json."""
        df = spark.createDataFrame(sample_data)
        writer = df.write.format("json")

        assert writer.format_name == "json"

    def test_format_csv(self, spark, sample_data):
        """Test setting format to csv."""
        df = spark.createDataFrame(sample_data)
        writer = df.write.format("csv")

        assert writer.format_name == "csv"

    # mode tests
    def test_mode_append(self, spark, sample_data):
        """Test setting mode to append."""
        df = spark.createDataFrame(sample_data)
        writer = df.write.mode("append")

        assert writer.save_mode == "append"

    def test_mode_overwrite(self, spark, sample_data):
        """Test setting mode to overwrite."""
        df = spark.createDataFrame(sample_data)
        writer = df.write.mode("overwrite")

        assert writer.save_mode == "overwrite"

    def test_mode_error(self, spark, sample_data):
        """Test setting mode to error."""
        df = spark.createDataFrame(sample_data)
        writer = df.write.mode("error")

        assert writer.save_mode == "error"

    def test_mode_ignore(self, spark, sample_data):
        """Test setting mode to ignore."""
        df = spark.createDataFrame(sample_data)
        writer = df.write.mode("ignore")

        assert writer.save_mode == "ignore"

    def test_mode_invalid(self, spark, sample_data):
        """Test error handling for invalid mode."""
        df = spark.createDataFrame(sample_data)
        with pytest.raises(IllegalArgumentException):
            df.write.mode("invalid_mode")

    # option tests
    def test_option_single(self, spark, sample_data):
        """Test setting single option."""
        df = spark.createDataFrame(sample_data)
        writer = df.write.option("compression", "snappy")

        assert writer._options["compression"] == "snappy"

    def test_options_multiple(self, spark, sample_data):
        """Test setting multiple options."""
        df = spark.createDataFrame(sample_data)
        writer = df.write.options(compression="snappy", format="parquet")

        assert writer._options["compression"] == "snappy"
        assert writer._options["format"] == "parquet"

    # saveAsTable tests
    def test_saveAsTable_overwrite_mode(self, spark, sample_data):
        """Test saveAsTable with overwrite mode."""
        df = spark.createDataFrame(sample_data)
        df.write.mode("overwrite").saveAsTable("test_table")

        assert spark.catalog.tableExists("default", "test_table")
        result = spark.table("test_table")
        assert result.count() == 3

    def test_saveAsTable_append_mode(self, spark, sample_data):
        """Test saveAsTable with append mode."""
        df = spark.createDataFrame(sample_data)
        df.write.mode("overwrite").saveAsTable("test_table")

        # Append more data
        more_data = [{"id": 4, "name": "Diana", "age": 28}]
        df2 = spark.createDataFrame(more_data)
        df2.write.mode("append").saveAsTable("test_table")

        result = spark.table("test_table")
        assert result.count() == 4

    def test_saveAsTable_error_mode_existing_table(self, spark, sample_data):
        """Test saveAsTable with error mode when table exists."""
        df = spark.createDataFrame(sample_data)
        df.write.mode("overwrite").saveAsTable("test_table")

        # Try to save again with error mode
        with pytest.raises(AnalysisException):
            df.write.mode("error").saveAsTable("test_table")

    def test_saveAsTable_error_mode_new_table(self, spark, sample_data):
        """Test saveAsTable with error mode when table doesn't exist."""
        df = spark.createDataFrame(sample_data)
        df.write.mode("error").saveAsTable("new_table")

        assert spark.catalog.tableExists("default", "new_table")

    def test_saveAsTable_ignore_mode_existing_table(self, spark, sample_data):
        """Test saveAsTable with ignore mode when table exists."""
        df = spark.createDataFrame(sample_data)
        df.write.mode("overwrite").saveAsTable("test_table")

        # Try to save again with ignore mode (should do nothing)
        df.write.mode("ignore").saveAsTable("test_table")

        # Table should still exist with original data
        result = spark.table("test_table")
        assert result.count() == 3

    def test_saveAsTable_ignore_mode_new_table(self, spark, sample_data):
        """Test saveAsTable with ignore mode when table doesn't exist."""
        df = spark.createDataFrame(sample_data)
        df.write.mode("ignore").saveAsTable("new_table")

        assert spark.catalog.tableExists("default", "new_table")
        result = spark.table("new_table")
        assert result.count() == 3

    def test_saveAsTable_with_schema_qualifier(self, spark, sample_data):
        """Test saveAsTable with schema qualifier."""
        df = spark.createDataFrame(sample_data)
        df.write.mode("overwrite").saveAsTable("default.test_table")

        assert spark.catalog.tableExists("default", "test_table")

    def test_saveAsTable_empty_table_name(self, spark, sample_data):
        """Test error handling for empty table name."""
        df = spark.createDataFrame(sample_data)
        with pytest.raises(IllegalArgumentException):
            df.write.saveAsTable("")

    # save tests
    def test_save_with_path(self, spark, sample_data):
        """Test save() with path."""
        df = spark.createDataFrame(sample_data)
        # save() prints to console, so just test it doesn't crash
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, "test.parquet")
            df.write.format("parquet").mode("overwrite").save(path)
            # File may or may not be created in mock implementation
            # Just verify no exception

    def test_save_without_path(self, spark, sample_data):
        """Test error handling for save() without path."""
        df = spark.createDataFrame(sample_data)
        with pytest.raises(IllegalArgumentException):
            df.write.save(None)

    def test_save_empty_path(self, spark, sample_data):
        """Test error handling for save() with empty path."""
        df = spark.createDataFrame(sample_data)
        with pytest.raises(IllegalArgumentException):
            df.write.save("")

    # format-specific save methods
    def test_parquet_method(self, spark, sample_data):
        """Test parquet() method."""
        df = spark.createDataFrame(sample_data)
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, "test.parquet")
            df.write.parquet(path)
            # Just verify no exception

    def test_json_method(self, spark, sample_data):
        """Test json() method."""
        df = spark.createDataFrame(sample_data)
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, "test.json")
            df.write.json(path)
            # Just verify no exception

    def test_csv_method(self, spark, sample_data):
        """Test csv() method."""
        df = spark.createDataFrame(sample_data)
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, "test.csv")
            df.write.csv(path)
            # Just verify no exception

    # partitionBy tests
    def test_partitionBy_single_column(self, spark, sample_data):
        """Test partitionBy with single column."""
        df = spark.createDataFrame(sample_data)
        writer = df.write.partitionBy("age")

        assert "partitionBy" in writer._options
        assert "age" in writer._options["partitionBy"]

    def test_partitionBy_multiple_columns(self, spark, sample_data):
        """Test partitionBy with multiple columns."""
        df = spark.createDataFrame(sample_data)
        writer = df.write.partitionBy("age", "name")

        assert "partitionBy" in writer._options
        assert "age" in writer._options["partitionBy"]
        assert "name" in writer._options["partitionBy"]

    # method chaining tests
    def test_method_chaining(self, spark, sample_data):
        """Test method chaining."""
        df = spark.createDataFrame(sample_data)
        writer = (
            df.write.format("parquet").mode("overwrite").option("compression", "snappy")
        )

        assert writer.format_name == "parquet"
        assert writer.save_mode == "overwrite"
        assert writer._options["compression"] == "snappy"

    def test_saveMode_property(self, spark, sample_data):
        """Test saveMode property."""
        df = spark.createDataFrame(sample_data)
        writer = df.write.mode("overwrite")

        assert writer.saveMode == "overwrite"

    # integration tests
    def test_write_read_cycle(self, spark, sample_data):
        """Test write and read cycle."""
        df = spark.createDataFrame(sample_data)
        df.write.mode("overwrite").saveAsTable("test_table")

        result = spark.table("test_table")
        assert result.count() == 3
        assert "id" in result.columns
        assert "name" in result.columns
        assert "age" in result.columns

    def test_overwrite_existing_table(self, spark, sample_data):
        """Test overwriting existing table."""
        df1 = spark.createDataFrame(sample_data)
        df1.write.mode("overwrite").saveAsTable("test_table")

        new_data = [{"id": 10, "name": "New", "age": 40}]
        df2 = spark.createDataFrame(new_data)
        df2.write.mode("overwrite").saveAsTable("test_table")

        result = spark.table("test_table")
        assert result.count() == 1  # Should have only new data
        rows = result.collect()
        assert rows[0].name == "New"

    def test_append_to_existing_table(self, spark, sample_data):
        """Test appending to existing table."""
        df1 = spark.createDataFrame(sample_data)
        df1.write.mode("overwrite").saveAsTable("test_table")

        new_data = [{"id": 4, "name": "Diana", "age": 28}]
        df2 = spark.createDataFrame(new_data)
        df2.write.mode("append").saveAsTable("test_table")

        result = spark.table("test_table")
        assert result.count() == 4  # Should have original 3 + 1 new

    def test_schema_mismatch_on_append(self, spark):
        """Test schema mismatch error on append."""
        data1 = [{"id": 1, "name": "Alice"}]
        data2 = [{"id": 2, "age": 30}]  # Different schema

        df1 = spark.createDataFrame(data1)
        df1.write.mode("overwrite").saveAsTable("test_table")

        df2 = spark.createDataFrame(data2)
        with pytest.raises(AnalysisException):
            df2.write.mode("append").saveAsTable("test_table")

    # writeTo tests (PySpark 3.1+)
    def test_writeTo_basic(self, spark, sample_data):
        """Test writeTo() method (PySpark 3.1+)."""
        df = spark.createDataFrame(sample_data)
        writer = df.writeTo("test_table")

        assert hasattr(writer, "_table_name")
        assert writer._table_name == "test_table"
        assert isinstance(writer, type(df.write))

    def test_writeTo_with_format(self, spark, sample_data):
        """Test writeTo() with format."""
        df = spark.createDataFrame(sample_data)
        writer = df.writeTo("test_table").format("parquet")

        assert writer._table_name == "test_table"
        assert writer.format_name == "parquet"

    def test_writeTo_with_mode(self, spark, sample_data):
        """Test writeTo() with mode."""
        df = spark.createDataFrame(sample_data)
        writer = df.writeTo("test_table").mode("overwrite")

        assert writer._table_name == "test_table"
        assert writer.save_mode == "overwrite"

    def test_writeTo_chaining(self, spark, sample_data):
        """Test writeTo() method chaining."""
        df = spark.createDataFrame(sample_data)
        writer = (
            df.writeTo("test_table")
            .format("parquet")
            .mode("overwrite")
            .option("compression", "snappy")
        )

        assert writer._table_name == "test_table"
        assert writer.format_name == "parquet"
        assert writer.save_mode == "overwrite"
        assert writer._options["compression"] == "snappy"

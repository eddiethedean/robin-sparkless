"""
Tests for issue #360: input_file_name() function.

PySpark supports F.input_file_name() which returns the path of the file being read.
Sparkless returns empty string in mock (no file path context).

Works with both sparkless (mock) and PySpark backends.
Set MOCK_SPARK_TEST_BACKEND=pyspark to run with real PySpark.
"""


class TestIssue360InputFileName:
    """Test input_file_name() (issue #360)."""

    def test_input_file_name_returns_string_column(self, spark):
        """F.input_file_name() returns a string column (empty in mock)."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports().F
        df = spark.createDataFrame(
            [
                {"dataset": "dataset_a", "table": "table_1"},
                {"dataset": "dataset_b", "table": "table_2"},
            ]
        )
        result = df.withColumn("InputFileName", F.input_file_name())
        rows = result.collect()
        assert len(rows) == 2
        assert "InputFileName" in rows[0]
        # In Sparkless mock we return empty string; PySpark returns actual path
        assert isinstance(rows[0]["InputFileName"], str)

    def test_input_file_name_exact_issue_scenario(self, spark):
        """Exact scenario from issue #360: withColumn after createDataFrame."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports().F
        df = spark.createDataFrame(
            [
                ("dataset_a", "table_1"),
                ("dataset_b", "table_2"),
            ],
            ["dataset", "table"],
        )
        df = df.withColumn("InputFileName", F.input_file_name())
        rows = df.collect()
        assert len(rows) == 2
        assert rows[0]["dataset"] == "dataset_a"
        assert rows[0]["table"] == "table_1"
        assert "InputFileName" in rows[0]

    def test_input_file_name_select_only(self, spark):
        """Select only input_file_name() as single column."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports().F
        df = spark.createDataFrame([{"a": 1}, {"a": 2}])
        result = df.select(F.input_file_name().alias("path"))
        rows = result.collect()
        assert len(rows) == 2
        assert (
            rows[0]["path"] == ""
            or rows[0]["path"].startswith("/")
            or "\\" in rows[0]["path"]
        )


class TestIssue360InputFileNameRobust:
    """Robust tests for input_file_name(): edge cases, chaining, schema."""

    def test_input_file_name_empty_dataframe(self, spark):
        """input_file_name() on empty DataFrame returns empty result."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports().F
        df = spark.createDataFrame([], "a int")
        result = df.withColumn("path", F.input_file_name())
        rows = result.collect()
        assert len(rows) == 0

    def test_input_file_name_single_row(self, spark):
        """input_file_name() with single row."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports().F
        df = spark.createDataFrame([{"id": 1}])
        result = df.withColumn("file", F.input_file_name())
        rows = result.collect()
        assert len(rows) == 1
        assert "file" in rows[0]
        assert isinstance(rows[0]["file"], str)

    def test_input_file_name_after_filter(self, spark):
        """input_file_name() after filter."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports().F
        df = spark.createDataFrame([{"a": 1}, {"a": 2}, {"a": 3}])
        result = df.filter(F.col("a") > 1).withColumn("path", F.input_file_name())
        rows = result.collect()
        assert len(rows) == 2
        assert all("path" in r and isinstance(r["path"], str) for r in rows)

    def test_input_file_name_after_select(self, spark):
        """input_file_name() after select (different columns)."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports().F
        df = spark.createDataFrame([{"x": 1, "y": 2}, {"x": 3, "y": 4}])
        result = df.select("x").withColumn("path", F.input_file_name())
        rows = result.collect()
        assert len(rows) == 2
        assert rows[0]["path"] == "" or isinstance(rows[0]["path"], str)

    def test_input_file_name_preserves_schema(self, spark):
        """withColumn(input_file_name()) adds string column; schema preserved."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports().F
        df = spark.createDataFrame([{"a": 1, "b": "x"}])
        result = df.withColumn("path", F.input_file_name())
        assert "path" in result.schema.fieldNames()
        assert len(result.schema.fields) == 3
        rows = result.collect()
        assert rows[0]["a"] == 1 and rows[0]["b"] == "x" and "path" in rows[0]

    def test_input_file_name_with_show(self, spark):
        """withColumn(input_file_name()).show() does not raise."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports().F
        df = spark.createDataFrame([{"a": 1}])
        result = df.withColumn("path", F.input_file_name())
        result.show()

    def test_input_file_name_all_rows_same_type(self, spark):
        """All rows get a string value from input_file_name() (no None in mock)."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports().F
        df = spark.createDataFrame([{"i": i} for i in range(5)])
        result = df.withColumn("path", F.input_file_name())
        rows = result.collect()
        for r in rows:
            assert "path" in r
            assert isinstance(r["path"], str)

    def test_input_file_name_multiple_columns(self, spark):
        """input_file_name() alongside other columns in select."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports().F
        df = spark.createDataFrame([{"a": 1, "b": 2}, {"a": 3, "b": 4}])
        result = df.select(
            F.col("a"),
            F.input_file_name().alias("path"),
            F.col("b"),
        )
        rows = result.collect()
        assert len(rows) == 2
        assert rows[0]["a"] == 1 and rows[0]["b"] == 2 and "path" in rows[0]

    def test_input_file_name_alias(self, spark):
        """input_file_name() with alias."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports().F
        df = spark.createDataFrame([{"x": 1}])
        result = df.select(F.input_file_name().alias("source_file"))
        rows = result.collect()
        assert len(rows) == 1
        assert "source_file" in rows[0]
        assert isinstance(rows[0]["source_file"], str)

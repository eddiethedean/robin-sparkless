"""
Tests for issue #367: F.array() and F.array([]) return empty array [].

PySpark supports F.array() and F.array([]) which return an empty array column.
Sparkless now supports both (PySpark parity).

Works with both sparkless (mock) and PySpark backends.
Set MOCK_SPARK_TEST_BACKEND=pyspark to run with real PySpark.
"""


class TestIssue367ArrayEmpty:
    """Test F.array() and F.array([]) return empty array (issue #367)."""

    def test_array_no_args_returns_empty_array(self, spark):
        """Exact scenario from issue #367: withColumn + F.array()."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports().F
        df = spark.createDataFrame([{"Name": "Alice"}, {"Name": "Bob"}])
        df = df.withColumn("NewArray", F.array())
        rows = df.collect()
        assert len(rows) == 2
        assert rows[0]["NewArray"] == []
        assert rows[1]["NewArray"] == []

    def test_array_empty_list_returns_empty_array(self, spark):
        """F.array([]) returns empty array [] (issue #367)."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports().F
        df = spark.createDataFrame([{"Name": "Alice"}, {"Name": "Bob"}])
        df = df.withColumn("NewArray", F.array([]))
        rows = df.collect()
        assert len(rows) == 2
        assert rows[0]["NewArray"] == []
        assert rows[1]["NewArray"] == []

    def test_array_empty_show(self, spark):
        """Exact issue scenario: withColumn + F.array() + show()."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports().F
        df = spark.createDataFrame([{"Name": "Alice"}, {"Name": "Bob"}])
        df = df.withColumn("NewArray", F.array())
        df.show()
        rows = df.collect()
        assert len(rows) == 2
        assert rows[0]["Name"] == "Alice" and rows[0]["NewArray"] == []
        assert rows[1]["Name"] == "Bob" and rows[1]["NewArray"] == []

    def test_array_empty_in_select(self, spark):
        """F.array() in select statement."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports().F
        df = spark.createDataFrame([{"id": 1}, {"id": 2}, {"id": 3}])
        result = df.select(F.col("id"), F.array().alias("empty_array"))
        rows = result.collect()
        assert len(rows) == 3
        for row in rows:
            assert row["empty_array"] == []
            assert "id" in row

    def test_array_empty_list_and_array_equivalent(self, spark):
        """F.array() and F.array([]) produce identical results."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports().F
        df = spark.createDataFrame([{"id": 1}, {"id": 2}])
        result = df.select(
            F.col("id"),
            F.array().alias("arr_no_args"),
            F.array([]).alias("arr_empty_list"),
        )
        rows = result.collect()
        assert len(rows) == 2
        for row in rows:
            assert row["arr_no_args"] == []
            assert row["arr_empty_list"] == []
            assert row["arr_no_args"] == row["arr_empty_list"]

    def test_array_empty_after_filter(self, spark):
        """F.array() works after filter."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports().F
        df = spark.createDataFrame(
            [
                {"id": 1, "value": 10},
                {"id": 2, "value": 20},
                {"id": 3, "value": 30},
            ]
        )
        result = df.filter(F.col("value") > 15).withColumn("empty_array", F.array())
        rows = result.collect()
        assert len(rows) == 2
        for row in rows:
            assert row["empty_array"] == []
            assert row["value"] > 15

    def test_array_empty_in_union(self, spark):
        """F.array() works with union."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports().F
        df1 = spark.createDataFrame([{"id": 1, "val": "a"}])
        df2 = spark.createDataFrame([{"id": 2, "val": "b"}])
        df1_with_arr = df1.withColumn("arr", F.array())
        df2_with_arr = df2.withColumn("arr", F.array())
        result = df1_with_arr.union(df2_with_arr)
        rows = result.collect()
        assert len(rows) == 2
        for row in rows:
            assert row["arr"] == []

    def test_array_empty_tuple_raises_like_pyspark(self, spark):
        """F.array(()) raises in Sparkless (matches PySpark, which rejects tuple)."""
        from tests.fixtures.spark_imports import get_spark_imports

        import pytest

        F = get_spark_imports().F
        with pytest.raises(
            Exception
        ):  # PySparkTypeError in PySpark, ValueError in Sparkless
            F.array(())

    def test_array_empty_multiple_times(self, spark):
        """Multiple F.array() in a single select."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports().F
        df = spark.createDataFrame([{"id": 1}])
        result = df.select(
            F.col("id"),
            F.array().alias("arr1"),
            F.array().alias("arr2"),
            F.array().alias("arr3"),
        )
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]["arr1"] == []
        assert rows[0]["arr2"] == []
        assert rows[0]["arr3"] == []

    def test_array_empty_with_different_data_types(self, spark):
        """F.array() with DataFrames containing different data types."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports().F
        df1 = spark.createDataFrame([{"name": "Alice"}])
        assert df1.withColumn("arr", F.array()).collect()[0]["arr"] == []

        df2 = spark.createDataFrame([{"age": 25}])
        assert df2.withColumn("arr", F.array()).collect()[0]["arr"] == []

        df3 = spark.createDataFrame([{"active": True}])
        assert df3.withColumn("arr", F.array()).collect()[0]["arr"] == []

    def test_array_empty_with_computed_columns(self, spark):
        """F.array() alongside computed columns."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports().F
        df = spark.createDataFrame([{"a": 10, "b": 20}])
        result = df.select(
            F.col("a"),
            F.col("b"),
            (F.col("a") + F.col("b")).alias("sum"),
            F.array().alias("empty_arr"),
        )
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]["sum"] == 30
        assert rows[0]["empty_arr"] == []

    def test_array_empty_in_join(self, spark):
        """F.array() works in join operations."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports().F
        df1 = spark.createDataFrame([{"id": 1, "name": "Alice"}])
        df2 = spark.createDataFrame([{"id": 1, "age": 25}])
        df1_with_arr = df1.withColumn("arr", F.array())
        result = df1_with_arr.join(df2, "id")
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]["arr"] == []
        assert rows[0]["name"] == "Alice"
        assert rows[0]["age"] == 25

    def test_array_empty_two_equivalent(self, spark):
        """F.array() and F.array([]) produce identical results (PySpark parity)."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports().F
        df = spark.createDataFrame([{"id": 1}, {"id": 2}])
        result = df.select(
            F.col("id"),
            F.array().alias("no_args"),
            F.array([]).alias("empty_list"),
        )
        rows = result.collect()
        assert len(rows) == 2
        for row in rows:
            assert row["no_args"] == []
            assert row["empty_list"] == []
            assert row["no_args"] == row["empty_list"]

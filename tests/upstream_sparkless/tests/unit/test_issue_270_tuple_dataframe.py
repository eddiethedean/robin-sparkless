"""
Unit tests for issue #270: tuple-based DataFrame creation.

Tests verify that createDataFrame correctly converts tuple-based data
to dictionaries when a StructType schema is provided, ensuring all
downstream operations work correctly.
"""

import pytest
from tests.fixtures.spark_imports import get_spark_imports
from tests.fixtures.spark_backend import get_backend_type, BackendType


class TestIssue270TupleDataFrame:
    """Test tuple-based DataFrame creation (Issue #270)."""

    def test_tuple_data_with_structtype_schema(self, spark):
        """Test that tuple data with StructType schema converts to dicts."""
        imports = get_spark_imports()
        T = imports

        data = [("Alice", 1), ("Bob", 2)]
        schema = T.StructType(
            [
                T.StructField(name="Name", dataType=T.StringType()),
                T.StructField(name="Value", dataType=T.IntegerType()),
            ]
        )

        df = spark.createDataFrame(data=data, schema=schema)

        # Verify show() works (the original failing operation)
        df.show()  # Should not raise AttributeError

        # Check backend-specific attributes (Sparkless has .data, PySpark doesn't)
        backend = get_backend_type()
        if backend == BackendType.MOCK:
            # Verify data is converted to dictionaries in Sparkless
            assert isinstance(df.data[0], dict)
            assert df.data[0] == {"Name": "Alice", "Value": 1}
            assert df.data[1] == {"Name": "Bob", "Value": 2}

        # Verify collect() works in both modes
        rows = df.collect()
        assert len(rows) == 2
        # Access via Row object or dict depending on backend
        if hasattr(rows[0], "Name"):
            assert rows[0].Name == "Alice"
            assert rows[0].Value == 1
        elif isinstance(rows[0], dict):
            assert rows[0]["Name"] == "Alice"
            assert rows[0]["Value"] == 1

    def test_tuple_data_show_works(self, spark):
        """Test that show() works with tuple-based data."""
        imports = get_spark_imports()
        T = imports

        data = [("Alice", 1), ("Bob", 2)]
        schema = T.StructType(
            [
                T.StructField(name="Name", dataType=T.StringType()),
                T.StructField(name="Value", dataType=T.IntegerType()),
            ]
        )

        df = spark.createDataFrame(data=data, schema=schema)

        # This should work without AttributeError
        try:
            df.show()
        except AttributeError as e:
            if "'tuple' object has no attribute" in str(e):
                pytest.fail(f"show() failed with tuple error: {e}")

    def test_tuple_data_unionByName_works(self, spark):
        """Test that unionByName() works with tuple-based data."""
        imports = get_spark_imports()
        T = imports

        data1 = [("Alice", 1), ("Bob", 2)]
        data2 = [("Charlie", 3)]
        schema = T.StructType(
            [
                T.StructField(name="Name", dataType=T.StringType()),
                T.StructField(name="Value", dataType=T.IntegerType()),
            ]
        )

        df1 = spark.createDataFrame(data=data1, schema=schema)
        df2 = spark.createDataFrame(data=data2, schema=schema)

        # This should work without AttributeError
        result = df1.unionByName(df2)
        assert result.count() == 3

    def test_tuple_data_operations_work(self, spark):
        """Test various operations that use .get(), .items(), .copy() work."""
        imports = get_spark_imports()
        T = imports

        data = [("Alice", 1, "IT"), ("Bob", 2, "HR")]
        schema = T.StructType(
            [
                T.StructField(name="Name", dataType=T.StringType()),
                T.StructField(name="Value", dataType=T.IntegerType()),
                T.StructField(name="Dept", dataType=T.StringType()),
            ]
        )

        df = spark.createDataFrame(data=data, schema=schema)

        # Test fillna (uses .copy())
        df_filled = df.fillna({"Value": 0})
        assert df_filled.count() == 2

        # Test replace (uses .items())
        df_replaced = df.replace({"IT": "Engineering"})
        assert df_replaced.count() == 2

        # Test select (uses .get() indirectly)
        result = df.select("Name", "Value").collect()
        assert len(result) == 2

    def test_mixed_tuple_and_dict_data(self, spark):
        """Test mixed tuple and dict data with StructType schema."""
        imports = get_spark_imports()
        T = imports

        data = [("Alice", 1), {"Name": "Bob", "Value": 2}]
        schema = T.StructType(
            [
                T.StructField(name="Name", dataType=T.StringType()),
                T.StructField(name="Value", dataType=T.IntegerType()),
            ]
        )

        df = spark.createDataFrame(data=data, schema=schema)
        assert df.count() == 2

        # Check backend-specific attributes (Sparkless has .data, PySpark doesn't)
        backend = get_backend_type()
        if backend == BackendType.MOCK:
            assert isinstance(df.data[0], dict)
            assert isinstance(df.data[1], dict)

    def test_tuple_data_single_column(self, spark):
        """Test tuple data with single column schema."""
        imports = get_spark_imports()
        T = imports

        data = [("Alice",), ("Bob",)]
        schema = T.StructType([T.StructField(name="Name", dataType=T.StringType())])

        df = spark.createDataFrame(data=data, schema=schema)
        assert df.count() == 2

        # Check backend-specific attributes (Sparkless has .data, PySpark doesn't)
        backend = get_backend_type()
        if backend == BackendType.MOCK:
            assert df.data[0] == {"Name": "Alice"}
        df.show()  # Should work

    def test_tuple_data_mismatched_length(self, spark):
        """Test tuple data where tuple length doesn't match schema.

        PySpark raises PySparkValueError for mismatched lengths.
        Sparkless should match this behavior.
        """
        imports = get_spark_imports()
        T = imports

        data = [("Alice",), ("Bob", 2)]  # First tuple missing a value
        schema = T.StructType(
            [
                T.StructField(name="Name", dataType=T.StringType()),
                T.StructField(name="Value", dataType=T.IntegerType()),
            ]
        )

        # Both PySpark and Sparkless should raise an error
        backend = get_backend_type()
        if backend == BackendType.PYSPARK:
            with pytest.raises(Exception) as exc_info:
                spark.createDataFrame(data=data, schema=schema)
            assert (
                "LENGTH_SHOULD_BE_THE_SAME" in str(exc_info.value)
                or "length" in str(exc_info.value).lower()
            )
        else:
            from sparkless.core.exceptions.validation import IllegalArgumentException

            with pytest.raises(IllegalArgumentException) as exc_info:
                spark.createDataFrame(data=data, schema=schema)
            assert "LENGTH_SHOULD_BE_THE_SAME" in str(exc_info.value)

    def test_tuple_data_empty_schema(self, spark):
        """Test tuple data with empty schema.

        PySpark raises PySparkValueError for mismatched lengths.
        Sparkless should match this behavior.
        """
        imports = get_spark_imports()
        T = imports

        data = [("Alice", 1), ("Bob", 2)]
        schema = T.StructType([])

        # Both PySpark and Sparkless should raise an error
        backend = get_backend_type()
        if backend == BackendType.PYSPARK:
            with pytest.raises(Exception) as exc_info:
                spark.createDataFrame(data=data, schema=schema)
            assert (
                "LENGTH_SHOULD_BE_THE_SAME" in str(exc_info.value)
                or "length" in str(exc_info.value).lower()
            )
        else:
            from sparkless.core.exceptions.validation import IllegalArgumentException

            with pytest.raises(IllegalArgumentException) as exc_info:
                spark.createDataFrame(data=data, schema=schema)
            assert "LENGTH_SHOULD_BE_THE_SAME" in str(exc_info.value)

    def test_list_data_with_structtype_schema(self, spark):
        """Test that list data (not just tuples) also converts correctly."""
        imports = get_spark_imports()
        T = imports

        data = [["Alice", 1], ["Bob", 2]]
        schema = T.StructType(
            [
                T.StructField(name="Name", dataType=T.StringType()),
                T.StructField(name="Value", dataType=T.IntegerType()),
            ]
        )

        df = spark.createDataFrame(data=data, schema=schema)

        # Check backend-specific attributes (Sparkless has .data, PySpark doesn't)
        backend = get_backend_type()
        if backend == BackendType.MOCK:
            assert isinstance(df.data[0], dict)
            assert df.data[0] == {"Name": "Alice", "Value": 1}
        df.show()  # Should work

    def test_pyspark_parity_exact_example(self, spark):
        """Test the exact example from issue #270."""
        imports = get_spark_imports()
        T = imports

        data = [("Alice", 1), ("Bob", 2)]
        schema = T.StructType(
            [
                T.StructField(name="Name", dataType=T.StringType()),
                T.StructField(name="Value", dataType=T.IntegerType()),
            ]
        )

        df = spark.createDataFrame(data=data, schema=schema)

        # This should work exactly as PySpark does
        df.show()

        # Verify structure matches expected output
        rows = df.collect()
        assert len(rows) == 2

        # Handle both Row objects (PySpark) and dicts (Sparkless)
        backend = get_backend_type()
        if backend == BackendType.PYSPARK:
            # PySpark returns Row objects
            assert rows[0].Name == "Alice"
            assert rows[0].Value == 1
            assert rows[1].Name == "Bob"
            assert rows[1].Value == 2
        else:
            # Sparkless returns dicts
            assert rows[0]["Name"] == "Alice"
            assert rows[0]["Value"] == 1
            assert rows[1]["Name"] == "Bob"
            assert rows[1]["Value"] == 2

    def test_tuple_with_none_values(self, spark):
        """Test tuple data with None values."""
        imports = get_spark_imports()
        T = imports

        data = [("Alice", None), ("Bob", 2), (None, 3)]
        schema = T.StructType(
            [
                T.StructField(name="Name", dataType=T.StringType()),
                T.StructField(name="Value", dataType=T.IntegerType()),
            ]
        )

        df = spark.createDataFrame(data=data, schema=schema)
        assert df.count() == 3
        df.show()  # Should work with None values

        # Verify None values are preserved
        rows = df.collect()
        backend = get_backend_type()
        if backend == BackendType.PYSPARK:
            assert rows[0].Value is None
            assert rows[2].Name is None
        else:
            assert rows[0]["Value"] is None
            assert rows[2]["Name"] is None

    def test_tuple_with_different_data_types(self, spark):
        """Test tuple data with various data types."""
        imports = get_spark_imports()
        T = imports

        data = [
            ("Alice", 25, 75000.50, True, "2024-01-01"),
            ("Bob", 30, 80000.75, False, "2024-02-01"),
        ]
        schema = T.StructType(
            [
                T.StructField(name="Name", dataType=T.StringType()),
                T.StructField(name="Age", dataType=T.IntegerType()),
                T.StructField(name="Salary", dataType=T.DoubleType()),
                T.StructField(name="Active", dataType=T.BooleanType()),
                T.StructField(name="Date", dataType=T.StringType()),
            ]
        )

        df = spark.createDataFrame(data=data, schema=schema)
        assert df.count() == 2
        df.show()

        # Verify all operations work
        df.select("Name", "Salary").show()
        df.filter(df["Age"] > 25).show()
        df.withColumn("SalaryK", df["Salary"] / 1000).show()

    def test_tuple_data_with_long_schema(self, spark):
        """Test tuple data with many columns."""
        imports = get_spark_imports()
        T = imports

        data = [tuple(range(10)), tuple(range(10, 20))]
        schema = T.StructType(
            [
                T.StructField(name=f"col_{i}", dataType=T.IntegerType())
                for i in range(10)
            ]
        )

        df = spark.createDataFrame(data=data, schema=schema)
        assert df.count() == 2
        assert len(df.columns) == 10
        df.show()

    def test_tuple_data_single_row(self, spark):
        """Test tuple data with single row."""
        imports = get_spark_imports()
        T = imports

        data = [("Alice", 1)]
        schema = T.StructType(
            [
                T.StructField(name="Name", dataType=T.StringType()),
                T.StructField(name="Value", dataType=T.IntegerType()),
            ]
        )

        df = spark.createDataFrame(data=data, schema=schema)
        assert df.count() == 1
        df.show()

    def test_tuple_data_empty_dataframe_with_schema(self, spark):
        """Test empty DataFrame with schema from tuple format."""
        imports = get_spark_imports()
        T = imports

        data = []
        schema = T.StructType(
            [
                T.StructField(name="Name", dataType=T.StringType()),
                T.StructField(name="Value", dataType=T.IntegerType()),
            ]
        )

        df = spark.createDataFrame(data=data, schema=schema)
        assert df.count() == 0
        assert len(df.columns) == 2
        df.show()  # Should show empty DataFrame

    def test_tuple_data_mixed_with_row_objects(self, spark):
        """Test mixed tuple data and Row objects with named fields.

        Note: Row objects need named fields when used with positional data.
        """
        imports = get_spark_imports()
        T = imports
        Row = imports.Row

        # Row objects need to use named fields (like dicts) or match tuple structure
        # Create Row objects with named fields for compatibility
        data = [("Alice", 1), Row(Name="Bob", Value=2), ("Charlie", 3)]
        schema = T.StructType(
            [
                T.StructField(name="Name", dataType=T.StringType()),
                T.StructField(name="Value", dataType=T.IntegerType()),
            ]
        )

        df = spark.createDataFrame(data=data, schema=schema)
        assert df.count() == 3
        df.show()

    def test_tuple_data_operations_comprehensive(self, spark):
        """Test comprehensive operations on tuple-based DataFrame."""
        imports = get_spark_imports()
        T = imports

        data = [("Alice", 1, "IT"), ("Bob", 2, "HR"), ("Charlie", 3, "IT")]
        schema = T.StructType(
            [
                T.StructField(name="Name", dataType=T.StringType()),
                T.StructField(name="Value", dataType=T.IntegerType()),
                T.StructField(name="Dept", dataType=T.StringType()),
            ]
        )

        df = spark.createDataFrame(data=data, schema=schema)

        # Test various operations that use .get(), .items(), .copy()
        # fillna with subset
        df_filled = df.fillna({"Value": 0}, subset=["Value"])
        assert df_filled.count() == 3

        # replace
        df_replaced = df.replace({"IT": "Engineering"})
        assert df_replaced.count() == 3

        # dropna with subset
        df_with_nulls = spark.createDataFrame(
            [("Alice", None, "IT"), ("Bob", 2, "HR")], schema
        )
        df_dropped = df_with_nulls.dropna(subset=["Value"])
        assert df_dropped.count() == 1

        # groupBy
        grouped = df.groupBy("Dept").count()
        assert grouped.count() == 2

        # orderBy
        ordered = df.orderBy("Value")
        assert ordered.count() == 3

        # distinct
        distinct_depts = df.select("Dept").distinct()
        assert distinct_depts.count() == 2

    def test_tuple_data_union_operations(self, spark):
        """Test union operations with tuple-based DataFrames."""
        imports = get_spark_imports()
        T = imports

        data1 = [("Alice", 1), ("Bob", 2)]
        data2 = [("Charlie", 3), ("Diana", 4)]
        schema = T.StructType(
            [
                T.StructField(name="Name", dataType=T.StringType()),
                T.StructField(name="Value", dataType=T.IntegerType()),
            ]
        )

        df1 = spark.createDataFrame(data=data1, schema=schema)
        df2 = spark.createDataFrame(data=data2, schema=schema)

        # unionByName
        unioned = df1.unionByName(df2)
        assert unioned.count() == 4

        # union (should also work)
        unioned2 = df1.union(df2)
        assert unioned2.count() == 4

    def test_tuple_data_join_operations(self, spark):
        """Test join operations with tuple-based DataFrames."""
        imports = get_spark_imports()
        T = imports

        employees = [("Alice", 1, "IT"), ("Bob", 2, "HR")]
        departments = [("IT", "Engineering"), ("HR", "Human Resources")]
        emp_schema = T.StructType(
            [
                T.StructField(name="Name", dataType=T.StringType()),
                T.StructField(name="Id", dataType=T.IntegerType()),
                T.StructField(name="Dept", dataType=T.StringType()),
            ]
        )
        dept_schema = T.StructType(
            [
                T.StructField(name="Dept", dataType=T.StringType()),
                T.StructField(name="Name", dataType=T.StringType()),
            ]
        )

        df_emp = spark.createDataFrame(data=employees, schema=emp_schema)
        df_dept = spark.createDataFrame(data=departments, schema=dept_schema)

        # inner join
        joined = df_emp.join(df_dept, "Dept", "inner")
        assert joined.count() == 2

    def test_tuple_data_error_message_matches_pyspark(self, spark):
        """Test that error messages match PySpark exactly."""
        imports = get_spark_imports()
        T = imports

        # Test mismatched length - first tuple too short
        data = [("Alice",), ("Bob", 2)]
        schema = T.StructType(
            [
                T.StructField(name="Name", dataType=T.StringType()),
                T.StructField(name="Value", dataType=T.IntegerType()),
            ]
        )

        with pytest.raises(Exception) as exc_info:
            spark.createDataFrame(data=data, schema=schema)

        error_msg = str(exc_info.value)
        assert "LENGTH_SHOULD_BE_THE_SAME" in error_msg or "length" in error_msg.lower()
        assert "1" in error_msg  # Got 1 element
        assert "2" in error_msg  # Expected 2 fields

        # Test mismatched length - tuple too long
        data2 = [("Alice", 1, 100), ("Bob", 2)]
        with pytest.raises(Exception) as exc_info2:
            spark.createDataFrame(data=data2, schema=schema)

        error_msg2 = str(exc_info2.value)
        assert (
            "LENGTH_SHOULD_BE_THE_SAME" in error_msg2 or "length" in error_msg2.lower()
        )
        assert "3" in error_msg2  # Got 3 elements
        assert "2" in error_msg2  # Expected 2 fields

    def test_tuple_data_all_operations_from_issue(self, spark):
        """Test all operations mentioned in issue #270."""
        imports = get_spark_imports()
        T = imports

        data = [("Alice", 1), ("Bob", 2)]
        schema = T.StructType(
            [
                T.StructField(name="Name", dataType=T.StringType()),
                T.StructField(name="Value", dataType=T.IntegerType()),
            ]
        )

        df = spark.createDataFrame(data=data, schema=schema)

        # All operations that were failing in the issue should now work:
        # .show() - uses .keys()
        df.show()

        # .unionByName() - uses .get(), .items()
        df2 = spark.createDataFrame([("Charlie", 3)], schema)
        unioned = df.unionByName(df2)
        assert unioned.count() == 3

        # Any operation using .get(), .items(), .copy() should work
        result = df.select("Name").collect()
        assert len(result) == 2

    def test_tuple_data_with_array_type(self, spark):
        """Test tuple data with ArrayType in schema."""
        imports = get_spark_imports()
        T = imports

        data = [("Alice", [1, 2, 3]), ("Bob", [4, 5])]
        schema = T.StructType(
            [
                T.StructField(name="Name", dataType=T.StringType()),
                T.StructField(name="Values", dataType=T.ArrayType(T.IntegerType())),
            ]
        )

        df = spark.createDataFrame(data=data, schema=schema)
        assert df.count() == 2
        df.show()

    def test_tuple_data_preserves_order(self, spark):
        """Test that tuple-to-dict conversion preserves field order."""
        imports = get_spark_imports()
        T = imports

        data = [("Alice", 1, "IT")]
        schema = T.StructType(
            [
                T.StructField(name="Name", dataType=T.StringType()),
                T.StructField(name="Id", dataType=T.IntegerType()),
                T.StructField(name="Dept", dataType=T.StringType()),
            ]
        )

        df = spark.createDataFrame(data=data, schema=schema)

        # Verify column order matches schema
        assert df.columns == ["Name", "Id", "Dept"]

        backend = get_backend_type()
        if backend == BackendType.MOCK:
            # Verify data order matches schema order
            assert list(df.data[0].keys()) == ["Name", "Id", "Dept"]
            assert df.data[0] == {"Name": "Alice", "Id": 1, "Dept": "IT"}

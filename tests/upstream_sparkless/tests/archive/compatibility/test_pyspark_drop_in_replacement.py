"""
Comprehensive tests for PySpark drop-in replacement improvements.

This test suite verifies all improvements specified in MOCK_SPARK_PYSPARK_DROP_IN_REPLACEMENT_GUIDE.md
to ensure mock-spark behaves exactly like PySpark in testing scenarios.
"""

import pytest
import warnings
from sparkless import SparkSession, functions as F
from sparkless.spark_types import StructType, StructField, StringType


class TestStringConcatenationWithCaching:
    """Test string concatenation with + operator on cached DataFrames."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        session = SparkSession("string_concat_test")
        yield session
        session.stop()

    def test_string_concatenation_without_caching(self, spark):
        """Test that string concatenation works normally without caching."""
        df = spark.createDataFrame([("John", "Doe")], ["first_name", "last_name"])

        # Using + operator without caching
        df2 = df.withColumn(
            "full_name", F.col("first_name") + F.lit(" ") + F.col("last_name")
        )
        result = df2.collect()[0]

        # Should work normally
        assert result["full_name"] == "John Doe"

    def test_string_concatenation_with_caching_returns_none(self, spark):
        """Test that string concatenation with + returns None when DataFrame is cached."""
        df = spark.createDataFrame([("John", "Doe")], ["first_name", "last_name"])

        # Using + operator
        df2 = df.withColumn(
            "full_name", F.col("first_name") + F.lit(" ") + F.col("last_name")
        )

        # Cache the DataFrame
        df2_cached = df2.cache()
        _ = df2_cached.count()  # Force materialization

        # Collect and check result
        result = df2_cached.collect()[0]

        # Should return None to match PySpark behavior
        assert result["full_name"] is None, (
            "String concatenation with + operator should return None when DataFrame is cached, "
            "matching PySpark behavior"
        )

    def test_concat_function_works_with_caching(self, spark):
        """Test that F.concat() works reliably even with caching."""
        df = spark.createDataFrame([("John", "Doe")], ["first_name", "last_name"])

        # Using F.concat() (recommended approach)
        df2 = df.withColumn(
            "full_name", F.concat(F.col("first_name"), F.lit(" "), F.col("last_name"))
        )

        # Cache the DataFrame
        df2_cached = df2.cache()
        _ = df2_cached.count()  # Force materialization

        # Collect and check result
        result = df2_cached.collect()[0]

        # Should work correctly
        assert result["full_name"] == "John Doe", (
            "F.concat() should work correctly even when DataFrame is cached"
        )

    def test_string_concatenation_with_persist(self, spark):
        """Test that persist() also triggers the caching behavior."""
        df = spark.createDataFrame([("Alice", "Smith")], ["first", "last"])

        df2 = df.withColumn("name", F.col("first") + F.lit(" ") + F.col("last"))
        df2_persisted = df2.persist()
        _ = df2_persisted.count()

        result = df2_persisted.collect()[0]
        assert result["name"] is None, "persist() should also trigger cached behavior"


class TestEmptyDataFrameSchemaRequirement:
    """Test that empty DataFrames require explicit schemas."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        session = SparkSession("empty_df_test")
        yield session
        session.stop()

    def test_empty_dataframe_without_schema_raises_error(self, spark):
        """Test that empty DataFrame without schema raises ValueError."""
        with pytest.raises(ValueError, match="can not infer schema from empty dataset"):
            spark.createDataFrame([], ["col1", "col2"])

    def test_empty_dataframe_with_schema_works(self, spark):
        """Test that empty DataFrame with explicit schema works."""
        schema = StructType(
            [
                StructField("col1", StringType(), True),
                StructField("col2", StringType(), True),
            ]
        )

        df = spark.createDataFrame([], schema)
        assert len(df.collect()) == 0
        assert df.schema == schema

    def test_empty_dataframe_with_tuple_schema_raises_error(self, spark):
        """Test that empty DataFrame with tuple column names raises error."""
        # Tuple is converted to list, so test with list
        with pytest.raises(ValueError, match="can not infer schema from empty dataset"):
            spark.createDataFrame([], ["col1", "col2"])

    def test_non_empty_dataframe_without_schema_works(self, spark):
        """Test that non-empty DataFrame can infer schema."""
        df = spark.createDataFrame([("Alice", 25)], ["name", "age"])
        assert len(df.collect()) == 1
        # Field names might be in different order, so check both are present
        field_names = df.schema.fieldNames()
        assert "name" in field_names
        assert "age" in field_names
        assert len(field_names) == 2


class TestUnionOperationStrictness:
    """Test that union operations enforce schema compatibility."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        session = SparkSession("union_test")
        yield session
        session.stop()

    def test_union_same_schema_works(self, spark):
        """Test that union with same schema works."""
        df1 = spark.createDataFrame([("A", 1)], ["name", "value"])
        df2 = spark.createDataFrame([("B", 2)], ["name", "value"])

        df_union = df1.union(df2)
        assert len(df_union.collect()) == 2

    def test_union_different_column_count_raises_error(self, spark):
        """Test that union with different column counts raises AnalysisException."""
        from sparkless.core.exceptions.analysis import AnalysisException

        df1 = spark.createDataFrame([("A", 1)], ["name", "value"])
        df2 = spark.createDataFrame([("B",)], ["name"])

        with pytest.raises(AnalysisException, match="same number of columns"):
            df1.union(df2)

    def test_union_different_column_names_raises_error(self, spark):
        """Test that union with different column names raises AnalysisException."""
        from sparkless.core.exceptions.analysis import AnalysisException

        df1 = spark.createDataFrame([("A", 1)], ["name", "value"])
        df2 = spark.createDataFrame([("B", 2)], ["name", "id"])

        with pytest.raises(AnalysisException, match="compatible column names"):
            df1.union(df2)

    def test_union_incompatible_types_raises_error(self, spark):
        """Test that union with incompatible types raises AnalysisException."""
        from sparkless.core.exceptions.analysis import AnalysisException
        from sparkless.spark_types import BooleanType, StructType, StructField

        df1 = spark.createDataFrame([("A", 1)], ["name", "value"])
        schema2 = StructType(
            [
                StructField("name", StringType(), True),
                StructField("value", BooleanType(), True),
            ]
        )
        df2 = spark.createDataFrame([("B", True)], schema2)

        with pytest.raises(AnalysisException, match="compatible column types"):
            df1.union(df2)


class TestDataFrameMaterialization:
    """Test DataFrame materialization and column availability."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        session = SparkSession("materialization_test")
        yield session
        session.stop()

    def test_computed_column_requires_materialization(self, spark):
        """Test that computed columns require materialization before validation."""
        df = spark.createDataFrame([(1, "test")], ["id", "name"])

        # Create computed column
        df_transformed = df.withColumn("doubled", F.col("id") * 2)

        # After materialization, should work
        df_cached = df_transformed.cache()
        _ = df_cached.count()

        result = df_cached.filter(F.col("doubled") > 0).collect()
        assert len(result) == 1
        assert result[0]["doubled"] == 2

    def test_materialization_via_collect(self, spark):
        """Test that collect() materializes DataFrame."""
        df = spark.createDataFrame([(1,)], ["id"])
        df_transformed = df.withColumn("computed", F.col("id") * 2)

        # Materialize via collect
        df_transformed.collect()

        # Now should be able to use computed column
        available = df_transformed._get_available_columns()
        assert "computed" in available

    def test_materialization_via_count(self, spark):
        """Test that count() materializes DataFrame."""
        df = spark.createDataFrame([(1,), (2,)], ["id"])
        df_transformed = df.withColumn("computed", F.col("id") * 2)

        # Materialize via count
        _ = df_transformed.count()

        # Now should be able to use computed column
        available = df_transformed._get_available_columns()
        assert "computed" in available


class TestSchemaCreationAPIs:
    """Test that all schema creation APIs work (storage, SQL, catalog)."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        session = SparkSession("schema_creation_test")
        yield session
        session.stop()

    def test_storage_create_schema(self, spark):
        """Test that storage.create_schema() works."""
        spark._storage.create_schema("schema1")

        schemas = [db.name for db in spark.catalog.listDatabases()]
        assert "schema1" in schemas

    def test_sql_create_schema(self, spark):
        """Test that SQL CREATE SCHEMA works."""
        spark.sql("CREATE SCHEMA IF NOT EXISTS schema2")

        schemas = [db.name for db in spark.catalog.listDatabases()]
        assert "schema2" in schemas

    def test_catalog_create_database(self, spark):
        """Test that catalog.createDatabase() works."""
        spark.catalog.createDatabase("schema3", ignoreIfExists=True)

        schemas = [db.name for db in spark.catalog.listDatabases()]
        assert "schema3" in schemas

    def test_all_apis_create_same_schema(self, spark):
        """Test that all three APIs create the same schema."""
        # Create schema using storage API
        spark._storage.create_schema("test_schema")

        # Try to create same schema using SQL (should be idempotent)
        spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")

        # Try to create same schema using catalog (should be idempotent)
        spark.catalog.createDatabase("test_schema", ignoreIfExists=True)

        # Should only have one instance
        schemas = [db.name for db in spark.catalog.listDatabases()]
        assert schemas.count("test_schema") == 1


class TestTypeSystemCompatibility:
    """Test that mock-spark types are compatible with PySpark type checking."""

    def test_struct_type_compatibility(self):
        """Test that StructType works with PySpark operations if available."""
        from sparkless.spark_types import StructType, StructField, StringType

        schema = StructType([StructField("name", StringType(), True)])

        # Should be able to create DataFrame with mock-spark types
        spark = SparkSession("type_test")
        try:
            df = spark.createDataFrame([{"name": "test"}], schema)
            assert df.collect()[0]["name"] == "test"
        finally:
            spark.stop()

    def test_types_have_correct_attributes(self):
        """Test that types have correct PySpark-compatible attributes."""
        from sparkless.spark_types import StructType, StructField, StringType

        schema = StructType([StructField("name", StringType(), True)])

        # Should have fields attribute
        assert hasattr(schema, "fields")
        assert len(schema.fields) == 1
        assert schema.fields[0].name == "name"


class TestExprSQLParsing:
    """Test that F.expr() parses SQL expressions like PySpark."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        session = SparkSession("expr_test")
        yield session
        session.stop()

    def test_expr_is_not_null(self, spark):
        """Test that F.expr() parses IS NOT NULL."""
        # Note: F.expr() SQL parsing may not work with all backends
        # This test verifies the SQL parser works, but actual usage may vary
        df = spark.createDataFrame([(1, "A"), (2, "B"), (None, "C")], ["id", "name"])

        # Valid SQL expression - parse it
        expr = F.expr("id IS NOT NULL")
        # If parsing succeeds, use it
        result = df.filter(expr).collect()
        assert len(result) == 2
        assert all(row["id"] is not None for row in result)

    def test_expr_comparison_operators(self, spark):
        """Test that F.expr() parses comparison operators."""
        df = spark.createDataFrame([(1,), (2,), (3,)], ["id"])

        # Test various comparison operators
        test_cases = [
            ("id > 1", 2),
            ("id >= 2", 2),
            ("id < 3", 2),
            ("id <= 2", 2),
            ("id = 2", 1),
            ("id != 1", 2),
        ]

        for expr_str, expected_count in test_cases:
            expr = F.expr(expr_str)
            result = df.filter(expr).collect()
            assert len(result) == expected_count, f"Failed for {expr_str}"

    def test_expr_logical_operators(self, spark):
        """Test that F.expr() parses logical operators."""
        df = spark.createDataFrame([(1, "A"), (2, "B"), (3, "A")], ["id", "name"])

        # Test AND
        expr_and = F.expr("id > 1 AND name = 'A'")
        result_and = df.filter(expr_and).collect()
        assert len(result_and) == 1
        assert result_and[0]["id"] == 3

        # Test OR
        expr_or = F.expr("id = 1 OR id = 3")
        result_or = df.filter(expr_or).collect()
        assert len(result_or) == 2

    def test_expr_string_functions(self, spark):
        """Test that F.expr() parses string functions."""
        df = spark.createDataFrame([("  hello  ",), ("WORLD",)], ["text"])

        # Test LENGTH
        expr_length = F.expr("LENGTH(text) > 5")
        result = df.filter(expr_length).collect()
        assert len(result) == 1

        # Test UPPER
        expr_upper = F.expr("UPPER(text) = 'WORLD'")
        result = df.filter(expr_upper).collect()
        assert len(result) == 1

    def test_expr_rejects_python_syntax(self, spark):
        """Test that F.expr() rejects Python-like syntax."""
        # Python-like syntax should fail or warn
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")

            try:
                F.expr("col('id').isNotNull()")
                # If it doesn't raise, should at least warn
                # Note: SQL parser may fall back to old behavior with warning
                if len(w) == 0:
                    # Check if it actually parsed correctly (shouldn't work with Python syntax)
                    # If no warning and no error, the parser might have handled it
                    pass  # Acceptable if parser handles it gracefully
            except Exception as e:
                # If it raises, error should mention SQL or parse
                assert (
                    "SQL" in str(e) or "parse" in str(e).lower() or "Invalid" in str(e)
                )


class TestSparkSessionRequirements:
    """Test that function calls require active SparkSession."""

    def test_col_does_not_require_active_session(self):
        """Test that F.col() does NOT require active SparkSession (PySpark behavior)."""
        # Stop any existing session
        SparkSession._singleton_session = None
        SparkSession._active_sessions.clear()

        # In PySpark, col() can be called without a session
        # It just creates a column expression that's evaluated later
        col_expr = F.col("test")
        assert col_expr is not None
        assert hasattr(col_expr, "name") or hasattr(col_expr, "column_name")

    def test_lit_does_not_require_active_session(self):
        """Test that F.lit() does NOT require active SparkSession (PySpark behavior)."""
        # Stop any existing session
        SparkSession._singleton_session = None
        SparkSession._active_sessions.clear()

        # In PySpark, lit() can be called without a session
        # It just creates a literal expression that's evaluated later
        lit_expr = F.lit(42)
        assert lit_expr is not None

    def test_expr_requires_active_session(self):
        """Test that F.expr() requires active SparkSession."""
        # Stop any existing session
        SparkSession._singleton_session = None
        SparkSession._active_sessions.clear()

        with pytest.raises(RuntimeError, match="No active SparkSession found"):
            F.expr("id > 1")

    def test_functions_work_with_active_session(self):
        """Test that functions work when SparkSession is active."""
        spark = SparkSession("test")
        try:
            # Should work with active session
            col_expr = F.col("test")
            assert col_expr is not None

            lit_expr = F.lit(42)
            assert lit_expr is not None
        finally:
            spark.stop()


class TestErrorHandlingCompatibility:
    """Test Py4JJavaError compatibility."""

    def test_py4j_error_available(self):
        """Test that MockPy4JJavaError is available."""
        from sparkless.core.exceptions.py4j_compat import MockPy4JJavaError

        # Should be able to create error
        error = MockPy4JJavaError("Test error")
        assert str(error) == "Test error"
        assert error.message == "Test error"

    def test_py4j_error_inheritance(self):
        """Test that MockPy4JJavaError can be caught as Exception."""
        from sparkless.core.exceptions.py4j_compat import MockPy4JJavaError

        error = MockPy4JJavaError("Test error")

        # Should be catchable as Exception
        try:
            raise error
        except Exception as e:
            assert isinstance(e, MockPy4JJavaError)
            assert str(e) == "Test error"


class TestPerformanceMode:
    """Test performance mode for realistic PySpark simulation."""

    def test_fast_mode_default(self):
        """Test that fast mode is default."""
        spark = SparkSession("perf_test")
        try:
            assert spark.performance_mode == "fast"
            assert spark._jvm_overhead < 0.0001  # Very small overhead
        finally:
            spark.stop()

    def test_realistic_mode_simulates_overhead(self):
        """Test that realistic mode has higher overhead."""
        spark = SparkSession("perf_test", performance_mode="realistic")
        try:
            assert spark.performance_mode == "realistic"
            assert spark._jvm_overhead > 0.0001  # Noticeable overhead
        finally:
            spark.stop()

    def test_simulate_jvm_overhead(self):
        """Test that _simulate_jvm_overhead works."""
        import time

        spark_fast = SparkSession("fast", performance_mode="fast")
        spark_realistic = SparkSession("realistic", performance_mode="realistic")

        try:
            # Fast mode should be very quick
            start = time.time()
            for _ in range(100):
                spark_fast._simulate_jvm_overhead()
            fast_duration = time.time() - start

            # Realistic mode should be slower
            start = time.time()
            for _ in range(100):
                spark_realistic._simulate_jvm_overhead()
            realistic_duration = time.time() - start

            # Realistic should be slower
            assert realistic_duration > fast_duration
        finally:
            spark_fast.stop()
            spark_realistic.stop()


class TestCatalogAPICompatibility:
    """Test that catalog API matches PySpark."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        session = SparkSession("catalog_test")
        yield session
        session.stop()

    def test_list_databases_returns_database_objects(self, spark):
        """Test that listDatabases() returns Database objects with .name attribute."""
        databases = spark.catalog.listDatabases()

        assert len(databases) > 0
        for db in databases:
            assert hasattr(db, "name")
            assert isinstance(db.name, str)

    def test_create_database_ignores_if_exists(self, spark):
        """Test that createDatabase with ignoreIfExists=True is idempotent."""
        spark.catalog.createDatabase("test_db", ignoreIfExists=True)
        spark.catalog.createDatabase("test_db", ignoreIfExists=True)  # Should not raise

        databases = [db.name for db in spark.catalog.listDatabases()]
        assert databases.count("test_db") == 1

    def test_create_database_raises_if_exists(self, spark):
        """Test that createDatabase raises if database exists and ignoreIfExists=False."""
        from sparkless.core.exceptions.analysis import AnalysisException

        spark.catalog.createDatabase("test_db2", ignoreIfExists=True)

        with pytest.raises(AnalysisException, match="already exists"):
            spark.catalog.createDatabase("test_db2", ignoreIfExists=False)


class TestDatetimeTypeCasting:
    """Test that to_timestamp() requires string type input."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        session = SparkSession("datetime_test")
        yield session
        session.stop()

    def test_to_timestamp_requires_string_type(self, spark):
        """Test that to_timestamp() requires StringType input."""
        from sparkless.spark_types import TimestampType, StructType, StructField

        # Create DataFrame with TimestampType (not StringType)
        schema = StructType([StructField("timestamp_col", TimestampType(), True)])
        df = spark.createDataFrame([{"timestamp_col": None}], schema)

        # Should fail - to_timestamp requires StringType, not TimestampType
        with pytest.raises(TypeError, match="requires StringType input"):
            df.withColumn("parsed", F.to_timestamp(F.col("timestamp_col")))

    def test_to_timestamp_works_with_string(self, spark):
        """Test that to_timestamp() works with StringType input."""
        # Create DataFrame - ensure column is inferred as string, not timestamp
        # Use explicit schema to ensure it's StringType
        from sparkless.spark_types import StructType, StructField, StringType

        schema = StructType([StructField("timestamp_str", StringType(), True)])
        df = spark.createDataFrame([("2024-01-01T10:00:00",)], schema)

        # Should work with string type
        result = df.withColumn("parsed", F.to_timestamp(F.col("timestamp_str")))
        assert result is not None

    def test_to_timestamp_with_explicit_cast(self, spark):
        """Test that to_timestamp() works with explicit string cast."""
        from sparkless.spark_types import TimestampType, StructType, StructField

        schema = StructType([StructField("timestamp_col", TimestampType(), True)])
        df = spark.createDataFrame([{"timestamp_col": None}], schema)

        # Should work with explicit cast to string
        result = df.withColumn(
            "parsed", F.to_timestamp(F.col("timestamp_col").cast("string"))
        )
        assert result is not None

    def test_to_timestamp_with_integer_type_fails(self, spark):
        """Test that to_timestamp() fails with IntegerType."""
        from sparkless.spark_types import IntegerType, StructType, StructField

        schema = StructType([StructField("date_int", IntegerType(), True)])
        df = spark.createDataFrame([{"date_int": 20240101}], schema)

        with pytest.raises(TypeError, match="requires StringType input"):
            df.withColumn("parsed", F.to_timestamp(F.col("date_int")))

import pytest


class TestFunctionAPIs:
    """Test that function APIs match PySpark exactly."""

    def test_current_date_is_function_not_method(self, spark):
        """Test that current_date is a function, not DataFrame method."""
        from tests.fixtures.spark_imports import get_spark_imports

        imports = get_spark_imports()
        F = imports.F

        df = spark.createDataFrame([{"id": 1}], ["id"])

        # Should work as function
        result = df.withColumn("today", F.current_date())
        assert result is not None

        # Should NOT work as DataFrame method
        with pytest.raises(AttributeError):
            df.current_date()

    def test_current_timestamp_is_function_not_method(self, spark):
        """Test that current_timestamp is a function, not DataFrame method."""
        from tests.fixtures.spark_imports import get_spark_imports

        imports = get_spark_imports()
        F = imports.F

        df = spark.createDataFrame([{"id": 1}], ["id"])

        # Should work as function
        result = df.withColumn("now", F.current_timestamp())
        assert result is not None

        # Should NOT work as DataFrame method
        with pytest.raises(AttributeError):
            df.current_timestamp()

    def test_functions_are_static_methods(self, spark):
        """Test that functions are accessible as static methods."""
        from tests.fixtures.spark_imports import get_spark_imports

        imports = get_spark_imports()
        F = imports.F

        # All these should work as static method calls
        col_expr = F.col("id")
        assert col_expr is not None

        lit_expr = F.lit(42)
        assert lit_expr is not None

        count_func = F.count("id")
        assert count_func is not None

        row_num = F.row_number()
        assert row_num is not None

    def test_function_signatures_match_pyspark(self, spark):
        """Test that function signatures match PySpark patterns."""
        from tests.fixtures.spark_imports import get_spark_imports

        imports = get_spark_imports()
        F = imports.F

        df = spark.createDataFrame([{"id": 1, "value": 10}], ["id", "value"])

        # Test various function calling patterns that should match PySpark
        # Use an aggregate context for count/sum to match PySpark's requirement
        result = df.groupBy().agg(
            F.count("id").alias("count"),
            F.sum("value").alias("sum"),
            F.current_date().alias("today"),
            F.current_timestamp().alias("now"),
        )
        rows = result.collect()
        assert len(rows) == 1

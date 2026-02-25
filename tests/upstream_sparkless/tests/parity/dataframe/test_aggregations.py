"""
PySpark parity tests for aggregation operations.

Tests validate that Sparkless aggregation operations behave identically to PySpark.
"""

from tests.fixtures.parity_base import ParityTestBase
from tests.fixtures.spark_imports import get_spark_imports


class TestAggregationsParity(ParityTestBase):
    """Test aggregation operations parity with PySpark."""

    def test_sum_aggregation(self, spark):
        """Test sum aggregation matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("aggregations", "sum_aggregation")
        df = spark.createDataFrame(expected["input_data"])
        result = df.groupBy("department").agg(F.sum(df.salary).alias("total_salary"))
        self.assert_parity(result, expected)

    def test_avg_aggregation(self, spark):
        """Test avg aggregation matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("aggregations", "avg_aggregation")
        df = spark.createDataFrame(expected["input_data"])
        result = df.groupBy("department").agg(F.avg(df.salary).alias("avg_salary"))
        self.assert_parity(result, expected)

    def test_count_aggregation(self, spark):
        """Test count aggregation matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("aggregations", "count_aggregation")
        df = spark.createDataFrame(expected["input_data"])
        result = df.groupBy("department").agg(F.count(df.id).alias("employee_count"))
        self.assert_parity(result, expected)

    def test_max_aggregation(self, spark):
        """Test max aggregation matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("aggregations", "max_aggregation")
        df = spark.createDataFrame(expected["input_data"])
        result = df.groupBy("department").agg(F.max(df.salary).alias("max_salary"))
        self.assert_parity(result, expected)

    def test_min_aggregation(self, spark):
        """Test min aggregation matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("aggregations", "min_aggregation")
        df = spark.createDataFrame(expected["input_data"])
        result = df.groupBy("department").agg(F.min(df.salary).alias("min_salary"))
        self.assert_parity(result, expected)

    def test_multiple_aggregations(self, spark):
        """Test multiple aggregations match PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("aggregations", "multiple_aggregations")
        df = spark.createDataFrame(expected["input_data"])
        result = df.groupBy("department").agg(
            F.sum(df.salary).alias("total_salary"),
            F.avg(df.salary).alias("avg_salary"),
            F.count(df.id).alias("employee_count"),
            F.max(df.age).alias("max_age"),
        )
        self.assert_parity(result, expected)

    def test_groupby_multiple_columns(self, spark):
        """Test groupBy with multiple columns matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("aggregations", "groupby_multiple_columns")
        df = spark.createDataFrame(expected["input_data"])
        result = df.groupBy("department", "level").agg(
            F.avg(df.salary).alias("avg_salary")
        )
        self.assert_parity(result, expected)

    def test_global_aggregation(self, spark):
        """Test global aggregation matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("aggregations", "global_aggregation")
        df = spark.createDataFrame(expected["input_data"])
        result = df.agg(
            F.sum(df.salary).alias("total_salary"),
            F.avg(df.salary).alias("avg_salary"),
            F.count(df.id).alias("total_employees"),
        )
        self.assert_parity(result, expected)

    def test_aggregation_with_nulls(self, spark):
        """Test aggregation with nulls matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("aggregations", "aggregation_with_nulls")
        df = spark.createDataFrame(expected["input_data"])
        result = df.groupBy("department").agg(F.avg(df.salary).alias("avg_salary"))
        self.assert_parity(result, expected)

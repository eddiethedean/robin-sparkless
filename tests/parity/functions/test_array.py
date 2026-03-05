"""
PySpark parity tests for array functions.

Tests validate that Sparkless array functions behave identically to PySpark.
"""

from tests.fixtures.parity_base import ParityTestBase
from tests.fixtures.spark_imports import get_spark_imports


class TestArrayFunctionsParity(ParityTestBase):
    """Test array function parity with PySpark."""

    def test_array_contains(self, spark):
        """Test array_contains function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("arrays", "array_contains")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.array_contains(df.scores, 90))
        self.assert_parity(result, expected)

    def test_array_position(self, spark):
        """Test array_position function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("arrays", "array_position")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.array_position(df.scores, 90))
        self.assert_parity(result, expected)

    def test_size(self, spark):
        """Test size function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("arrays", "size")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.size(df.scores))
        self.assert_parity(result, expected)

    def test_element_at(self, spark):
        """Test element_at function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("arrays", "element_at")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.element_at(df.scores, 2))
        self.assert_parity(result, expected)

    def test_explode(self, spark):
        """Test explode function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("arrays", "explode")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(df.name, F.explode(df.scores).alias("score"))
        self.assert_parity(result, expected)

    def test_array_distinct(self, spark):
        """Test array_distinct function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("arrays", "array_distinct")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.array_distinct(df.tags))
        # array_distinct doesn't guarantee order of elements within arrays
        # PySpark may return arrays in different order than expected
        # The expected output has been updated with sorted arrays, so we need to sort our result too
        # Use alias to avoid column name conflict
        result = result.select(
            F.array_sort(F.col("array_distinct(tags)")).alias("array_distinct(tags)")
        )
        self.assert_parity(result, expected)

    def test_array_join(self, spark):
        """Test array_join function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("arrays", "array_join")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.array_join(df.arr1, "-"))
        self.assert_parity(result, expected)

    def test_array_union(self, spark):
        """Test array_union function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("arrays", "array_union")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.array_union(df.arr1, df.arr2))
        self.assert_parity(result, expected)

    def test_array_sort(self, spark):
        """Test array_sort function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("arrays", "array_sort")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.array_sort(df.arr3))
        self.assert_parity(result, expected)

    def test_array_remove(self, spark):
        """Test array_remove function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("arrays", "array_remove")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.array_remove(df.scores, 90))
        self.assert_parity(result, expected)

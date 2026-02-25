"""
PySpark parity tests for null handling functions.

Tests validate that Sparkless null handling functions behave identically to PySpark.
"""

from tests.fixtures.parity_base import ParityTestBase
from tests.fixtures.spark_imports import get_spark_imports


class TestNullHandlingFunctionsParity(ParityTestBase):
    """Test null handling function parity with PySpark."""

    def test_coalesce(self, spark):
        """Test coalesce function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("null_handling", "coalesce")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.coalesce(df.salary, F.lit(0)))
        self.assert_parity(result, expected)

    def test_isnull(self, spark):
        """Test isnull function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("null_handling", "isnull")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.isnull(df.name))
        self.assert_parity(result, expected)

    def test_isnotnull(self, spark):
        """Test isnotnull function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("null_handling", "isnotnull")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.isnotnull(df.name))
        self.assert_parity(result, expected)

    def test_when_otherwise(self, spark):
        """Test when/otherwise function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("null_handling", "when_otherwise")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.when(df.salary.isNull(), 0).otherwise(df.salary))
        self.assert_parity(result, expected)

    def test_nvl(self, spark):
        """Test nvl function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("null_handling", "nvl")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.nvl(df.salary, F.lit(0)))
        self.assert_parity(result, expected)

    def test_nullif(self, spark):
        """Test nullif function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("null_handling", "nullif")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.nullif(df.age, F.lit(30)))
        self.assert_parity(result, expected)

    def test_ifnull(self, spark):
        """Test ifnull function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("functions", "ifnull")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.when(df.salary.isNull(), 0).otherwise(df.salary))
        self.assert_parity(result, expected)

    def test_nanvl(self, spark):
        """Test nanvl function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("functions", "nanvl")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.nanvl(df.salary, F.lit(0)))
        self.assert_parity(result, expected)

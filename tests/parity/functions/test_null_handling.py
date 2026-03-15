"""
PySpark parity tests for null handling functions.

Tests validate that Sparkless null handling functions behave identically to PySpark.
"""

from tests.tools.parity_base import ParityTestBase
from sparkless.testing import get_imports


class TestNullHandlingFunctionsParity(ParityTestBase):
    """Test null handling function parity with PySpark."""

    def test_coalesce(self, spark):
        """Test coalesce function matches PySpark behavior."""
        imports = get_imports()
        F = imports.F
        expected = self.load_expected("null_handling", "coalesce")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.coalesce(df.salary, F.lit(0)))
        self.assert_parity(result, expected)

    def test_isnull(self, spark):
        """Test isnull function matches PySpark behavior."""
        imports = get_imports()
        F = imports.F
        expected = self.load_expected("null_handling", "isnull")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.isnull(df.name))
        self.assert_parity(result, expected)

    def test_isnotnull(self, spark):
        """Test isnotnull function matches PySpark behavior."""
        imports = get_imports()
        F = imports.F
        expected = self.load_expected("null_handling", "isnotnull")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.isnotnull(df.name))
        self.assert_parity(result, expected)

    def test_when_otherwise(self, spark):
        """Test when/otherwise function matches PySpark behavior."""
        imports = get_imports()
        F = imports.F
        expected = self.load_expected("null_handling", "when_otherwise")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.when(df.salary.isNull(), 0).otherwise(df.salary))
        self.assert_parity(result, expected)

    def test_nvl(self, spark):
        """Test nvl function matches PySpark behavior."""
        imports = get_imports()
        F = imports.F
        expected = self.load_expected("null_handling", "nvl")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.nvl(df.salary, F.lit(0)))
        self.assert_parity(result, expected)

    def test_nvl_column_replacement(self, spark):
        """nvl(col1, col2) uses value of col2 when col1 is null, not literal column name (#1476)."""
        imports = get_imports()
        F = imports.F

        df = spark.createDataFrame(
            [
                {"a": None, "b": "default"},
                {"a": "value", "b": "default"},
            ]
        )
        result = df.select(F.nvl("a", "b").alias("result"))
        rows = result.collect()
        assert len(rows) == 2
        assert rows[0]["result"] == "default"
        assert rows[1]["result"] == "value"

    def test_nullif(self, spark):
        """Test nullif function matches PySpark behavior."""
        imports = get_imports()
        F = imports.F
        expected = self.load_expected("null_handling", "nullif")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.nullif(df.age, F.lit(30)))
        self.assert_parity(result, expected)

    def test_ifnull(self, spark):
        """Test ifnull function matches PySpark behavior."""
        imports = get_imports()
        F = imports.F
        expected = self.load_expected("functions", "ifnull")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.when(df.salary.isNull(), 0).otherwise(df.salary))
        self.assert_parity(result, expected)

    def test_nanvl(self, spark):
        """Test nanvl function matches PySpark behavior."""
        imports = get_imports()
        F = imports.F
        expected = self.load_expected("functions", "nanvl")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.nanvl(df.salary, F.lit(0)))
        self.assert_parity(result, expected)

"""
PySpark parity tests for datetime functions.

Tests validate that Sparkless datetime functions behave identically to PySpark.
"""

from tests.fixtures.parity_base import ParityTestBase
from tests.fixtures.spark_imports import get_spark_imports


class TestDatetimeFunctionsParity(ParityTestBase):
    """Test datetime function parity with PySpark."""

    def test_year(self, spark):
        """Test year function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("datetime", "year")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.year(df.hire_date))
        self.assert_parity(result, expected)

    def test_month(self, spark):
        """Test month function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("datetime", "month")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.month(df.hire_date))
        self.assert_parity(result, expected)

    def test_dayofmonth(self, spark):
        """Test dayofmonth function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("datetime", "dayofmonth")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.dayofmonth(df.date))
        self.assert_parity(result, expected)

    def test_dayofweek(self, spark):
        """Test dayofweek function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("datetime", "dayofweek")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.dayofweek(df.hire_date))
        self.assert_parity(result, expected)

    def test_date_add(self, spark):
        """Test date_add function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("datetime", "date_add")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.date_add(df.hire_date, 30))
        self.assert_parity(result, expected)

    def test_date_sub(self, spark):
        """Test date_sub function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("datetime", "date_sub")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.date_sub(df.hire_date, 30))
        self.assert_parity(result, expected)

    def test_date_format(self, spark):
        """Test date_format function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("datetime", "date_format")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.date_format(df.hire_date, "yyyy-MM"))
        self.assert_parity(result, expected)

    def test_to_date(self, spark):
        """Test to_date function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("datetime", "to_date")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.to_date(df.hire_date))
        self.assert_parity(result, expected)

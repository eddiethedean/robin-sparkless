"""
PySpark parity tests for math functions.

Tests validate that Sparkless math functions behave identically to PySpark.
"""

from tests.fixtures.parity_base import ParityTestBase
from tests.fixtures.spark_imports import get_spark_imports


class TestMathFunctionsParity(ParityTestBase):
    """Test math function parity with PySpark."""

    def test_math_abs(self, spark):
        """Test abs function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("functions", "math_abs")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.abs(df.salary))
        self.assert_parity(result, expected)

    def test_math_round(self, spark):
        """Test round function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("functions", "math_round")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.round(df.salary, -3))
        self.assert_parity(result, expected)

    def test_math_sqrt(self, spark):
        """Test sqrt function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("functions", "math_sqrt")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.sqrt(df.salary))
        self.assert_parity(result, expected)

    def test_math_pow(self, spark):
        """Test pow function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("functions", "math_pow")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.pow(df.age, 2))
        self.assert_parity(result, expected)

    def test_math_log(self, spark):
        """Test log function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("functions", "math_log")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.log(df.salary))
        self.assert_parity(result, expected)

    def test_math_exp(self, spark):
        """Test exp function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("functions", "math_exp")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.exp(F.lit(1)))
        self.assert_parity(result, expected)

    def test_math_sin(self, spark):
        """Test sin function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("functions", "math_sin")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.sin(df.angle))
        self.assert_parity(result, expected)

    def test_math_cos(self, spark):
        """Test cos function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("functions", "math_cos")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.cos(df.angle))
        self.assert_parity(result, expected)

    def test_math_tan(self, spark):
        """Test tan function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("functions", "math_tan")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.tan(df.angle))
        self.assert_parity(result, expected)

    def test_math_ceil(self, spark):
        """Test ceil function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("functions", "math_ceil")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.ceil(df.value))
        self.assert_parity(result, expected)

    def test_math_floor(self, spark):
        """Test floor function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("functions", "math_floor")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.floor(df.value))
        self.assert_parity(result, expected)

    def test_math_greatest(self, spark):
        """Test greatest function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("functions", "math_greatest")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.greatest(df.a, df.b, df.c))
        self.assert_parity(result, expected)

    def test_math_least(self, spark):
        """Test least function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        expected = self.load_expected("functions", "math_least")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.least(df.a, df.b, df.c))
        self.assert_parity(result, expected)

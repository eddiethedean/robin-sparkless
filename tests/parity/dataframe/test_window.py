"""
PySpark parity tests for DataFrame window operations.

Tests validate that Sparkless window operations behave identically to PySpark.
"""

from tests.fixtures.parity_base import ParityTestBase
from tests.fixtures.spark_imports import get_spark_imports
from tests.fixtures.spark_backend import get_backend_type, BackendType


def _is_pyspark_mode() -> bool:
    """Check if running in PySpark mode."""
    backend = get_backend_type()
    return backend == BackendType.PYSPARK


class TestWindowOperationsParity(ParityTestBase):
    """Test DataFrame window operations parity with PySpark."""

    def test_row_number(self, spark):
        """Test row_number window function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        Window = imports.Window
        expected = self.load_expected("windows", "row_number")

        df = spark.createDataFrame(expected["input_data"])
        window_spec = Window.partitionBy("dept").orderBy("salary")
        result = df.withColumn("row_num", F.row_number().over(window_spec))

        self.assert_parity(result, expected)

    def test_rank(self, spark):
        """Test rank window function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        Window = imports.Window
        expected = self.load_expected("windows", "rank")

        df = spark.createDataFrame(expected["input_data"])
        window_spec = Window.partitionBy("dept").orderBy("salary")
        result = df.withColumn("rank", F.rank().over(window_spec))

        self.assert_parity(result, expected)

    def test_dense_rank(self, spark):
        """Test dense_rank window function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        Window = imports.Window
        expected = self.load_expected("windows", "dense_rank")

        df = spark.createDataFrame(expected["input_data"])
        window_spec = Window.partitionBy("dept").orderBy("salary")
        result = df.withColumn("dense_rank", F.dense_rank().over(window_spec))

        self.assert_parity(result, expected)

    def test_sum_over_window(self, spark):
        """Test sum over window matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        Window = imports.Window
        expected = self.load_expected("windows", "sum_over_window")

        df = spark.createDataFrame(expected["input_data"])
        # Match PySpark fixture: total salary per department, repeated on each
        # row for that department (not a running total).
        window_spec = Window.partitionBy("department")
        result = df.withColumn("dept_total", F.sum("salary").over(window_spec))

        self.assert_parity(result, expected)

    def test_lag(self, spark):
        """Test lag window function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        Window = imports.Window
        expected = self.load_expected("windows", "lag")

        df = spark.createDataFrame(expected["input_data"])
        window_spec = Window.partitionBy("dept").orderBy("salary")
        result = df.withColumn("lag_salary", F.lag("salary", 1).over(window_spec))

        self.assert_parity(result, expected)

    def test_lead(self, spark):
        """Test lead window function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        Window = imports.Window
        expected = self.load_expected("windows", "lead")

        df = spark.createDataFrame(expected["input_data"])
        window_spec = Window.partitionBy("dept").orderBy("salary")
        result = df.withColumn("lead_salary", F.lead("salary", 1).over(window_spec))

        self.assert_parity(result, expected)

    def test_cume_dist(self, spark):
        """Test cume_dist window function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        Window = imports.Window
        expected = self.load_expected("windows", "cume_dist")

        df = spark.createDataFrame(expected["input_data"])
        window_spec = Window.partitionBy("dept").orderBy("salary")
        result = df.withColumn("cume_dist", F.cume_dist().over(window_spec))

        self.assert_parity(result, expected)

    def test_first_value(self, spark):
        """Test first_value window function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        Window = imports.Window
        expected = self.load_expected("windows", "first_value")

        df = spark.createDataFrame(expected["input_data"])
        window_spec = Window.partitionBy("dept").orderBy("salary")
        result = df.withColumn("first_salary", F.first("salary").over(window_spec))

        self.assert_parity(result, expected)

    def test_last_value(self, spark):
        """Test last_value window function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        Window = imports.Window
        expected = self.load_expected("windows", "last_value")

        df = spark.createDataFrame(expected["input_data"])
        window_spec = Window.partitionBy("dept").orderBy("salary")
        result = df.withColumn("last_salary", F.last("salary").over(window_spec))

        self.assert_parity(result, expected)

    def test_percent_rank(self, spark):
        """Test percent_rank window function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        Window = imports.Window
        expected = self.load_expected("windows", "percent_rank")

        df = spark.createDataFrame(expected["input_data"])
        window_spec = Window.partitionBy("dept").orderBy("salary")
        result = df.withColumn("percent_rank", F.percent_rank().over(window_spec))

        self.assert_parity(result, expected)

    def test_ntile(self, spark):
        """Test ntile window function matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        Window = imports.Window
        expected = self.load_expected("windows", "ntile")

        df = spark.createDataFrame(expected["input_data"])
        window_spec = Window.partitionBy("dept").orderBy("salary")
        result = df.withColumn("ntile", F.ntile(2).over(window_spec))

        self.assert_parity(result, expected)

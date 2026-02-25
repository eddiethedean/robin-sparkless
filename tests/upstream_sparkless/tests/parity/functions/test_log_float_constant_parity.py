"""PySpark parity tests for log() function with float constants.

These tests ensure that Sparkless's log() function behavior matches PySpark exactly.
"""

from tests.fixtures.spark_imports import get_spark_imports


class TestLogFloatConstantParity:
    """PySpark parity tests for log() with float constants."""

    def test_log_with_float_base_parity(self, spark):
        """Test log with float constant as base matches PySpark."""
        imports = get_spark_imports()
        F = imports.F

        df = spark.createDataFrame(
            [
                {"Value": 100.0},
                {"Value": 1000.0},
            ]
        )

        # PySpark signature: log(base, column)
        result = df.select(
            "Value",
            F.log(10.0, F.col("Value")).alias("Log10"),
        )

        rows = result.collect()
        assert len(rows) == 2

        # log10(100) = 2.0, log10(1000) = 3.0
        row1 = [r for r in rows if r["Value"] == 100.0][0]
        row2 = [r for r in rows if r["Value"] == 1000.0][0]

        assert abs(row1["Log10"] - 2.0) < 0.0001
        assert abs(row2["Log10"] - 3.0) < 0.0001

    def test_log_natural_log_parity(self, spark):
        """Test natural log matches PySpark."""
        import math

        imports = get_spark_imports()
        F = imports.F

        df = spark.createDataFrame(
            [
                {"Value": math.e},
            ]
        )

        # Natural log: log(column)
        result = df.select(F.log(F.col("Value")).alias("Ln"))

        rows = result.collect()
        assert len(rows) == 1
        # ln(e) = 1.0
        assert abs(rows[0]["Ln"] - 1.0) < 0.0001

    def test_log_with_different_bases_parity(self, spark):
        """Test log with different bases matches PySpark."""
        imports = get_spark_imports()
        F = imports.F

        df = spark.createDataFrame(
            [
                {"Value": 100.0},
            ]
        )

        result = df.select(
            F.log(10.0, F.col("Value")).alias("Log10"),
            F.log(2.0, F.col("Value")).alias("Log2"),
        )

        rows = result.collect()
        assert len(rows) == 1
        row = rows[0]

        # log10(100) = 2.0
        assert abs(row["Log10"] - 2.0) < 0.0001
        # log2(100) ≈ 6.644
        assert abs(row["Log2"] - 6.644) < 0.01

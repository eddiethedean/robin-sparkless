"""
Parity tests for covar_pop/covar_samp with nulls (#1475).

PySpark excludes rows where either column is null before computing covariance.
"""

from tests.tools.parity_base import ParityTestBase
from sparkless.testing import get_imports


class TestCovarNullsParity(ParityTestBase):
    """Test covariance aggregates exclude null pairs like corr()."""

    def test_covar_pop_excludes_null_pairs(self, spark):
        imports = get_imports()
        F = imports.F

        df = spark.createDataFrame(
            [
                {"x": 1.0, "y": 2.0},
                {"x": 2.0, "y": 4.0},
                {"x": 3.0, "y": None},
                {"x": 4.0, "y": 8.0},
            ]
        )
        result = df.agg(F.covar_pop("x", "y").alias("cov"))
        rows = result.collect()
        assert len(rows) == 1
        cov_val = rows[0]["cov"]
        assert cov_val is not None
        # Population covariance on three non-null pairs (1,2),(2,4),(4,8)
        assert abs(cov_val - 28.0 / 9.0) < 1e-5, (
            f"Expected ~{28.0 / 9.0}, got {cov_val}"
        )

    def test_covar_samp_excludes_null_pairs(self, spark):
        imports = get_imports()
        F = imports.F

        df = spark.createDataFrame(
            [
                {"x": 1.0, "y": 2.0},
                {"x": 2.0, "y": 4.0},
                {"x": 3.0, "y": None},
                {"x": 4.0, "y": 8.0},
            ]
        )
        result = df.agg(F.covar_samp("x", "y").alias("cov"))
        rows = result.collect()
        assert len(rows) == 1
        cov_val = rows[0]["cov"]
        assert cov_val is not None
        # Sample covariance on three non-null pairs
        assert abs(cov_val - 14.0 / 3.0) < 1e-5, (
            f"Expected ~{14.0 / 3.0}, got {cov_val}"
        )

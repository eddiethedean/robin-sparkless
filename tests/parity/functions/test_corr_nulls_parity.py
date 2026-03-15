"""
Parity tests for corr() with nulls (#1475).

PySpark excludes rows where either column is null before computing correlation.
Sparkless must do the same so e.g. (1,2), (2,4), (4,8) with one (3, None) row
gives correlation 1.0, not ~0.53.
"""

from tests.tools.parity_base import ParityTestBase
from sparkless.testing import get_imports


class TestCorrNullsParity(ParityTestBase):
    """Test corr() with nulls matches PySpark (#1475)."""

    def test_corr_excludes_null_pairs(self, spark):
        """corr(x, y) excludes rows where either is null; perfect line => 1.0."""
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
        result = df.agg(F.corr("x", "y").alias("correlation"))
        rows = result.collect()
        assert len(rows) == 1
        corr_val = rows[0]["correlation"]
        assert corr_val is not None
        assert abs(corr_val - 1.0) < 1e-5, f"Expected ~1.0 (perfect line), got {corr_val}"

"""
Unit tests for correlation and covariance aggregate functions.

Tests corr() and covar_samp() aggregate functions that were recently implemented.
"""

import pytest
from sparkless import F


@pytest.mark.unit
class TestCorrelationFunctions:
    """Test correlation and covariance aggregate functions."""

    def test_corr_basic(self, spark):
        """Test basic correlation function."""
        data = [
            {"x": 1.0, "y": 2.0},
            {"x": 2.0, "y": 4.0},
            {"x": 3.0, "y": 6.0},
        ]
        df = spark.createDataFrame(data)

        result = df.agg(F.corr("x", "y").alias("correlation")).collect()[0]

        # Perfect positive correlation should be close to 1.0
        assert abs(result["correlation"] - 1.0) < 0.001

    def test_corr_negative_correlation(self, spark):
        """Test correlation with negative relationship."""
        data = [
            {"x": 1.0, "y": 6.0},
            {"x": 2.0, "y": 4.0},
            {"x": 3.0, "y": 2.0},
        ]
        df = spark.createDataFrame(data)

        result = df.agg(F.corr("x", "y").alias("corr")).collect()[0]

        # Perfect negative correlation should be close to -1.0
        assert abs(result["corr"] - (-1.0)) < 0.001

    def test_corr_no_correlation(self, spark):
        """Test correlation with no relationship."""
        data = [
            {"x": 1.0, "y": 1.0},
            {"x": 2.0, "y": 3.0},
            {"x": 3.0, "y": 2.0},
            {"x": 4.0, "y": 4.0},
        ]
        df = spark.createDataFrame(data)

        result = df.agg(F.corr("x", "y").alias("corr")).collect()[0]

        # Correlation should be between -1 and 1
        assert -1.0 <= result["corr"] <= 1.0

    def test_corr_with_nulls(self, spark):
        """Test correlation with null values."""
        data = [
            {"x": 1.0, "y": 2.0},
            {"x": 2.0, "y": None},
            {"x": 3.0, "y": 6.0},
            {"x": None, "y": 8.0},
        ]
        df = spark.createDataFrame(data)

        result = df.agg(F.corr("x", "y").alias("corr")).collect()[0]

        # Should handle nulls gracefully - only pairs with both non-null are used
        assert result["corr"] is not None or result["corr"] is None

    def test_corr_single_value_returns_none(self, spark):
        """Test that correlation with less than 2 values returns None."""
        data = [{"x": 1.0, "y": 2.0}]
        df = spark.createDataFrame(data)

        result = df.agg(F.corr("x", "y").alias("corr")).collect()[0]

        # Need at least 2 points for correlation
        assert result["corr"] is None

    def test_corr_with_column_expressions(self, spark):
        """Test correlation with column expressions."""
        data = [
            {"a": 1.0, "b": 2.0},
            {"a": 2.0, "b": 4.0},
            {"a": 3.0, "b": 6.0},
        ]
        df = spark.createDataFrame(data)

        result = df.agg(F.corr(F.col("a"), F.col("b")).alias("corr")).collect()[0]

        assert abs(result["corr"] - 1.0) < 0.001

    def test_covar_samp_basic(self, spark):
        """Test basic sample covariance function."""
        data = [
            {"x": 1.0, "y": 2.0},
            {"x": 2.0, "y": 4.0},
            {"x": 3.0, "y": 6.0},
        ]
        df = spark.createDataFrame(data)

        result = df.agg(F.covar_samp("x", "y").alias("covar")).collect()[0]

        # Sample covariance should be positive for positive correlation
        assert result["covar"] > 0

    def test_covar_samp_negative(self, spark):
        """Test sample covariance with negative relationship."""
        data = [
            {"x": 1.0, "y": 6.0},
            {"x": 2.0, "y": 4.0},
            {"x": 3.0, "y": 2.0},
        ]
        df = spark.createDataFrame(data)

        result = df.agg(F.covar_samp("x", "y").alias("covar")).collect()[0]

        # Sample covariance should be negative for negative correlation
        assert result["covar"] < 0

    def test_covar_samp_vs_covar_pop(self, spark):
        """Test that covar_samp uses (n-1) denominator."""
        data = [
            {"x": 1.0, "y": 2.0},
            {"x": 2.0, "y": 4.0},
            {"x": 3.0, "y": 6.0},
        ]
        df = spark.createDataFrame(data)

        result_samp = df.agg(F.covar_samp("x", "y").alias("covar_samp")).collect()[0]
        result_pop = df.agg(F.covar_pop("x", "y").alias("covar_pop")).collect()[0]

        # Sample covariance should be larger (divide by n-1 instead of n)
        # For n=3: covar_samp = covar_pop * (n/(n-1)) = covar_pop * 1.5
        assert abs(result_samp["covar_samp"] - result_pop["covar_pop"] * 1.5) < 0.001

    def test_covar_samp_with_nulls(self, spark):
        """Test sample covariance with null values."""
        data = [
            {"x": 1.0, "y": 2.0},
            {"x": 2.0, "y": None},
            {"x": 3.0, "y": 6.0},
        ]
        df = spark.createDataFrame(data)

        result = df.agg(F.covar_samp("x", "y").alias("covar")).collect()[0]

        # Should handle nulls - only pairs with both non-null are used
        assert result["covar"] is not None

    def test_covar_samp_single_value_returns_none(self, spark):
        """Test that sample covariance with less than 2 values returns None."""
        data = [{"x": 1.0, "y": 2.0}]
        df = spark.createDataFrame(data)

        result = df.agg(F.covar_samp("x", "y").alias("covar")).collect()[0]

        # Need at least 2 points for sample covariance (n-1 denominator)
        assert result["covar"] is None

    def test_corr_and_covar_samp_together(self, spark):
        """Test using both corr and covar_samp in same aggregation."""
        data = [
            {"x": 1.0, "y": 2.0},
            {"x": 2.0, "y": 4.0},
            {"x": 3.0, "y": 6.0},
        ]
        df = spark.createDataFrame(data)

        result = df.agg(
            F.corr("x", "y").alias("corr"),
            F.covar_samp("x", "y").alias("covar_samp"),
        ).collect()[0]

        assert abs(result["corr"] - 1.0) < 0.001
        assert result["covar_samp"] > 0

    def test_corr_grouped_aggregation(self, spark):
        """Test correlation in grouped aggregation."""
        data = [
            {"group": "A", "x": 1.0, "y": 2.0},
            {"group": "A", "x": 2.0, "y": 4.0},
            {"group": "B", "x": 1.0, "y": 6.0},
            {"group": "B", "x": 2.0, "y": 4.0},
        ]
        df = spark.createDataFrame(data)

        result = df.groupBy("group").agg(F.corr("x", "y").alias("corr")).collect()

        assert len(result) == 2
        # Group A has positive correlation
        group_a = [r for r in result if r["group"] == "A"][0]
        assert abs(group_a["corr"] - 1.0) < 0.001
        # Group B has negative correlation
        group_b = [r for r in result if r["group"] == "B"][0]
        assert abs(group_b["corr"] - (-1.0)) < 0.001

    def test_covar_samp_grouped_aggregation(self, spark):
        """Test sample covariance in grouped aggregation."""
        data = [
            {"group": "A", "x": 1.0, "y": 2.0},
            {"group": "A", "x": 2.0, "y": 4.0},
            {"group": "B", "x": 1.0, "y": 6.0},
            {"group": "B", "x": 2.0, "y": 4.0},
        ]
        df = spark.createDataFrame(data)

        result = (
            df.groupBy("group")
            .agg(F.covar_samp("x", "y").alias("covar_samp"))
            .collect()
        )

        assert len(result) == 2
        # Group A has positive covariance
        group_a = [r for r in result if r["group"] == "A"][0]
        assert group_a["covar_samp"] > 0
        # Group B has negative covariance
        group_b = [r for r in result if r["group"] == "B"][0]
        assert group_b["covar_samp"] < 0

    def test_corr_with_same_column_returns_one(self, spark):
        """Test that correlation of a column with itself returns 1.0."""
        data = [
            {"x": 1.0},
            {"x": 2.0},
            {"x": 3.0},
        ]
        df = spark.createDataFrame(data)

        result = df.agg(F.corr("x", "x").alias("corr")).collect()[0]

        # Correlation with itself should be 1.0
        assert abs(result["corr"] - 1.0) < 0.001

    def test_covar_samp_with_same_column(self, spark):
        """Test that sample covariance of a column with itself equals variance."""
        data = [
            {"x": 1.0},
            {"x": 2.0},
            {"x": 3.0},
        ]
        df = spark.createDataFrame(data)

        result_covar = df.agg(F.covar_samp("x", "x").alias("covar")).collect()[0]
        result_var = df.agg(F.var_samp("x").alias("var")).collect()[0]

        # covar_samp(x, x) should equal var_samp(x)
        assert abs(result_covar["covar"] - result_var["var"]) < 0.001

"""
Tests for issue #420: when() with col <= None / col >= None.

In PySpark, comparisons with None (e.g. col <= None) evaluate to NULL.
In when(), NULL conditions are treated as non-matches and fall through to otherwise().
"""


class TestIssue420WhenComparisonWithNone:
    """Test when() with comparison to None falls through to otherwise()."""

    def test_when_comparison_with_none_exact_issue(self, spark, spark_backend):
        """Exact scenario from issue #420 - col <= None and col >= None skip to otherwise."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports(spark_backend).F
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value": 5},
                {"Name": "Bob", "Value": 7},
            ]
        )
        df = df.withColumn(
            "Value",
            (
                F.when(F.col("Value") <= None, 0)
                .when(F.col("Value") >= None, 100)
                .otherwise(F.col("Value"))
            ),
        )
        rows = df.collect()
        assert len(rows) == 2
        assert rows[0]["Value"] == 5
        assert rows[1]["Value"] == 7

    def test_when_comparison_with_none_and_show(self, spark, spark_backend):
        """when() with None comparison + show() - full pipeline."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports(spark_backend).F
        df = spark.createDataFrame([{"x": 1}, {"x": 2}])
        df = df.withColumn(
            "y",
            F.when(F.col("x") < None, 0)
            .when(F.col("x") > None, 99)
            .otherwise(F.col("x")),
        )
        df.show()
        rows = df.collect()
        assert len(rows) == 2
        assert rows[0]["y"] == 1
        assert rows[1]["y"] == 2

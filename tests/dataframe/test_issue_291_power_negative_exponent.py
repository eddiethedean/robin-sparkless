"""
Robust tests for issue #291: power operator with negative exponent (unary minus).

The fix ensures that -F.col("Value") is correctly treated as unary minus when
nested inside 2.0 ** (-F.col("Value")), not as binary minus with None RHS.
"""


class TestIssue291PowerNegativeExponent:
    """Robust tests for power with negative exponent (unary minus in nested expr)."""

    def test_power_negative_exponent_exact_issue(self, spark, spark_backend):
        """Exact scenario from issue - 2.0 ** (-F.col("Value"))."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports(spark_backend).F
        df = spark.createDataFrame([{"Value": 2}, {"Value": 3}])
        df = df.withColumn("Result", 2.0 ** (-F.col("Value")))
        rows = df.collect()
        assert len(rows) == 2
        assert abs(rows[0]["Result"] - 0.25) < 0.01  # 2.0 ** (-2) = 0.25
        assert abs(rows[1]["Result"] - 0.125) < 0.01  # 2.0 ** (-3) = 0.125

    def test_power_negative_exponent_multiple_values(self, spark, spark_backend):
        """Power with negative exponent across more values."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports(spark_backend).F
        df = spark.createDataFrame(
            [{"Value": 1}, {"Value": 2}, {"Value": 4}, {"Value": 5}]
        )
        df = df.withColumn("Result", 2.0 ** (-F.col("Value")))
        rows = df.collect()
        assert len(rows) == 4
        by_val = {r["Value"]: r["Result"] for r in rows}
        assert abs(by_val[1] - 0.5) < 0.01  # 2 ** -1
        assert abs(by_val[2] - 0.25) < 0.01  # 2 ** -2
        assert abs(by_val[4] - 0.0625) < 0.01  # 2 ** -4
        assert abs(by_val[5] - 0.03125) < 0.01  # 2 ** -5

    def test_power_negative_exponent_in_select(self, spark, spark_backend):
        """Power with negative exponent in select()."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports(spark_backend).F
        df = spark.createDataFrame([{"Exp": 2}, {"Exp": 3}])
        result = df.select("Exp", (10.0 ** (-F.col("Exp"))).alias("Result"))
        rows = result.collect()
        assert len(rows) == 2
        assert abs(rows[0]["Result"] - 0.01) < 0.0001  # 10 ** -2
        assert abs(rows[1]["Result"] - 0.001) < 0.00001  # 10 ** -3

    def test_power_negative_exponent_with_show(self, spark, spark_backend):
        """Power with negative exponent + show() - full pipeline."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports(spark_backend).F
        df = spark.createDataFrame([{"x": 1}, {"x": 2}])
        df = df.withColumn("y", 3.0 ** (-F.col("x")))
        df.show()
        rows = df.collect()
        assert len(rows) == 2
        assert abs(rows[0]["y"] - 0.333333) < 0.001  # 3 ** -1
        assert abs(rows[1]["y"] - 0.111111) < 0.001  # 3 ** -2

    def test_unary_minus_standalone_in_withcolumn(self, spark, spark_backend):
        """Standalone unary minus -(-F.col("Value")) to verify unary path works."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports(spark_backend).F
        df = spark.createDataFrame([{"Value": 5}, {"Value": -3}])
        df = df.withColumn("Negated", -F.col("Value"))
        rows = df.collect()
        assert len(rows) == 2
        assert rows[0]["Negated"] == -5
        assert rows[1]["Negated"] == 3

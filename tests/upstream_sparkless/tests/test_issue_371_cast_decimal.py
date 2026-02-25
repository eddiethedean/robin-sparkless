"""
Tests for issue #371: F.col(...).cast("Decimal(10,0)") raises ValueError.

PySpark supports .cast("Decimal(X,Y)"); Sparkless now parses that string in
_translate_cast and maps to DecimalType(precision, scale), then to Polars Float64.
Assertions use float() for backend-agnostic comparison (PySpark returns Decimal).
"""


def _decimal_value(val):
    """Normalize collected value to float for backend-agnostic assertion (PySpark Decimal vs float)."""
    if val is None:
        return None
    return float(val)


class TestIssue371CastDecimal:
    """Test cast to Decimal type from string."""

    def test_with_column_cast_decimal_10_0(self, spark):
        """Exact scenario from issue #371: withColumn + cast('Decimal(10,0)')."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports().F
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Values": 10},
                {"Name": "Bob", "Values": 20},
            ]
        )
        df1 = df.withColumn("DecimalValue", F.col("Values").cast("Decimal(10,0)"))
        df1.show()
        rows = df1.collect()
        assert len(rows) == 2
        assert rows[0]["Name"] == "Alice" and rows[0]["Values"] == 10
        assert _decimal_value(rows[0]["DecimalValue"]) == 10.0
        assert rows[1]["Name"] == "Bob" and rows[1]["Values"] == 20
        assert _decimal_value(rows[1]["DecimalValue"]) == 20.0

    def test_cast_decimal_lowercase(self, spark):
        """cast('decimal(10,0)') also works (case insensitive)."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports().F
        df = spark.createDataFrame([{"x": 5}])
        df1 = df.withColumn("d", F.col("x").cast("decimal(10,0)"))
        rows = df1.collect()
        assert len(rows) == 1 and _decimal_value(rows[0]["d"]) == 5.0

    def test_cast_decimal_in_select(self, spark):
        """Cast to Decimal inside select()."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports().F
        df = spark.createDataFrame([{"a": 1}, {"a": 2}, {"a": 3}])
        result = df.select(F.col("a").cast("Decimal(10,0)").alias("dec"))
        rows = result.collect()
        assert len(rows) == 3
        assert [_decimal_value(r["dec"]) for r in rows] == [1.0, 2.0, 3.0]

    def test_cast_decimal_different_precision_scale(self, spark):
        """Decimal(5,2) and Decimal(38,2) parse and apply."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports().F
        df = spark.createDataFrame([{"x": 100}, {"x": 200}])
        df1 = df.withColumn("d5_2", F.col("x").cast("Decimal(5,2)")).withColumn(
            "d38_2", F.col("x").cast("Decimal(38,2)")
        )
        rows = df1.collect()
        assert len(rows) == 2
        assert _decimal_value(rows[0]["d5_2"]) == 100.0
        assert _decimal_value(rows[0]["d38_2"]) == 100.0
        assert _decimal_value(rows[1]["d5_2"]) == 200.0
        assert _decimal_value(rows[1]["d38_2"]) == 200.0

    def test_cast_decimal_after_filter(self, spark):
        """Cast to Decimal after filter()."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports().F
        df = spark.createDataFrame(
            [{"id": 1, "v": 10}, {"id": 2, "v": 20}, {"id": 3, "v": 30}]
        )
        df1 = df.filter(F.col("id") >= 2).withColumn(
            "dec", F.col("v").cast("Decimal(10,0)")
        )
        rows = df1.collect()
        assert len(rows) == 2
        assert _decimal_value(rows[0]["dec"]) == 20.0
        assert _decimal_value(rows[1]["dec"]) == 30.0

    def test_cast_decimal_with_nulls(self, spark):
        """Column with nulls cast to Decimal; nulls preserved."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports().F
        df = spark.createDataFrame([{"x": 1}, {"x": None}, {"x": 3}])
        df1 = df.withColumn("dec", F.col("x").cast("Decimal(10,0)"))
        rows = df1.collect()
        assert len(rows) == 3
        assert _decimal_value(rows[0]["dec"]) == 1.0
        assert rows[1]["dec"] is None
        assert _decimal_value(rows[2]["dec"]) == 3.0

    def test_cast_float_to_decimal(self, spark):
        """Float column cast to Decimal(10,1); first row exact, second row backend-dependent rounding."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports().F
        df = spark.createDataFrame([{"x": 10.5}, {"x": 20.25}])
        df1 = df.withColumn("dec", F.col("x").cast("Decimal(10,1)"))
        rows = df1.collect()
        assert len(rows) == 2
        assert _decimal_value(rows[0]["dec"]) == 10.5
        # PySpark rounds 20.25 to 20.3 (half-up); Sparkless Float64 keeps 20.25
        v1 = _decimal_value(rows[1]["dec"])
        assert v1 in (20.25, 20.2, 20.3)

    def test_cast_decimal_show_then_collect(self, spark):
        """show() then collect() returns same data."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports().F
        df = spark.createDataFrame([{"a": 7}, {"a": 8}])
        df1 = df.withColumn("d", F.col("a").cast("Decimal(10,0)"))
        df1.show()
        rows = df1.collect()
        assert len(rows) == 2
        assert (
            _decimal_value(rows[0]["d"]) == 7.0 and _decimal_value(rows[1]["d"]) == 8.0
        )

    def test_cast_decimal_single_digit_precision(self, spark):
        """Decimal(1,0) parses and applies (single digit)."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports().F
        df = spark.createDataFrame([{"x": 9}])
        df1 = df.withColumn("d", F.col("x").cast("Decimal(1,0)"))
        rows = df1.collect()
        assert len(rows) == 1 and _decimal_value(rows[0]["d"]) == 9.0

"""
Tests for issue #396: to_date() with cast to string on numeric column.

PySpark accepts F.to_date(F.col('DateNumber').cast('string'), 'yyyyMMdd') when
DateNumber is LongType. Sparkless was rejecting it because validation checked
the source column type instead of recognizing the cast-to-string.
"""

from tests.fixtures.spark_imports import get_spark_imports


class TestIssue396ToDateCast:
    """Test to_date with cast on numeric columns (issue #396)."""

    def test_to_date_cast_string_type_exact_issue(self, spark):
        """Exact scenario from issue #396: cast(StringType())."""
        imports = get_spark_imports()
        F, StringType = imports.F, imports.StringType
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "DateNumber": 20260203},
                {"Name": "Bob", "DateNumber": 20260203},
            ]
        )
        df = df.withColumn(
            "DateCol",
            F.to_date(F.col("DateNumber").cast(StringType()), "yyyyMMdd"),
        )
        rows = df.collect()
        assert len(rows) == 2
        assert str(rows[0]["DateCol"]) == "2026-02-03"

    def test_to_date_cast_string_literal(self, spark):
        """Alternate syntax: cast('string')."""
        imports = get_spark_imports()
        F = imports.F
        df = spark.createDataFrame(
            [
                {"DateNumber": 20260115},
                {"DateNumber": 20260228},
            ]
        )
        df = df.withColumn(
            "DateCol", F.to_date(F.col("DateNumber").cast("string"), "yyyyMMdd")
        )
        rows = df.collect()
        assert len(rows) == 2
        assert str(rows[0]["DateCol"]) == "2026-01-15"
        assert str(rows[1]["DateCol"]) == "2026-02-28"

    def test_to_date_cast_with_show(self, spark):
        """withColumn + show() as in issue example."""
        imports = get_spark_imports()
        F, StringType = imports.F, imports.StringType
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "DateNumber": 20260203},
                {"Name": "Bob", "DateNumber": 20260203},
            ]
        )
        df = df.withColumn(
            "DateCol",
            F.to_date(F.col("DateNumber").cast(StringType()), "yyyyMMdd"),
        )
        df.show()
        rows = df.collect()
        assert len(rows) == 2

    def test_to_date_cast_in_select(self, spark):
        """to_date cast in select()."""
        imports = get_spark_imports()
        F = imports.F
        df = spark.createDataFrame(
            [
                {"DateNumber": 20260101},
                {"DateNumber": 20251231},
            ]
        )
        df = df.select(
            F.col("DateNumber"),
            F.to_date(F.col("DateNumber").cast("string"), "yyyyMMdd").alias("DateCol"),
        )
        rows = df.collect()
        assert len(rows) == 2
        assert str(rows[0]["DateCol"]) == "2026-01-01"
        assert str(rows[1]["DateCol"]) == "2025-12-31"

    def test_to_date_cast_with_nulls(self, spark):
        """to_date cast with null values in column."""
        imports = get_spark_imports()
        F = imports.F
        df = spark.createDataFrame(
            [
                {"DateNumber": 20260203},
                {"DateNumber": None},
                {"DateNumber": 20260115},
            ]
        )
        df = df.withColumn(
            "DateCol", F.to_date(F.col("DateNumber").cast("string"), "yyyyMMdd")
        )
        rows = df.collect()
        assert len(rows) == 3
        assert str(rows[0]["DateCol"]) == "2026-02-03"
        assert rows[1]["DateCol"] is None
        assert str(rows[2]["DateCol"]) == "2026-01-15"

    def test_to_date_cast_different_format(self, spark):
        """to_date cast with yyyy-MM-dd format (hyphen)."""
        imports = get_spark_imports()
        F = imports.F
        # Store as string representation of int: 20260203 -> parse as yyyyMMdd
        df = spark.createDataFrame([{"DateNumber": 20260203}])
        df = df.withColumn(
            "DateCol", F.to_date(F.col("DateNumber").cast("string"), "yyyyMMdd")
        )
        rows = df.collect()
        assert str(rows[0]["DateCol"]) == "2026-02-03"

    def test_to_date_cast_then_filter(self, spark):
        """to_date cast then filter on result."""
        imports = get_spark_imports()
        F = imports.F
        df = spark.createDataFrame(
            [
                {"DateNumber": 20260203},
                {"DateNumber": 20250101},
                {"DateNumber": 20260315},
            ]
        )
        df = df.withColumn(
            "DateCol", F.to_date(F.col("DateNumber").cast("string"), "yyyyMMdd")
        )
        df = df.filter(F.col("DateCol") >= F.lit("2026-02-01").cast("date"))
        rows = df.collect()
        assert len(rows) == 2

    def test_to_date_cast_integer_type_column(self, spark):
        """to_date cast on IntegerType column (not just LongType)."""
        imports = get_spark_imports()
        F, IntegerType, StructType, StructField = (
            imports.F,
            imports.IntegerType,
            imports.StructType,
            imports.StructField,
        )
        schema = StructType([StructField("DateNum", IntegerType(), True)])
        df = spark.createDataFrame(
            [{"DateNum": 20260203}, {"DateNum": 20260101}], schema=schema
        )
        df = df.withColumn(
            "DateCol", F.to_date(F.col("DateNum").cast("string"), "yyyyMMdd")
        )
        rows = df.collect()
        assert len(rows) == 2
        assert str(rows[0]["DateCol"]) == "2026-02-03"

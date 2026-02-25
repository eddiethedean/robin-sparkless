"""
PySpark parity tests for split() function with limit parameter.

Tests validate that Sparkless split() function with limit behaves identically to PySpark.
"""

from tests.fixtures.parity_base import ParityTestBase
from tests.fixtures.spark_imports import get_spark_imports


class TestSplitLimitParity(ParityTestBase):
    """Test split() function with limit parameter parity with PySpark."""

    def test_split_with_limit_parity(self, spark):
        """Test split with limit parameter matches PySpark (issue #328)."""
        imports = get_spark_imports()
        F = imports.F

        # Exact example from issue #328
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "StringValue": "A,B,C,D,E,F"},
            ]
        )

        # split StringValue with a pattern match limit of 3
        result = df.withColumn("StringArray", F.split(F.col("StringValue"), ",", 3))

        # show that the limit was applied by exploding the array
        result = result.withColumn("StringArray", F.explode(F.col("StringArray")))

        rows = result.collect()
        assert len(rows) == 3  # Should have 3 elements after limit=3

        # Expected: ["A", "B", "C,D,E,F"]
        values = [r["StringArray"] for r in rows]
        assert "A" in values
        assert "B" in values
        assert "C,D,E,F" in values
        assert "C" not in values  # Should not be split further

    def test_split_with_limit_1_parity(self, spark):
        """Test split with limit=1 matches PySpark (no split, returns original)."""
        imports = get_spark_imports()
        F = imports.F

        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value": "A,B,C,D"},
            ]
        )

        result = df.withColumn("Array", F.split(F.col("Value"), ",", 1))
        result = result.withColumn("Array", F.explode(F.col("Array")))

        rows = result.collect()
        assert len(rows) == 1  # limit=1 means 1 part (no split)

        values = [r["Array"] for r in rows]
        assert "A,B,C,D" in values  # Original string unsplit

    def test_split_without_limit_parity(self, spark):
        """Test split without limit matches PySpark default behavior."""
        imports = get_spark_imports()
        F = imports.F

        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value": "A,B,C,D"},
            ]
        )

        # No limit parameter - should split all
        result = df.withColumn("Array", F.split(F.col("Value"), ","))
        result = result.withColumn("Array", F.explode(F.col("Array")))

        rows = result.collect()
        assert len(rows) == 4  # All parts split

        values = [r["Array"] for r in rows]
        assert "A" in values
        assert "B" in values
        assert "C" in values
        assert "D" in values

    def test_split_with_limit_minus_one_parity(self, spark):
        """Test split with limit=-1 (no limit) matches PySpark."""
        imports = get_spark_imports()
        F = imports.F

        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value": "A,B,C,D"},
            ]
        )

        # limit=-1 should behave like no limit
        result = df.withColumn("Array", F.split(F.col("Value"), ",", -1))
        result = result.withColumn("Array", F.explode(F.col("Array")))

        rows = result.collect()
        assert len(rows) == 4  # All parts split

        values = [r["Array"] for r in rows]
        assert "A" in values
        assert "B" in values
        assert "C" in values
        assert "D" in values

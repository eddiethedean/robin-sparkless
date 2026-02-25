"""PySpark parity test for approx_count_distinct with rsd parameter (Issue #266)."""

from tests.fixtures.parity_base import ParityTestBase
from tests.fixtures.spark_imports import get_spark_imports


class TestApproxCountDistinctRsdParity(ParityTestBase):
    """Test approx_count_distinct with rsd parameter parity with PySpark (Issue #266)."""

    def test_approx_count_distinct_rsd_issue_266(self, spark):
        """Test the exact example from issue #266.

        This test verifies the exact example from issue #266:
        https://github.com/eddiethedean/sparkless/issues/266
        """
        imports = get_spark_imports()
        F = imports.F
        Window = imports.Window

        data = [
            {"type": "A", "value": 1},
            {"type": "A", "value": 10},
            {"type": "B", "value": 5},
        ]
        df = spark.createDataFrame(data)

        type_window = Window().partitionBy("type")

        # This is the exact code from issue #266
        result = df.withColumn(
            "approx_count", F.approx_count_distinct("value", rsd=0.01).over(type_window)
        )

        rows = result.collect()
        assert len(rows) == 3

        # Verify the structure matches PySpark
        schema = result.schema
        assert "approx_count" in [f.name for f in schema.fields]

        # Verify values match PySpark expected output
        # Group A: values [1, 10] -> 2 distinct
        # Group B: values [5] -> 1 distinct
        for row in rows:
            assert row["approx_count"] is not None, "approx_count should not be None"
            assert isinstance(row["approx_count"], int), (
                f"approx_count should be int, got {type(row['approx_count'])}"
            )

            if row["type"] == "A":
                assert row["approx_count"] == 2
            elif row["type"] == "B":
                assert row["approx_count"] == 1

    def test_approx_count_distinct_rsd_parity(self, spark):
        """Test approx_count_distinct with rsd parameter matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F

        data = [
            {"group": "A", "value": 1},
            {"group": "A", "value": 2},
            {"group": "A", "value": 1},  # Duplicate
            {"group": "B", "value": 10},
            {"group": "B", "value": 20},
        ]
        df = spark.createDataFrame(data)

        # Test with rsd parameter
        result = df.groupby("group").agg(
            F.approx_count_distinct("value", rsd=0.01).alias("distinct_count")
        )

        rows = result.collect()
        assert len(rows) == 2

        # Group A: values [1, 2, 1] -> 2 distinct
        row_a = next((r for r in rows if r["group"] == "A"), None)
        assert row_a is not None
        assert row_a["distinct_count"] == 2

        # Group B: values [10, 20] -> 2 distinct
        row_b = next((r for r in rows if r["group"] == "B"), None)
        assert row_b is not None
        assert row_b["distinct_count"] == 2

    def test_approx_count_distinct_window_parity(self, spark):
        """Test approx_count_distinct in Window functions matches PySpark behavior."""
        imports = get_spark_imports()
        F = imports.F
        Window = imports.Window

        data = [
            {"type": "A", "value": 1},
            {"type": "A", "value": 10},
            {"type": "A", "value": 1},  # Duplicate
            {"type": "B", "value": 5},
        ]
        df = spark.createDataFrame(data)

        type_window = Window().partitionBy("type")

        result = df.withColumn(
            "approx_count", F.approx_count_distinct("value", rsd=0.05).over(type_window)
        )

        rows = result.collect()
        assert len(rows) == 4

        # Verify no None values (this was the bug)
        for row in rows:
            assert row["approx_count"] is not None, "approx_count should not be None"
            assert isinstance(row["approx_count"], int), (
                f"approx_count should be int, got {type(row['approx_count'])}"
            )

            if row["type"] == "A":
                assert row["approx_count"] == 2  # Distinct: 1, 10
            elif row["type"] == "B":
                assert row["approx_count"] == 1  # Distinct: 5

    def test_approx_count_distinct_without_rsd_parity(self, spark):
        """Test approx_count_distinct without rsd parameter matches PySpark."""
        imports = get_spark_imports()
        F = imports.F

        data = [
            {"group": "A", "value": 1},
            {"group": "A", "value": 2},
            {"group": "B", "value": 3},
        ]
        df = spark.createDataFrame(data)

        result = df.groupby("group").agg(F.approx_count_distinct("value"))

        rows = result.collect()
        assert len(rows) == 2

        # Verify column name format
        schema = result.schema
        field_names = [f.name for f in schema.fields]
        assert any("approx_count_distinct" in name for name in field_names)

        # Verify values
        for row in rows:
            for col_name in row.asDict():
                if "approx_count_distinct" in col_name:
                    assert row[col_name] is not None
                    assert isinstance(row[col_name], int)
                    break

    def test_approx_count_distinct_rsd_different_values_parity(self, spark):
        """Test that different rsd values are accepted (API compatibility)."""
        imports = get_spark_imports()
        F = imports.F

        data = [{"value": 1}, {"value": 2}, {"value": 1}]
        df = spark.createDataFrame(data)

        # Test various rsd values - all should work
        rsd_values = [0.01, 0.05, 0.1, 0.2]

        for rsd in rsd_values:
            result = df.agg(F.approx_count_distinct("value", rsd=rsd))
            rows = result.collect()
            assert len(rows) == 1

            # Find the approx_count_distinct column
            for col_name in rows[0].asDict():
                if "approx_count_distinct" in col_name:
                    assert rows[0][col_name] == 2  # Distinct: 1, 2
                    # PySpark doesn't include rsd in column name, just uses base name
                    assert col_name == "approx_count_distinct(value)"
                    break

    def test_approx_count_distinct_window_without_rsd_parity(self, spark):
        """Test approx_count_distinct in Window without rsd (backward compatibility)."""
        imports = get_spark_imports()
        F = imports.F
        Window = imports.Window

        data = [
            {"type": "A", "value": 1},
            {"type": "A", "value": 10},
            {"type": "B", "value": 5},
        ]
        df = spark.createDataFrame(data)

        type_window = Window().partitionBy("type")

        # Without rsd parameter
        result = df.withColumn(
            "approx_count", F.approx_count_distinct("value").over(type_window)
        )

        rows = result.collect()
        assert len(rows) == 3

        # Verify no None values
        for row in rows:
            assert row["approx_count"] is not None, "approx_count should not be None"
            assert isinstance(row["approx_count"], int)

            if row["type"] == "A":
                assert row["approx_count"] == 2
            elif row["type"] == "B":
                assert row["approx_count"] == 1

"""PySpark parity tests for PivotGroupedData aggregate methods (Issue #267)."""

from tests.fixtures.parity_base import ParityTestBase


class TestPivotGroupedDataParity(ParityTestBase):
    """PySpark parity tests for PivotGroupedData."""

    def test_pivot_sum_parity(self, spark):
        """Test pivot with sum() matches PySpark."""
        data = [
            {"type": "A", "value": 1},
            {"type": "A", "value": 10},
            {"type": "B", "value": 5},
        ]
        df = spark.createDataFrame(data)
        result = df.groupBy("type").pivot("type", ["A", "B"]).sum("value")
        rows = result.collect()

        # Verify schema matches PySpark
        schema_names = [f.name for f in result.schema.fields]
        assert "type" in schema_names
        assert "A" in schema_names
        assert "B" in schema_names

        # Verify data matches PySpark
        assert len(rows) == 2
        row_a = next((r for r in rows if r["type"] == "A"), None)
        row_b = next((r for r in rows if r["type"] == "B"), None)

        assert row_a is not None
        assert row_b is not None
        assert row_a["A"] == 11
        assert row_a["B"] is None
        assert row_b["A"] is None
        assert row_b["B"] == 5

    def test_pivot_avg_parity(self, spark):
        """Test pivot with avg() matches PySpark."""
        data = [
            {"type": "A", "value": 1},
            {"type": "A", "value": 10},
            {"type": "B", "value": 5},
        ]
        df = spark.createDataFrame(data)
        result = df.groupBy("type").pivot("type", ["A", "B"]).avg("value")
        rows = result.collect()

        assert len(rows) == 2
        row_a = next((r for r in rows if r["type"] == "A"), None)
        row_b = next((r for r in rows if r["type"] == "B"), None)

        assert row_a is not None
        assert row_b is not None
        assert row_a["A"] == 5.5
        assert row_a["B"] is None
        assert row_b["A"] is None
        assert row_b["B"] == 5.0

    def test_pivot_count_parity(self, spark):
        """Test pivot with count() matches PySpark."""
        data = [
            {"type": "A", "value": 1},
            {"type": "A", "value": 10},
            {"type": "B", "value": 5},
        ]
        df = spark.createDataFrame(data)
        result = df.groupBy("type").pivot("type", ["A", "B"]).count()
        rows = result.collect()

        assert len(rows) == 2
        row_a = next((r for r in rows if r["type"] == "A"), None)
        row_b = next((r for r in rows if r["type"] == "B"), None)

        assert row_a is not None
        assert row_b is not None
        assert row_a["A"] == 2
        assert row_a["B"] is None
        assert row_b["A"] is None
        assert row_b["B"] == 1

    def test_pivot_max_parity(self, spark):
        """Test pivot with max() matches PySpark."""
        data = [
            {"type": "A", "value": 1},
            {"type": "A", "value": 10},
            {"type": "B", "value": 5},
        ]
        df = spark.createDataFrame(data)
        result = df.groupBy("type").pivot("type", ["A", "B"]).max("value")
        rows = result.collect()

        assert len(rows) == 2
        row_a = next((r for r in rows if r["type"] == "A"), None)
        row_b = next((r for r in rows if r["type"] == "B"), None)

        assert row_a is not None
        assert row_b is not None
        assert row_a["A"] == 10
        assert row_a["B"] is None
        assert row_b["A"] is None
        assert row_b["B"] == 5

    def test_pivot_min_parity(self, spark):
        """Test pivot with min() matches PySpark."""
        data = [
            {"type": "A", "value": 1},
            {"type": "A", "value": 10},
            {"type": "B", "value": 5},
        ]
        df = spark.createDataFrame(data)
        result = df.groupBy("type").pivot("type", ["A", "B"]).min("value")
        rows = result.collect()

        assert len(rows) == 2
        row_a = next((r for r in rows if r["type"] == "A"), None)
        row_b = next((r for r in rows if r["type"] == "B"), None)

        assert row_a is not None
        assert row_b is not None
        assert row_a["A"] == 1
        assert row_a["B"] is None
        assert row_b["A"] is None
        assert row_b["B"] == 5

    def test_pivot_issue_267_example(self, spark):
        """Test the exact example from Issue #267."""
        df = spark.createDataFrame(
            [
                {"type": "A", "value": 1},
                {"type": "A", "value": 10},
                {"type": "B", "value": 5},
            ]
        )

        result = df.groupBy("type").pivot("type", ["A", "B"]).sum("value")
        rows = result.collect()

        # Verify schema matches expected output from issue
        schema_names = [f.name for f in result.schema.fields]
        assert "type" in schema_names
        assert "A" in schema_names
        assert "B" in schema_names

        # Verify data matches expected output
        assert len(rows) == 2
        row_a = next((r for r in rows if r["type"] == "A"), None)
        row_b = next((r for r in rows if r["type"] == "B"), None)

        assert row_a is not None
        assert row_b is not None
        # Expected: type=A, A=11, B=NULL
        assert row_a["A"] == 11
        assert row_a["B"] is None
        # Expected: type=B, A=NULL, B=5
        assert row_b["A"] is None
        assert row_b["B"] == 5

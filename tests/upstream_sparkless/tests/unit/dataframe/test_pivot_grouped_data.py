"""Unit tests for PivotGroupedData aggregate methods (Issue #267)."""

import pytest
from sparkless.sql import SparkSession
import sparkless.sql.functions as F


class TestPivotGroupedData:
    """Test PivotGroupedData convenience methods."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        return SparkSession.builder.appName("test").getOrCreate()

    @pytest.fixture
    def sample_data(self):
        """Sample data for testing."""
        return [
            {"type": "A", "value": 1},
            {"type": "A", "value": 10},
            {"type": "B", "value": 5},
        ]

    def test_pivot_sum(self, spark, sample_data):
        """Test pivot with sum() method."""
        df = spark.createDataFrame(sample_data)
        result = df.groupBy("type").pivot("type", ["A", "B"]).sum("value")
        rows = result.collect()

        assert len(rows) == 2
        # Check column names - should be just pivot values
        schema_names = [f.name for f in result.schema.fields]
        assert "type" in schema_names
        assert "A" in schema_names
        assert "B" in schema_names

        # Find rows by type
        row_a = next((r for r in rows if r["type"] == "A"), None)
        row_b = next((r for r in rows if r["type"] == "B"), None)

        assert row_a is not None
        assert row_b is not None
        # Type A has values 1 and 10, so sum should be 11
        assert row_a["A"] == 11
        assert row_a["B"] is None  # No B values for type A
        # Type B has value 5
        assert row_b["A"] is None  # No A values for type B
        assert row_b["B"] == 5

    def test_pivot_avg(self, spark, sample_data):
        """Test pivot with avg() method."""
        df = spark.createDataFrame(sample_data)
        result = df.groupBy("type").pivot("type", ["A", "B"]).avg("value")
        rows = result.collect()

        assert len(rows) == 2
        row_a = next((r for r in rows if r["type"] == "A"), None)
        row_b = next((r for r in rows if r["type"] == "B"), None)

        assert row_a is not None
        assert row_b is not None
        # Type A: (1 + 10) / 2 = 5.5
        assert row_a["A"] == 5.5
        assert row_a["B"] is None
        # Type B: 5 / 1 = 5.0
        assert row_b["A"] is None
        assert row_b["B"] == 5.0

    def test_pivot_count(self, spark, sample_data):
        """Test pivot with count() method."""
        df = spark.createDataFrame(sample_data)
        result = df.groupBy("type").pivot("type", ["A", "B"]).count()
        rows = result.collect()

        assert len(rows) == 2
        row_a = next((r for r in rows if r["type"] == "A"), None)
        row_b = next((r for r in rows if r["type"] == "B"), None)

        assert row_a is not None
        assert row_b is not None
        # Type A has 2 rows
        assert row_a["A"] == 2
        assert row_a["B"] is None
        # Type B has 1 row
        assert row_b["A"] is None
        assert row_b["B"] == 1

    def test_pivot_max(self, spark, sample_data):
        """Test pivot with max() method."""
        df = spark.createDataFrame(sample_data)
        result = df.groupBy("type").pivot("type", ["A", "B"]).max("value")
        rows = result.collect()

        assert len(rows) == 2
        row_a = next((r for r in rows if r["type"] == "A"), None)
        row_b = next((r for r in rows if r["type"] == "B"), None)

        assert row_a is not None
        assert row_b is not None
        # Type A: max(1, 10) = 10
        assert row_a["A"] == 10
        assert row_a["B"] is None
        # Type B: max(5) = 5
        assert row_b["A"] is None
        assert row_b["B"] == 5

    def test_pivot_min(self, spark, sample_data):
        """Test pivot with min() method."""
        df = spark.createDataFrame(sample_data)
        result = df.groupBy("type").pivot("type", ["A", "B"]).min("value")
        rows = result.collect()

        assert len(rows) == 2
        row_a = next((r for r in rows if r["type"] == "A"), None)
        row_b = next((r for r in rows if r["type"] == "B"), None)

        assert row_a is not None
        assert row_b is not None
        # Type A: min(1, 10) = 1
        assert row_a["A"] == 1
        assert row_a["B"] is None
        # Type B: min(5) = 5
        assert row_b["A"] is None
        assert row_b["B"] == 5

    def test_pivot_count_distinct(self, spark):
        """Test pivot with count_distinct() method."""
        data = [
            {"type": "A", "value": 1},
            {"type": "A", "value": 1},  # Duplicate
            {"type": "A", "value": 10},
            {"type": "B", "value": 5},
        ]
        df = spark.createDataFrame(data)
        result = df.groupBy("type").pivot("type", ["A", "B"]).count_distinct("value")
        rows = result.collect()

        assert len(rows) == 2
        row_a = next((r for r in rows if r["type"] == "A"), None)
        row_b = next((r for r in rows if r["type"] == "B"), None)

        assert row_a is not None
        assert row_b is not None
        # Type A: distinct values are 1, 10 = 2
        assert row_a["A"] == 2
        assert row_a["B"] is None
        # Type B: distinct value is 5 = 1
        assert row_b["A"] is None
        assert row_b["B"] == 1

    def test_pivot_collect_list(self, spark):
        """Test pivot with collect_list() method."""
        data = [
            {"type": "A", "value": 1},
            {"type": "A", "value": 10},
            {"type": "B", "value": 5},
        ]
        df = spark.createDataFrame(data)
        result = df.groupBy("type").pivot("type", ["A", "B"]).collect_list("value")
        rows = result.collect()

        assert len(rows) == 2
        row_a = next((r for r in rows if r["type"] == "A"), None)
        row_b = next((r for r in rows if r["type"] == "B"), None)

        assert row_a is not None
        assert row_b is not None
        # Type A: [1, 10]
        assert row_a["A"] == [1, 10]
        assert row_a["B"] is None
        # Type B: [5]
        assert row_b["A"] is None
        assert row_b["B"] == [5]

    def test_pivot_collect_set(self, spark):
        """Test pivot with collect_set() method."""
        data = [
            {"type": "A", "value": 1},
            {"type": "A", "value": 1},  # Duplicate
            {"type": "A", "value": 10},
            {"type": "B", "value": 5},
        ]
        df = spark.createDataFrame(data)
        result = df.groupBy("type").pivot("type", ["A", "B"]).collect_set("value")
        rows = result.collect()

        assert len(rows) == 2
        row_a = next((r for r in rows if r["type"] == "A"), None)
        row_b = next((r for r in rows if r["type"] == "B"), None)

        assert row_a is not None
        assert row_b is not None
        # Type A: {1, 10} (as list)
        assert set(row_a["A"]) == {1, 10}
        assert row_a["B"] is None
        # Type B: {5}
        assert row_b["A"] is None
        assert row_b["B"] == [5]

    def test_pivot_first(self, spark, sample_data):
        """Test pivot with first() method."""
        df = spark.createDataFrame(sample_data)
        result = df.groupBy("type").pivot("type", ["A", "B"]).first("value")
        rows = result.collect()

        assert len(rows) == 2
        row_a = next((r for r in rows if r["type"] == "A"), None)
        row_b = next((r for r in rows if r["type"] == "B"), None)

        assert row_a is not None
        assert row_b is not None
        # Type A: first value is 1
        assert row_a["A"] == 1
        assert row_a["B"] is None
        # Type B: first value is 5
        assert row_b["A"] is None
        assert row_b["B"] == 5

    def test_pivot_last(self, spark, sample_data):
        """Test pivot with last() method."""
        df = spark.createDataFrame(sample_data)
        result = df.groupBy("type").pivot("type", ["A", "B"]).last("value")
        rows = result.collect()

        assert len(rows) == 2
        row_a = next((r for r in rows if r["type"] == "A"), None)
        row_b = next((r for r in rows if r["type"] == "B"), None)

        assert row_a is not None
        assert row_b is not None
        # Type A: last value is 10
        assert row_a["A"] == 10
        assert row_a["B"] is None
        # Type B: last value is 5
        assert row_b["A"] is None
        assert row_b["B"] == 5

    def test_pivot_stddev(self, spark):
        """Test pivot with stddev() method."""
        data = [
            {"type": "A", "value": 1},
            {"type": "A", "value": 10},
            {"type": "B", "value": 5},
        ]
        df = spark.createDataFrame(data)
        result = df.groupBy("type").pivot("type", ["A", "B"]).stddev("value")
        rows = result.collect()

        assert len(rows) == 2
        row_a = next((r for r in rows if r["type"] == "A"), None)
        row_b = next((r for r in rows if r["type"] == "B"), None)

        assert row_a is not None
        assert row_b is not None
        # Type A: stddev of [1, 10] should be calculated
        assert row_a["A"] is not None
        assert isinstance(row_a["A"], float)
        assert row_a["B"] is None
        # Type B: only one value, stddev should be None
        assert row_b["A"] is None
        assert row_b["B"] is None

    def test_pivot_variance(self, spark):
        """Test pivot with variance() method."""
        data = [
            {"type": "A", "value": 1},
            {"type": "A", "value": 10},
            {"type": "B", "value": 5},
        ]
        df = spark.createDataFrame(data)
        result = df.groupBy("type").pivot("type", ["A", "B"]).variance("value")
        rows = result.collect()

        assert len(rows) == 2
        row_a = next((r for r in rows if r["type"] == "A"), None)
        row_b = next((r for r in rows if r["type"] == "B"), None)

        assert row_a is not None
        assert row_b is not None
        # Type A: variance of [1, 10] should be calculated
        assert row_a["A"] is not None
        assert isinstance(row_a["A"], float)
        assert row_a["B"] is None
        # Type B: only one value, variance should be None
        assert row_b["A"] is None
        assert row_b["B"] is None

    def test_pivot_mean(self, spark, sample_data):
        """Test pivot with mean() method (alias for avg)."""
        df = spark.createDataFrame(sample_data)
        result = df.groupBy("type").pivot("type", ["A", "B"]).mean("value")
        rows = result.collect()

        assert len(rows) == 2
        row_a = next((r for r in rows if r["type"] == "A"), None)
        row_b = next((r for r in rows if r["type"] == "B"), None)

        assert row_a is not None
        assert row_b is not None
        # Should be same as avg
        assert row_a["A"] == 5.5
        assert row_a["B"] is None
        assert row_b["A"] is None
        assert row_b["B"] == 5.0

    def test_pivot_multiple_aggregates(self, spark, sample_data):
        """Test pivot with multiple aggregate expressions."""
        df = spark.createDataFrame(sample_data)
        result = (
            df.groupBy("type")
            .pivot("type", ["A", "B"])
            .agg(F.sum("value").alias("total"), F.avg("value").alias("avg_val"))
        )
        rows = result.collect()

        assert len(rows) == 2
        # With multiple expressions, PySpark uses {pivot_value}_{alias} format
        schema_names = [f.name for f in result.schema.fields]
        assert "type" in schema_names
        # Should have columns like A_total, A_avg_val, B_total, B_avg_val
        assert "A_total" in schema_names
        assert "A_avg_val" in schema_names
        assert "B_total" in schema_names
        assert "B_avg_val" in schema_names

    def test_pivot_single_aggregate_with_alias(self, spark, sample_data):
        """Test pivot with single aggregate expression with alias."""
        df = spark.createDataFrame(sample_data)
        result = (
            df.groupBy("type")
            .pivot("type", ["A", "B"])
            .agg(F.sum("value").alias("total"))
        )
        rows = result.collect()

        assert len(rows) == 2
        # With single expression and alias, should use alias as column name
        schema_names = [f.name for f in result.schema.fields]
        assert "type" in schema_names
        assert "total" in schema_names  # Should be just "total", not "total_A"

    def test_pivot_empty_groups(self, spark):
        """Test pivot with empty groups."""
        data = [{"type": "A", "value": 1}]
        df = spark.createDataFrame(data)
        result = df.groupBy("type").pivot("type", ["A", "B"]).sum("value")
        rows = result.collect()

        assert len(rows) == 1
        row_a = next((r for r in rows if r["type"] == "A"), None)
        assert row_a is not None
        assert row_a["A"] == 1
        assert row_a["B"] is None  # No B values

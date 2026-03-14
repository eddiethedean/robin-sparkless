"""
PySpark parity tests for stricter parameter validation.

These tests verify that Sparkless matches PySpark's strict parameter requirements
for various functions that were previously too lenient.

Issue: PySpark Parity Hunt - Areas Where Sparkless Was Too Lenient
"""

import pytest
from tests.tools.parity_base import ParityTestBase
from sparkless.testing import get_imports


class TestArrayPositionRequiredValue(ParityTestBase):
    """Test that array_position requires the value parameter (PySpark parity)."""

    def test_array_position_with_value(self, spark):
        """Test array_position works correctly with value parameter."""
        imports = get_imports()
        F = imports.F

        data = [
            {"arr": [1, 2, 3, 4, 5]},
            {"arr": [10, 20, 30]},
            {"arr": [1, 1, 1]},
        ]
        df = spark.createDataFrame(data)

        result = df.select(F.array_position("arr", 1).alias("pos"))
        rows = result.collect()

        assert len(rows) == 3
        assert rows[0]["pos"] == 1  # 1-based index
        assert rows[1]["pos"] == 0  # Not found
        assert rows[2]["pos"] == 1  # First occurrence

    def test_array_position_requires_value(self, spark):
        """Test array_position raises TypeError when value is missing."""
        imports = get_imports()
        F = imports.F

        data = [{"arr": [1, 2, 3]}]
        df = spark.createDataFrame(data)

        with pytest.raises(TypeError):
            df.select(F.array_position("arr"))


class TestArrayRemoveRequiredValue(ParityTestBase):
    """Test that array_remove requires the value parameter (PySpark parity)."""

    def test_array_remove_with_value(self, spark):
        """Test array_remove works correctly with value parameter."""
        imports = get_imports()
        F = imports.F

        data = [
            {"arr": [1, 2, 3, 2, 4]},
            {"arr": [10, 20, 30]},
        ]
        df = spark.createDataFrame(data)

        result = df.select(F.array_remove("arr", 2).alias("result"))
        rows = result.collect()

        assert len(rows) == 2
        assert rows[0]["result"] == [1, 3, 4]  # All 2s removed
        assert rows[1]["result"] == [10, 20, 30]  # Unchanged

    def test_array_remove_requires_value(self, spark):
        """Test array_remove raises TypeError when value is missing."""
        imports = get_imports()
        F = imports.F

        data = [{"arr": [1, 2, 3]}]
        df = spark.createDataFrame(data)

        with pytest.raises(TypeError):
            df.select(F.array_remove("arr"))


class TestSubstringRequiredLength(ParityTestBase):
    """Test that substring requires the length parameter (PySpark parity)."""

    def test_substring_with_length(self, spark):
        """Test substring works correctly with length parameter."""
        imports = get_imports()
        F = imports.F

        data = [
            {"text": "Hello World"},
            {"text": "Python"},
        ]
        df = spark.createDataFrame(data)

        result = df.select(F.substring("text", 1, 5).alias("sub"))
        rows = result.collect()

        assert len(rows) == 2
        assert rows[0]["sub"] == "Hello"
        assert rows[1]["sub"] == "Pytho"

    def test_substring_requires_length(self, spark):
        """Test substring raises TypeError when length is missing."""
        imports = get_imports()
        F = imports.F

        data = [{"text": "Hello"}]
        df = spark.createDataFrame(data)

        with pytest.raises(TypeError):
            df.select(F.substring("text", 1))


class TestWhenRequiredValue(ParityTestBase):
    """Test that when() requires the value parameter (PySpark parity)."""

    def test_when_with_value(self, spark):
        """Test when works correctly with value parameter."""
        imports = get_imports()
        F = imports.F

        data = [
            {"score": 90},
            {"score": 70},
            {"score": 50},
        ]
        df = spark.createDataFrame(data)

        result = df.select(
            F.when(F.col("score") >= 80, "A")
            .when(F.col("score") >= 60, "B")
            .otherwise("C")
            .alias("grade")
        )
        rows = result.collect()

        assert len(rows) == 3
        assert rows[0]["grade"] == "A"
        assert rows[1]["grade"] == "B"
        assert rows[2]["grade"] == "C"

    def test_when_requires_value(self, spark):
        """Test when raises TypeError when value is missing."""
        imports = get_imports()
        F = imports.F

        with pytest.raises(TypeError):
            F.when(F.col("score") >= 80)


class TestArrayJoinNullReplacement(ParityTestBase):
    """Test that array_join properly handles null_replacement parameter."""

    def test_array_join_basic(self, spark):
        """Test basic array_join without null_replacement."""
        imports = get_imports()
        F = imports.F

        data = [
            {"arr": ["a", "b", "c"]},
            {"arr": ["x", "y"]},
        ]
        df = spark.createDataFrame(data)

        result = df.select(F.array_join("arr", "-").alias("joined"))
        rows = result.collect()

        assert len(rows) == 2
        assert rows[0]["joined"] == "a-b-c"
        assert rows[1]["joined"] == "x-y"

    def test_array_join_with_null_replacement(self, spark):
        """Test array_join with null_replacement parameter."""
        imports = get_imports()
        F = imports.F

        data = [
            {"arr": ["a", None, "c"]},
            {"arr": [None, "y", None]},
        ]
        df = spark.createDataFrame(data)

        result = df.select(F.array_join("arr", "-", "NULL").alias("joined"))
        rows = result.collect()

        assert len(rows) == 2
        assert rows[0]["joined"] == "a-NULL-c"
        assert rows[1]["joined"] == "NULL-y-NULL"

    def test_array_join_null_replacement_skips_nulls_when_none(self, spark):
        """Test array_join skips nulls when null_replacement is not provided."""
        imports = get_imports()
        F = imports.F

        data = [
            {"arr": ["a", None, "c"]},
        ]
        df = spark.createDataFrame(data)

        result = df.select(F.array_join("arr", "-").alias("joined"))
        rows = result.collect()

        assert len(rows) == 1
        assert rows[0]["joined"] == "a-c"


class TestSortArrayAscParameter(ParityTestBase):
    """Test that sort_array properly handles asc parameter."""

    def test_sort_array_ascending(self, spark):
        """Test sort_array with asc=True (default)."""
        imports = get_imports()
        F = imports.F

        data = [
            {"arr": [3, 1, 4, 1, 5, 9, 2, 6]},
            {"arr": [10, 5, 20]},
        ]
        df = spark.createDataFrame(data)

        result = df.select(F.sort_array("arr", asc=True).alias("sorted"))
        rows = result.collect()

        assert len(rows) == 2
        assert rows[0]["sorted"] == [1, 1, 2, 3, 4, 5, 6, 9]
        assert rows[1]["sorted"] == [5, 10, 20]

    def test_sort_array_descending(self, spark):
        """Test sort_array with asc=False."""
        imports = get_imports()
        F = imports.F

        data = [
            {"arr": [3, 1, 4, 1, 5, 9, 2, 6]},
            {"arr": [10, 5, 20]},
        ]
        df = spark.createDataFrame(data)

        result = df.select(F.sort_array("arr", asc=False).alias("sorted"))
        rows = result.collect()

        assert len(rows) == 2
        assert rows[0]["sorted"] == [9, 6, 5, 4, 3, 2, 1, 1]
        assert rows[1]["sorted"] == [20, 10, 5]

    def test_sort_array_default_ascending(self, spark):
        """Test sort_array defaults to ascending."""
        imports = get_imports()
        F = imports.F

        data = [{"arr": [3, 1, 2]}]
        df = spark.createDataFrame(data)

        result = df.select(F.sort_array("arr").alias("sorted"))
        rows = result.collect()

        assert len(rows) == 1
        assert rows[0]["sorted"] == [1, 2, 3]


class TestArraySortAscParameter(ParityTestBase):
    """Test that array_sort properly handles asc parameter."""

    def test_array_sort_ascending(self, spark):
        """Test array_sort with asc=True."""
        imports = get_imports()
        F = imports.F

        data = [{"arr": [5, 2, 8, 1]}]
        df = spark.createDataFrame(data)

        result = df.select(F.array_sort("arr", asc=True).alias("sorted"))
        rows = result.collect()

        assert len(rows) == 1
        assert rows[0]["sorted"] == [1, 2, 5, 8]

    def test_array_sort_descending(self, spark):
        """Test array_sort with asc=False."""
        imports = get_imports()
        F = imports.F

        data = [{"arr": [5, 2, 8, 1]}]
        df = spark.createDataFrame(data)

        result = df.select(F.array_sort("arr", asc=False).alias("sorted"))
        rows = result.collect()

        assert len(rows) == 1
        assert rows[0]["sorted"] == [8, 5, 2, 1]


class TestNaReplaceValidation(ParityTestBase):
    """Test that na.replace validates value parameter (PySpark parity)."""

    def test_na_replace_with_dict(self, spark):
        """Test na.replace works with dict (value not required)."""
        data = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": None},
            {"name": None, "age": 25},
        ]
        df = spark.createDataFrame(data)

        result = df.na.replace({"Bob": "Robert"})
        rows = result.collect()

        assert len(rows) == 3
        row_dict = {r["name"]: r for r in rows if r["name"] is not None}
        assert "Robert" in row_dict

    def test_na_replace_with_scalar_and_value(self, spark):
        """Test na.replace works with scalar to_replace and value."""
        data = [
            {"name": "Alice"},
            {"name": "Bob"},
            {"name": "Charlie"},
        ]
        df = spark.createDataFrame(data)

        result = df.na.replace("Bob", "Robert")
        rows = result.collect()

        names = [r["name"] for r in rows]
        assert "Robert" in names
        assert "Bob" not in names

    def test_na_replace_string_to_null_allowed(self, spark):
        """Test na.replace allows string to_replace with value=None (replaces with null)."""
        data = [{"name": "Alice"}, {"name": "Bob"}]
        df = spark.createDataFrame(data)

        # PySpark allows string replacement with None (replaces with null)
        result = df.na.replace("Alice")
        rows = result.collect()

        names = [r["name"] for r in rows]
        assert None in names
        assert "Bob" in names

    def test_na_replace_requires_value_for_numeric(self, spark):
        """Test na.replace raises error when to_replace is numeric and value is None."""
        data = [{"value": 1}, {"value": 2}]
        df = spark.createDataFrame(data)

        # PySpark raises error for numeric to_replace without value
        with pytest.raises(Exception) as exc_info:
            df.na.replace(1)

        assert "value is required" in str(exc_info.value)

    def test_na_replace_requires_value_for_list(self, spark):
        """Test na.replace raises error when to_replace is list and value is None."""
        data = [{"name": "Alice"}]
        df = spark.createDataFrame(data)

        with pytest.raises(Exception) as exc_info:
            df.na.replace(["Alice", "Bob"])

        assert "value is required" in str(exc_info.value)


class TestApproxCountDistinctSingleDefinition(ParityTestBase):
    """Test that approx_count_distinct has consistent behavior."""

    def test_approx_count_distinct_default_rsd(self, spark):
        """Test approx_count_distinct uses default rsd=0.05."""
        imports = get_imports()
        F = imports.F

        data = [
            {"value": 1},
            {"value": 2},
            {"value": 1},
            {"value": 3},
        ]
        df = spark.createDataFrame(data)

        result = df.agg(F.approx_count_distinct("value").alias("count"))
        rows = result.collect()

        assert len(rows) == 1
        assert rows[0]["count"] == 3  # Distinct: 1, 2, 3

    def test_approx_count_distinct_with_rsd(self, spark):
        """Test approx_count_distinct accepts rsd parameter."""
        imports = get_imports()
        F = imports.F

        data = [
            {"value": 1},
            {"value": 2},
            {"value": 3},
        ]
        df = spark.createDataFrame(data)

        result = df.agg(F.approx_count_distinct("value", rsd=0.01).alias("count"))
        rows = result.collect()

        assert len(rows) == 1
        assert rows[0]["count"] == 3

    def test_approx_count_distinct_in_groupby(self, spark):
        """Test approx_count_distinct works in groupBy aggregation."""
        imports = get_imports()
        F = imports.F

        data = [
            {"group": "A", "value": 1},
            {"group": "A", "value": 2},
            {"group": "A", "value": 1},
            {"group": "B", "value": 10},
            {"group": "B", "value": 20},
        ]
        df = spark.createDataFrame(data)

        result = df.groupby("group").agg(
            F.approx_count_distinct("value").alias("distinct_count")
        )
        rows = result.collect()

        assert len(rows) == 2
        row_dict = {r["group"]: r["distinct_count"] for r in rows}
        assert row_dict["A"] == 2
        assert row_dict["B"] == 2

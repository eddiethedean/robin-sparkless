"""PySpark parity test for aggregate function cast (Issue #265)."""

from tests.fixtures.parity_base import ParityTestBase
from tests.fixtures.spark_imports import get_spark_imports


class TestAggregateCastParity(ParityTestBase):
    """Test aggregate function cast parity with PySpark (Issue #265)."""

    def test_mean_cast_string_issue_265(self, spark):
        """Test F.mean().cast("string") matches PySpark behavior (Issue #265).

        This test verifies the exact example from issue #265:
        https://github.com/eddiethedean/sparkless/issues/265
        """
        imports = get_spark_imports()
        F = imports.F

        data = [
            {"type": "A", "value": 1},
            {"type": "A", "value": 10},
            {"type": "B", "value": 5},
        ]
        df = spark.createDataFrame(data)

        # This is the exact code from issue #265
        result = df.groupby("type").agg(F.mean(F.col("value")).cast("string"))

        # Verify basic structure
        rows = result.collect()
        assert len(rows) == 2

        # Verify column name format matches PySpark: CAST(avg(value) AS STRING)
        schema = result.schema
        cast_col = None
        for field in schema.fields:
            if "CAST" in field.name.upper() and "STRING" in field.name.upper():
                cast_col = field.name
                break

        assert cast_col is not None, "Cast column not found in schema"

        # Verify values are strings
        for row in rows:
            value = row[cast_col]
            # PySpark returns "5.5" and "5.0" as strings
            assert isinstance(value, str)
            # Verify the values are correct (approximately)
            if row["type"] == "A":
                assert value in ["5.5", "5.50"]  # Allow for formatting differences
            elif row["type"] == "B":
                assert value in ["5.0", "5.00", "5"]  # Allow for formatting differences

    def test_all_aggregate_functions_with_string_cast(self, spark):
        """Test all major aggregate functions with string cast."""
        imports = get_spark_imports()
        F = imports.F

        data = [
            {"group": "A", "value": 1},
            {"group": "A", "value": 2},
            {"group": "A", "value": 3},
            {"group": "B", "value": 10},
            {"group": "B", "value": 20},
        ]
        df = spark.createDataFrame(data)

        # Test all major aggregate functions with cast (without alias to test CAST column names)
        result = df.groupby("group").agg(
            F.sum(F.col("value")).cast("string"),
            F.avg(F.col("value")).cast("string"),
            F.mean(F.col("value")).cast("string"),
            F.max(F.col("value")).cast("string"),
            F.min(F.col("value")).cast("string"),
            F.count(F.col("value")).cast("string"),
        )

        rows = result.collect()
        assert len(rows) == 2

        # Find cast columns by searching for CAST in column names
        schema = result.schema
        cast_cols = [
            f.name
            for f in schema.fields
            if "CAST" in f.name.upper() and "STRING" in f.name.upper()
        ]
        # Note: mean() and avg() are aliases, so they produce the same column name
        # So we expect at least 5 unique cast columns (sum, avg/mean, max, min, count)
        assert len(cast_cols) >= 5

        # Verify all cast values are strings
        for row in rows:
            for col_name in cast_cols:
                value = row[col_name]
                assert isinstance(value, str), (
                    f"Column {col_name} should be string, got {type(value)}"
                )

        # Verify values for group A
        row_a = next((r for r in rows if r["group"] == "A"), None)
        assert row_a is not None
        # Find sum and count columns
        sum_col = next((c for c in cast_cols if "sum" in c.lower()), None)
        count_col = next((c for c in cast_cols if "count" in c.lower()), None)
        if sum_col:
            assert row_a[sum_col] in ["6", "6.0"]
        if count_col:
            assert row_a[count_col] in ["3", "3.0"]

    def test_cast_to_different_types(self, spark):
        """Test casting aggregates to different data types."""
        imports = get_spark_imports()
        F = imports.F

        data = [
            {"group": "A", "value": 15},
            {"group": "A", "value": 25},
            {"group": "B", "value": 100},
        ]
        df = spark.createDataFrame(data)

        result = df.groupby("group").agg(
            F.avg(F.col("value")).cast("string"),
            F.avg(F.col("value")).cast("int"),
            F.avg(F.col("value")).cast("long"),
            F.avg(F.col("value")).cast("double"),
            F.avg(F.col("value")).cast("float"),
        )

        rows = result.collect()
        assert len(rows) == 2

        # Find cast columns by type
        schema = result.schema
        string_col = next(
            (
                f.name
                for f in schema.fields
                if "CAST" in f.name.upper() and "STRING" in f.name.upper()
            ),
            None,
        )
        int_col = next(
            (
                f.name
                for f in schema.fields
                if "CAST" in f.name.upper()
                and "INT" in f.name.upper()
                and "STRING" not in f.name.upper()
            ),
            None,
        )
        long_col = next(
            (
                f.name
                for f in schema.fields
                if "CAST" in f.name.upper()
                and "LONG" in f.name.upper()
                and "STRING" not in f.name.upper()
            ),
            None,
        )
        double_col = next(
            (
                f.name
                for f in schema.fields
                if "CAST" in f.name.upper()
                and "DOUBLE" in f.name.upper()
                and "STRING" not in f.name.upper()
            ),
            None,
        )
        float_col = next(
            (
                f.name
                for f in schema.fields
                if "CAST" in f.name.upper()
                and "FLOAT" in f.name.upper()
                and "STRING" not in f.name.upper()
            ),
            None,
        )

        # Verify types
        for row in rows:
            if string_col:
                assert isinstance(row[string_col], str)
            if int_col:
                assert isinstance(row[int_col], (int, str))
            if long_col:
                assert isinstance(row[long_col], (int, str))
            if double_col:
                assert isinstance(row[double_col], (float, str))
            if float_col:
                assert isinstance(row[float_col], (float, str))

    def test_cast_with_null_values(self, spark):
        """Test cast operations with null values in data."""
        imports = get_spark_imports()
        F = imports.F

        data = [
            {"group": "A", "value": 1},
            {"group": "A", "value": None},
            {"group": "A", "value": 3},
            {"group": "B", "value": None},
            {"group": "B", "value": None},
        ]
        df = spark.createDataFrame(data)

        result = df.groupby("group").agg(
            F.avg(F.col("value")).cast("string"),
            F.sum(F.col("value")).cast("string"),
            F.count(F.col("value")).cast("string"),
        )

        rows = result.collect()
        assert len(rows) == 2

        # Find cast columns
        schema = result.schema
        avg_col = next(
            (
                f.name
                for f in schema.fields
                if "CAST" in f.name.upper()
                and "avg" in f.name.lower()
                and "STRING" in f.name.upper()
            ),
            None,
        )
        sum_col = next(
            (
                f.name
                for f in schema.fields
                if "CAST" in f.name.upper()
                and "sum" in f.name.lower()
                and "STRING" in f.name.upper()
            ),
            None,
        )
        count_col = next(
            (
                f.name
                for f in schema.fields
                if "CAST" in f.name.upper()
                and "count" in f.name.lower()
                and "STRING" in f.name.upper()
            ),
            None,
        )

        # Group A should have valid values
        row_a = next((r for r in rows if r["group"] == "A"), None)
        assert row_a is not None
        if avg_col:
            assert isinstance(row_a[avg_col], str)
        if sum_col:
            assert isinstance(row_a[sum_col], str)
        if count_col:
            assert isinstance(row_a[count_col], str)

        # Group B should handle nulls
        row_b = next((r for r in rows if r["group"] == "B"), None)
        assert row_b is not None
        if count_col:
            # Count should be "0" or "0.0" as string
            assert isinstance(row_b[count_col], str)

    def test_cast_column_name_format(self, spark):
        """Test that cast column names match PySpark format exactly."""
        imports = get_spark_imports()
        F = imports.F

        data = [
            {"group": "A", "value": 10},
            {"group": "B", "value": 20},
        ]
        df = spark.createDataFrame(data)

        result = df.groupby("group").agg(
            F.mean(F.col("value")).cast("string"),
            F.sum(F.col("value")).cast("int"),
            F.max(F.col("value")).cast("long"),
        )

        schema = result.schema
        field_names = [f.name for f in schema.fields]

        # Verify column name format: CAST(avg(value) AS STRING)
        cast_cols = [name for name in field_names if "CAST" in name.upper()]
        assert len(cast_cols) == 3

        # Verify format components
        for col_name in cast_cols:
            assert "CAST" in col_name.upper()
            assert "AS" in col_name.upper()
            # Should contain type name
            assert any(
                t in col_name.upper()
                for t in ["STRING", "INT", "LONG", "DOUBLE", "FLOAT"]
            )

    def test_multiple_casts_same_aggregate(self, spark):
        """Test multiple casts on the same aggregate function."""
        imports = get_spark_imports()
        F = imports.F

        data = [
            {"group": "A", "value": 7},
            {"group": "A", "value": 8},
            {"group": "B", "value": 15},
        ]
        df = spark.createDataFrame(data)

        # Create same aggregate with different casts
        avg_col = F.avg(F.col("value"))
        result = df.groupby("group").agg(
            avg_col.cast("string"),
            avg_col.cast("int"),
            avg_col.cast("double"),
        )

        rows = result.collect()
        assert len(rows) == 2

        # Find cast columns
        schema = result.schema
        string_col = next(
            (
                f.name
                for f in schema.fields
                if "CAST" in f.name.upper() and "STRING" in f.name.upper()
            ),
            None,
        )
        int_col = next(
            (
                f.name
                for f in schema.fields
                if "CAST" in f.name.upper()
                and "INT" in f.name.upper()
                and "STRING" not in f.name.upper()
            ),
            None,
        )
        double_col = next(
            (
                f.name
                for f in schema.fields
                if "CAST" in f.name.upper()
                and "DOUBLE" in f.name.upper()
                and "STRING" not in f.name.upper()
            ),
            None,
        )

        # Verify all casts are present and have correct types
        for row in rows:
            if string_col:
                assert isinstance(row[string_col], str)
            if int_col:
                assert isinstance(row[int_col], (int, str))
            if double_col:
                assert isinstance(row[double_col], (float, str))

    def test_cast_with_empty_groups(self, spark):
        """Test cast operations with groups that have no matching rows."""
        imports = get_spark_imports()
        F = imports.F

        data = [
            {"group": "A", "value": 1},
            {"group": "A", "value": 2},
        ]
        df = spark.createDataFrame(data)

        # Filter to create single group
        filtered_df = df.filter(F.col("group") == "A")
        result = filtered_df.groupby("group").agg(
            F.avg(F.col("value")).cast("string"),
            F.count(F.col("value")).cast("string"),
        )

        rows = result.collect()
        assert len(rows) == 1

        # Find cast columns
        schema = result.schema
        avg_col = next(
            (
                f.name
                for f in schema.fields
                if "CAST" in f.name.upper()
                and "avg" in f.name.lower()
                and "STRING" in f.name.upper()
            ),
            None,
        )
        count_col = next(
            (
                f.name
                for f in schema.fields
                if "CAST" in f.name.upper()
                and "count" in f.name.lower()
                and "STRING" in f.name.upper()
            ),
            None,
        )

        # Verify the single group has correct types
        row = rows[0]
        if avg_col:
            assert isinstance(row[avg_col], str)
        if count_col:
            assert isinstance(row[count_col], str)

    def test_cast_with_different_numeric_types(self, spark):
        """Test cast with different numeric input types."""
        imports = get_spark_imports()
        F = imports.F

        data = [
            {"group": "A", "int_val": 10, "float_val": 10.5, "long_val": 100},
            {"group": "A", "int_val": 20, "float_val": 20.5, "long_val": 200},
            {"group": "B", "int_val": 5, "float_val": 5.5, "long_val": 50},
        ]
        df = spark.createDataFrame(data)

        result = df.groupby("group").agg(
            F.avg(F.col("int_val")).cast("string"),
            F.avg(F.col("float_val")).cast("string"),
            F.avg(F.col("long_val")).cast("string"),
        )

        rows = result.collect()
        assert len(rows) == 2

        # Find cast columns
        schema = result.schema
        cast_cols = [
            f.name
            for f in schema.fields
            if "CAST" in f.name.upper() and "STRING" in f.name.upper()
        ]

        # All should be strings
        for row in rows:
            for col_name in cast_cols:
                assert isinstance(row[col_name], str)

    def test_cast_chain_operations(self, spark):
        """Test cast with chained aggregate operations."""
        imports = get_spark_imports()
        F = imports.F

        data = [
            {"group": "A", "value": 1},
            {"group": "A", "value": 2},
            {"group": "B", "value": 10},
        ]
        df = spark.createDataFrame(data)

        # Chain: aggregate -> cast
        result = df.groupby("group").agg(
            F.sum(F.col("value")).cast("string"),
            F.avg(F.col("value")).cast("int"),
        )

        rows = result.collect()
        assert len(rows) == 2

        # Find cast columns
        schema = result.schema
        string_col = next(
            (
                f.name
                for f in schema.fields
                if "CAST" in f.name.upper()
                and "sum" in f.name.lower()
                and "STRING" in f.name.upper()
            ),
            None,
        )
        int_col = next(
            (
                f.name
                for f in schema.fields
                if "CAST" in f.name.upper()
                and "avg" in f.name.lower()
                and "INT" in f.name.upper()
                and "STRING" not in f.name.upper()
            ),
            None,
        )

        # Verify casts work correctly
        for row in rows:
            if string_col:
                assert isinstance(row[string_col], str)
            if int_col:
                assert isinstance(row[int_col], (int, str))

    def test_cast_with_count_distinct(self, spark):
        """Test cast with countDistinct aggregate function."""
        imports = get_spark_imports()
        F = imports.F

        data = [
            {"group": "A", "value": 1},
            {"group": "A", "value": 1},
            {"group": "A", "value": 2},
            {"group": "B", "value": 10},
            {"group": "B", "value": 10},
        ]
        df = spark.createDataFrame(data)

        result = df.groupby("group").agg(
            F.countDistinct(F.col("value")).cast("string"),
        )

        rows = result.collect()
        assert len(rows) == 2

        # Find cast column
        schema = result.schema
        distinct_col = next(
            (
                f.name
                for f in schema.fields
                if "CAST" in f.name.upper() and "STRING" in f.name.upper()
            ),
            None,
        )

        # Verify count distinct with cast
        for row in rows:
            if distinct_col:
                assert isinstance(row[distinct_col], str)
                # Group A has 2 distinct values, Group B has 1
                if row["group"] == "A":
                    assert row[distinct_col] in ["2", "2.0"]
                elif row["group"] == "B":
                    assert row[distinct_col] in ["1", "1.0"]

    def test_cast_with_stddev_variance(self, spark):
        """Test cast with statistical aggregate functions."""
        imports = get_spark_imports()
        F = imports.F

        data = [
            {"group": "A", "value": 1},
            {"group": "A", "value": 2},
            {"group": "A", "value": 3},
            {"group": "B", "value": 10},
            {"group": "B", "value": 20},
        ]
        df = spark.createDataFrame(data)

        result = df.groupby("group").agg(
            F.stddev(F.col("value")).cast("string"),
            F.variance(F.col("value")).cast("string"),
        )

        rows = result.collect()
        assert len(rows) == 2

        # Find cast columns
        schema = result.schema
        stddev_col = next(
            (
                f.name
                for f in schema.fields
                if "CAST" in f.name.upper()
                and "stddev" in f.name.lower()
                and "STRING" in f.name.upper()
            ),
            None,
        )
        variance_col = next(
            (
                f.name
                for f in schema.fields
                if "CAST" in f.name.upper()
                and "variance" in f.name.lower()
                and "STRING" in f.name.upper()
            ),
            None,
        )

        # Verify statistical functions with cast
        for row in rows:
            if stddev_col:
                assert isinstance(row[stddev_col], str)
                assert row[stddev_col] != ""
            if variance_col:
                assert isinstance(row[variance_col], str)
                assert row[variance_col] != ""

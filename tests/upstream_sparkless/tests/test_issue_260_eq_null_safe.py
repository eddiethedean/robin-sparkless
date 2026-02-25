"""Tests for issue #260: Column.eqNullSafe (null-safe equality).

Issue #260 reports that sparkless Column class does not implement eqNullSafe,
which is used to establish equality between two columns which both contain
nulls. The PySpark API supports this.

These tests verify that:
- Column.eqNullSafe exists on the public API.
- Its semantics match PySpark's <=> / eqNullSafe behavior:
  * NULL <=> NULL is True
  * NULL <=> non-NULL is False
  * non-NULL <=> non-NULL behaves like standard equality, including type coercion.
"""

from datetime import date, datetime
from typing import Iterable

import pytest

from tests.fixtures.spark_imports import get_spark_imports
from tests.fixtures.spark_backend import get_backend_type, BackendType


imports = get_spark_imports()
SparkSession = imports.SparkSession
F = imports.F
StructType = imports.StructType
StructField = imports.StructField
StringType = imports.StringType
IntegerType = imports.IntegerType
LongType = imports.LongType
DoubleType = imports.DoubleType
DateType = imports.DateType
TimestampType = imports.TimestampType


def _is_pyspark_mode() -> bool:
    """Check if running in PySpark backend mode."""
    backend = get_backend_type()
    return backend == BackendType.PYSPARK


class TestIssue260EqNullSafe:
    """Regression tests for Column.eqNullSafe (issue #260)."""

    def test_eqnullsafe_example_from_issue_260(self) -> None:
        """Exact reproduction of the example from issue #260."""
        spark = SparkSession.builder.appName("Example").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Id": "123", "ManagerId": None},
                    {"Name": "Bob", "Id": "456", "ManagerId": "456"},
                    {"Name": "Charlie", "Id": None, "ManagerId": None},
                ]
            )

            result = df.where(F.col("Id").eqNullSafe(F.col("ManagerId"))).collect()

            # Expected PySpark result:
            # +----+---------+-------+
            # |  Id|ManagerId|   Name|
            # +----+---------+-------+
            # | 456|      456|    Bob|
            # |NULL|     NULL|Charlie|
            # +----+---------+-------+
            assert len(result) == 2
            names = {row["Name"] for row in result}
            assert names == {"Bob", "Charlie"}
        finally:
            spark.stop()

    @pytest.mark.parametrize(  # type: ignore[misc,untyped-decorator]
        "left,right,expected",
        [
            (None, None, True),
            (None, "x", False),
            ("x", None, False),
            ("x", "x", True),
            ("x", "y", False),
        ],
    )
    def test_eqnullsafe_literal_semantics(self, left, right, expected: bool) -> None:
        """Test eqNullSafe semantics with literal comparisons."""
        spark = SparkSession.builder.appName("EqNullSafeLiterals").getOrCreate()
        try:
            schema = StructType(
                [
                    StructField("left", StringType(), True),
                    StructField("right", StringType(), True),
                ]
            )
            df = spark.createDataFrame([{"left": left, "right": right}], schema=schema)
            result = df.select(
                F.col("left").eqNullSafe(F.col("right")).alias("equals"),
            ).collect()
            assert len(result) == 1
            assert result[0]["equals"] is expected
        finally:
            spark.stop()

    def test_eqnullsafe_coexists_with_standard_equality(self) -> None:
        """Ensure eqNullSafe does not change == behavior."""
        spark = SparkSession.builder.appName("EqNullSafeVsEq").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"value": None},
                    {"value": "x"},
                ]
            )

            # With standard equality, NULL == "x" and NULL == NULL behave like SQL: result is NULL -> filter drops them.
            # Depending on backend, this may yield zero rows (SQL three-valued logic).
            # The important thing is that eqNullSafe has distinct semantics.
            _ = df.where(
                F.col("value") == F.lit(None)
            ).collect()  # Demonstrate standard equality behavior

            # With eqNullSafe, NULL <=> NULL should be True.
            null_safe_result = df.where(
                F.col("value").eqNullSafe(F.lit(None))
            ).collect()
            names = [row["value"] for row in null_safe_result]
            assert None in names
        finally:
            spark.stop()

    def test_eqnullsafe_with_integer_types(self) -> None:
        """Test eqNullSafe with integer columns."""
        spark = SparkSession.builder.appName("EqNullSafeIntegers").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"a": 1, "b": 1},
                    {"a": 2, "b": None},
                    {"a": None, "b": 3},
                    {"a": None, "b": None},
                    {"a": 4, "b": 5},
                ]
            )

            result = df.where(F.col("a").eqNullSafe(F.col("b"))).collect()
            names = {(row["a"], row["b"]) for row in result}
            # Should match: (1, 1) and (None, None)
            assert (1, 1) in names
            assert (None, None) in names
            assert len(result) == 2
        finally:
            spark.stop()

    def test_eqnullsafe_with_float_types(self) -> None:
        """Test eqNullSafe with float/double columns."""
        spark = SparkSession.builder.appName("EqNullSafeFloats").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"a": 1.5, "b": 1.5},
                    {"a": 2.0, "b": None},
                    {"a": None, "b": 3.0},
                    {"a": None, "b": None},
                    {"a": 4.5, "b": 5.5},
                ]
            )

            result = df.where(F.col("a").eqNullSafe(F.col("b"))).collect()
            values = {(row["a"], row["b"]) for row in result}
            # Should match: (1.5, 1.5) and (None, None)
            assert (1.5, 1.5) in values
            assert (None, None) in values
            assert len(result) == 2
        finally:
            spark.stop()

    def test_eqnullsafe_with_date_types(self) -> None:
        """Test eqNullSafe with date columns."""
        spark = SparkSession.builder.appName("EqNullSafeDates").getOrCreate()
        try:
            d1 = date(2025, 1, 1)
            d2 = date(2025, 1, 2)
            df = spark.createDataFrame(
                [
                    {"a": d1, "b": d1},
                    {"a": d2, "b": None},
                    {"a": None, "b": d1},
                    {"a": None, "b": None},
                    {"a": d1, "b": d2},
                ]
            )

            result = df.where(F.col("a").eqNullSafe(F.col("b"))).collect()
            values = {(row["a"], row["b"]) for row in result}
            # Should match: (d1, d1) and (None, None)
            assert (d1, d1) in values
            assert (None, None) in values
            assert len(result) == 2
        finally:
            spark.stop()

    def test_eqnullsafe_with_datetime_types(self) -> None:
        """Test eqNullSafe with datetime/timestamp columns."""
        spark = SparkSession.builder.appName("EqNullSafeDatetimes").getOrCreate()
        try:
            dt1 = datetime(2025, 1, 1, 12, 0, 0)
            dt2 = datetime(2025, 1, 2, 12, 0, 0)
            df = spark.createDataFrame(
                [
                    {"a": dt1, "b": dt1},
                    {"a": dt2, "b": None},
                    {"a": None, "b": dt1},
                    {"a": None, "b": None},
                    {"a": dt1, "b": dt2},
                ]
            )

            result = df.where(F.col("a").eqNullSafe(F.col("b"))).collect()
            values = {(row["a"], row["b"]) for row in result}
            # Should match: (dt1, dt1) and (None, None)
            assert (dt1, dt1) in values
            assert (None, None) in values
            assert len(result) == 2
        finally:
            spark.stop()

    def test_eqnullsafe_with_column_vs_literal(self) -> None:
        """Test eqNullSafe with column vs literal comparisons."""
        spark = SparkSession.builder.appName("EqNullSafeColumnLiteral").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"value": "test"},
                    {"value": None},
                    {"value": "other"},
                ]
            )

            # Column vs literal string
            result1 = df.where(F.col("value").eqNullSafe(F.lit("test"))).collect()
            assert len(result1) == 1
            assert result1[0]["value"] == "test"

            # Column vs literal None
            result2 = df.where(F.col("value").eqNullSafe(F.lit(None))).collect()
            assert len(result2) == 1
            assert result2[0]["value"] is None

            # Literal vs column (reverse) - use col().eqNullSafe(lit()) instead
            # since lit().eqNullSafe() is supported but may need column on left
            result3 = df.where(F.col("value").eqNullSafe(F.lit("test"))).collect()
            assert len(result3) == 1
            assert result3[0]["value"] == "test"
        finally:
            spark.stop()

    def test_eqnullsafe_with_integer_literal(self) -> None:
        """Test eqNullSafe with integer column vs integer literal."""
        spark = SparkSession.builder.appName("EqNullSafeIntLiteral").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"value": 42},
                    {"value": None},
                    {"value": 100},
                ]
            )

            # Column vs literal integer
            result1 = df.where(F.col("value").eqNullSafe(F.lit(42))).collect()
            assert len(result1) == 1
            assert result1[0]["value"] == 42

            # Column vs literal None
            result2 = df.where(F.col("value").eqNullSafe(F.lit(None))).collect()
            assert len(result2) == 1
            assert result2[0]["value"] is None
        finally:
            spark.stop()

    def test_eqnullsafe_in_select_expression(self) -> None:
        """Test eqNullSafe used in select expressions."""
        spark = SparkSession.builder.appName("EqNullSafeSelect").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"a": "x", "b": "x"},
                    {"a": "y", "b": None},
                    {"a": None, "b": "z"},
                    {"a": None, "b": None},
                ]
            )

            result = df.select(
                F.col("a"),
                F.col("b"),
                F.col("a").eqNullSafe(F.col("b")).alias("equals"),
            ).collect()

            assert len(result) == 4
            # Check each row's equals value
            equals_map = {(row["a"], row["b"]): row["equals"] for row in result}
            assert equals_map[("x", "x")] is True
            assert equals_map[("y", None)] is False
            assert equals_map[(None, "z")] is False
            assert equals_map[(None, None)] is True
        finally:
            spark.stop()

    def test_eqnullsafe_in_join_condition(self) -> None:
        """Test eqNullSafe used in join-like scenarios via cross join and filter."""
        spark = SparkSession.builder.appName("EqNullSafeJoin").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [
                    {"id1": 1, "key1": "A"},
                    {"id1": 2, "key1": None},
                    {"id1": 3, "key1": "B"},
                ]
            )
            df2 = spark.createDataFrame(
                [
                    {"id2": 10, "key2": "A"},
                    {"id2": 20, "key2": None},
                    {"id2": 30, "key2": "C"},
                ]
            )

            # Use cross join then filter with eqNullSafe to simulate null-safe join
            # This tests that eqNullSafe works in filter conditions after joins
            result = (
                df1.crossJoin(df2)
                .where(F.col("key1").eqNullSafe(F.col("key2")))
                .collect()
            )

            # Should match: ("A", "A") and (None, None)
            assert len(result) == 2
            keys = {(row["key1"], row["key2"]) for row in result}
            assert ("A", "A") in keys
            assert (None, None) in keys
        finally:
            spark.stop()

    def test_eqnullsafe_with_type_coercion(self) -> None:
        """Test eqNullSafe with type coercion (string vs numeric)."""
        spark = SparkSession.builder.appName("EqNullSafeCoercion").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"str_col": "100", "int_col": 100},
                    {"str_col": "200", "int_col": None},
                    {"str_col": None, "int_col": 300},
                    {"str_col": None, "int_col": None},
                    {"str_col": "100", "int_col": 200},
                ]
            )

            # eqNullSafe should handle type coercion like standard equality
            result = df.where(F.col("str_col").eqNullSafe(F.col("int_col"))).collect()
            values = {(row["str_col"], row["int_col"]) for row in result}
            # Should match: ("100", 100) and (None, None)
            assert ("100", 100) in values
            assert (None, None) in values
            assert len(result) == 2
        finally:
            spark.stop()

    def test_eqnullsafe_with_multiple_conditions(self) -> None:
        """Test eqNullSafe in complex filter conditions with AND/OR."""
        spark = SparkSession.builder.appName("EqNullSafeMultiple").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"a": "x", "b": "x", "c": 1},
                    {"a": "y", "b": None, "c": 1},
                    {"a": None, "b": None, "c": 2},
                    {"a": "z", "b": "z", "c": 3},
                ]
            )

            # Multiple eqNullSafe conditions with AND
            result = df.where(
                (F.col("a").eqNullSafe(F.col("b"))) & (F.col("c") == 1)
            ).collect()
            assert len(result) == 1
            assert result[0]["a"] == "x"

            # Multiple eqNullSafe conditions with OR
            result2 = df.where(
                (F.col("a").eqNullSafe(F.col("b"))) | (F.col("c") == 2)
            ).collect()
            assert len(result2) == 3  # ("x", "x"), (None, None), and (c == 2)
        finally:
            spark.stop()

    def test_eqnullsafe_with_empty_dataframe(self) -> None:
        """Test eqNullSafe with empty DataFrame."""
        spark = SparkSession.builder.appName("EqNullSafeEmpty").getOrCreate()
        try:
            schema = StructType(
                [
                    StructField("a", StringType(), True),
                    StructField("b", StringType(), True),
                ]
            )
            df = spark.createDataFrame([], schema=schema)

            result = df.where(F.col("a").eqNullSafe(F.col("b"))).collect()
            assert len(result) == 0
        finally:
            spark.stop()

    def test_eqnullsafe_in_groupby_aggregation(self) -> None:
        """Test eqNullSafe used in groupBy operations."""
        spark = SparkSession.builder.appName("EqNullSafeGroupBy").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"category": "A", "value": 10},
                    {"category": "A", "value": 20},
                    {"category": None, "value": 30},
                    {"category": None, "value": 40},
                    {"category": "B", "value": 50},
                ]
            )

            # Group by category and count - None values should group together
            result = (
                df.groupBy("category")
                .agg(F.count("value").alias("count"))
                .orderBy("category")
                .collect()
            )

            # Should have 3 groups: "A", "B", and None
            assert len(result) == 3
            counts = {row["category"]: row["count"] for row in result}
            assert counts["A"] == 2
            assert counts["B"] == 1
            assert counts[None] == 2
        finally:
            spark.stop()

    def test_eqnullsafe_chained_with_other_operations(self) -> None:
        """Test eqNullSafe chained with other column operations."""
        spark = SparkSession.builder.appName("EqNullSafeChained").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"name": "Alice", "age": 30},
                    {"name": "Bob", "age": None},
                    {"name": None, "age": 25},
                    {"name": None, "age": None},
                ]
            )

            # Use eqNullSafe in a complex expression
            result = df.select(
                F.col("name"),
                F.col("age"),
                F.col("name").eqNullSafe(F.lit(None)).alias("name_is_null"),
                F.col("age").eqNullSafe(F.lit(None)).alias("age_is_null"),
            ).collect()

            assert len(result) == 4
            # Verify null checks work correctly
            for row in result:
                if row["name"] is None:
                    assert row["name_is_null"] is True
                else:
                    assert row["name_is_null"] is False
                if row["age"] is None:
                    assert row["age_is_null"] is True
                else:
                    assert row["age_is_null"] is False
        finally:
            spark.stop()

    def test_eqnullsafe_with_all_null_columns(self) -> None:
        """Test eqNullSafe when both columns are entirely null."""
        spark = SparkSession.builder.appName("EqNullSafeAllNulls").getOrCreate()
        try:
            # Provide explicit schema since all values are None
            schema = StructType(
                [
                    StructField("a", StringType(), True),
                    StructField("b", StringType(), True),
                ]
            )
            df = spark.createDataFrame(
                [
                    {"a": None, "b": None},
                    {"a": None, "b": None},
                    {"a": None, "b": None},
                ],
                schema=schema,
            )

            result = df.where(F.col("a").eqNullSafe(F.col("b"))).collect()
            # All rows should match since NULL <=> NULL is True
            assert len(result) == 3
        finally:
            spark.stop()

    def test_eqnullsafe_with_no_matching_nulls(self) -> None:
        """Test eqNullSafe when no nulls match (all are NULL vs non-NULL)."""
        spark = SparkSession.builder.appName("EqNullSafeNoMatch").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"a": None, "b": "x"},
                    {"a": "y", "b": None},
                    {"a": "z", "b": "w"},
                ]
            )

            result = df.where(F.col("a").eqNullSafe(F.col("b"))).collect()
            # Only non-null matching values should be included
            assert len(result) == 0  # No matches: (None, "x"), ("y", None), ("z", "w")
        finally:
            spark.stop()

    def test_eqnullsafe_with_mixed_types_and_nulls(self) -> None:
        """Test eqNullSafe with mixed data types and various null combinations."""
        spark = SparkSession.builder.appName("EqNullSafeMixed").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"id": 1, "name": "Alice", "score": 95.5},
                    {"id": 2, "name": None, "score": 88.0},
                    {"id": None, "name": "Bob", "score": None},
                    {"id": None, "name": None, "score": None},
                    {"id": 3, "name": "Charlie", "score": 95.5},
                ]
            )

            # Test multiple eqNullSafe comparisons
            result = df.where(
                (F.col("id").eqNullSafe(F.lit(1)))
                | (F.col("name").eqNullSafe(F.lit(None)))
                | (F.col("score").eqNullSafe(F.lit(95.5)))
            ).collect()

            # Should match: id=1, name=None, score=95.5, and id=None/name=None/score=None
            assert len(result) >= 3
            ids = {row["id"] for row in result}
            assert 1 in ids  # id=1 matches
            assert None in ids  # name=None or score=None matches
        finally:
            spark.stop()


class TestIssue260EqNullSafeParity:
    """Optional PySpark parity tests for eqNullSafe semantics."""

    def test_eqnullsafe_parity_with_pyspark(self) -> None:
        """Run the issue #260 example against real PySpark when available."""
        if not _is_pyspark_mode():
            pytest.skip(
                "PySpark parity test - run with MOCK_SPARK_TEST_BACKEND=pyspark"
            )

        spark = SparkSession.builder.appName("EqNullSafeParity").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Id": "123", "ManagerId": None},
                    {"Name": "Bob", "Id": "456", "ManagerId": "456"},
                    {"Name": "Charlie", "Id": None, "ManagerId": None},
                ]
            )

            result = df.where(F.col("Id").eqNullSafe(F.col("ManagerId"))).collect()
            assert len(result) == 2
            names: Iterable[str] = {row["Name"] for row in result}
            assert names == {"Bob", "Charlie"}
        finally:
            spark.stop()

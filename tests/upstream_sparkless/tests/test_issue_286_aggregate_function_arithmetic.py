"""
Tests for issue #286: AggregateFunction arithmetic operations.

PySpark allows arithmetic operations on aggregate functions (e.g., F.countDistinct() - 1).
This test verifies that Sparkless supports the same operations.
"""

from sparkless.sql import SparkSession
import sparkless.sql.functions as F


class TestIssue286AggregateFunctionArithmetic:
    """Test arithmetic operations on aggregate functions."""

    def test_count_distinct_minus_one(self):
        """Test subtracting from countDistinct aggregate function."""
        spark = SparkSession.builder.appName("issue-286").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 2},
                    {"Name": "Bob", "Value": 3},
                ]
            )

            # Subtract 1 from countDistinct - should work without TypeError
            result = df.groupBy("Name").agg(
                (F.countDistinct("Value") - 1).alias("count_minus_one")
            )

            rows = result.collect()
            assert len(rows) == 2

            # Alice has 2 distinct values (1, 2), so countDistinct - 1 = 1
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["count_minus_one"] == 1

            # Bob has 1 distinct value (3), so countDistinct - 1 = 0
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["count_minus_one"] == 0

            # Verify column name
            assert "count_minus_one" in result.columns
        finally:
            spark.stop()

    def test_count_plus_one(self):
        """Test adding to count aggregate function."""
        spark = SparkSession.builder.appName("issue-286").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 2},
                    {"Name": "Bob", "Value": 3},
                ]
            )

            result = df.groupBy("Name").agg(
                (F.count("Value") + 1).alias("count_plus_one")
            )

            rows = result.collect()
            assert len(rows) == 2

            # Alice has 2 rows, so count + 1 = 3
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["count_plus_one"] == 3

            # Bob has 1 row, so count + 1 = 2
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["count_plus_one"] == 2
        finally:
            spark.stop()

    def test_sum_multiply(self):
        """Test multiplying sum aggregate function."""
        spark = SparkSession.builder.appName("issue-286").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 2},
                    {"Name": "Bob", "Value": 3},
                ]
            )

            result = df.groupBy("Name").agg((F.sum("Value") * 2).alias("sum_times_two"))

            rows = result.collect()
            assert len(rows) == 2

            # Alice: sum = 3, so 3 * 2 = 6
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["sum_times_two"] == 6

            # Bob: sum = 3, so 3 * 2 = 6
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["sum_times_two"] == 6
        finally:
            spark.stop()

    def test_avg_divide(self):
        """Test dividing avg aggregate function."""
        spark = SparkSession.builder.appName("issue-286").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Alice", "Value": 20},
                    {"Name": "Bob", "Value": 30},
                ]
            )

            result = df.groupBy("Name").agg(
                (F.avg("Value") / 2).alias("avg_divided_by_two")
            )

            rows = result.collect()
            assert len(rows) == 2

            # Alice: avg = 15, so 15 / 2 = 7.5
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["avg_divided_by_two"] == 7.5

            # Bob: avg = 30, so 30 / 2 = 15.0
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["avg_divided_by_two"] == 15.0
        finally:
            spark.stop()

    def test_max_modulo(self):
        """Test modulo operation on max aggregate function."""
        spark = SparkSession.builder.appName("issue-286").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 7},
                    {"Name": "Alice", "Value": 8},
                    {"Name": "Bob", "Value": 9},
                ]
            )

            result = df.groupBy("Name").agg((F.max("Value") % 3).alias("max_mod_three"))

            rows = result.collect()
            assert len(rows) == 2

            # Alice: max = 8, so 8 % 3 = 2
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["max_mod_three"] == 2

            # Bob: max = 9, so 9 % 3 = 0
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["max_mod_three"] == 0
        finally:
            spark.stop()

    def test_reverse_operations(self):
        """Test reverse arithmetic operations (e.g., 1 - countDistinct)."""
        spark = SparkSession.builder.appName("issue-286").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 2},
                    {"Name": "Bob", "Value": 3},
                ]
            )

            # Test reverse subtraction: 10 - countDistinct
            result = df.groupBy("Name").agg(
                (10 - F.countDistinct("Value")).alias("ten_minus_count")
            )

            rows = result.collect()
            assert len(rows) == 2

            # Alice: 10 - 2 = 8
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["ten_minus_count"] == 8

            # Bob: 10 - 1 = 9
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["ten_minus_count"] == 9
        finally:
            spark.stop()

    def test_chained_arithmetic(self):
        """Test chained arithmetic operations on aggregate functions."""
        spark = SparkSession.builder.appName("issue-286").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 2},
                    {"Name": "Bob", "Value": 3},
                ]
            )

            # Test: (countDistinct - 1) * 2
            result = df.groupBy("Name").agg(
                ((F.countDistinct("Value") - 1) * 2).alias("count_minus_one_times_two")
            )

            rows = result.collect()
            assert len(rows) == 2

            # Alice: (2 - 1) * 2 = 2
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["count_minus_one_times_two"] == 2

            # Bob: (1 - 1) * 2 = 0
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["count_minus_one_times_two"] == 0
        finally:
            spark.stop()

    def test_multiple_aggregate_arithmetic(self):
        """Test multiple aggregate functions with arithmetic in same aggregation."""
        spark = SparkSession.builder.appName("issue-286").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 2},
                    {"Name": "Bob", "Value": 3},
                ]
            )

            result = df.groupBy("Name").agg(
                (F.countDistinct("Value") - 1).alias("count_minus_one"),
                (F.count("Value") + 1).alias("count_plus_one"),
                (F.sum("Value") * 2).alias("sum_times_two"),
            )

            rows = result.collect()
            assert len(rows) == 2

            # Verify all columns exist
            assert "count_minus_one" in result.columns
            assert "count_plus_one" in result.columns
            assert "sum_times_two" in result.columns

            # Verify Alice's values
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["count_minus_one"] == 1
            assert alice_row["count_plus_one"] == 3
            assert alice_row["sum_times_two"] == 6
        finally:
            spark.stop()

    def test_arithmetic_with_nulls(self):
        """Test arithmetic operations when aggregate results might be None."""
        spark = SparkSession.builder.appName("issue-286").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": None},
                    {"Name": "Alice", "Value": None},
                    {"Name": "Bob", "Value": 5},
                ]
            )

            # Count should work even with nulls
            # Note: count("Value") counts all rows (including nulls) in Sparkless
            result = df.groupBy("Name").agg(
                (F.count("Value") + 1).alias("count_plus_one")
            )

            rows = result.collect()
            assert len(rows) == 2

            # Alice: 2 rows (both with null), so count + 1 = 3
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["count_plus_one"] == 3

            # Bob: 1 row with value, so count + 1 = 2
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["count_plus_one"] == 2
        finally:
            spark.stop()

    def test_arithmetic_with_floats(self):
        """Test arithmetic operations with floating point values."""
        spark = SparkSession.builder.appName("issue-286").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1.5},
                    {"Name": "Alice", "Value": 2.5},
                    {"Name": "Bob", "Value": 3.7},
                ]
            )

            result = df.groupBy("Name").agg(
                (F.sum("Value") * 1.5).alias("sum_times_one_five")
            )

            rows = result.collect()
            assert len(rows) == 2

            # Alice: sum = 4.0, so 4.0 * 1.5 = 6.0
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert abs(alice_row["sum_times_one_five"] - 6.0) < 0.001

            # Bob: sum = 3.7, so 3.7 * 1.5 = 5.55
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert abs(bob_row["sum_times_one_five"] - 5.55) < 0.001
        finally:
            spark.stop()

    def test_arithmetic_with_negative_numbers(self):
        """Test arithmetic operations with negative numbers."""
        spark = SparkSession.builder.appName("issue-286").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": -5},
                    {"Name": "Alice", "Value": -3},
                    {"Name": "Bob", "Value": 10},
                ]
            )

            result = df.groupBy("Name").agg((F.sum("Value") + 10).alias("sum_plus_ten"))

            rows = result.collect()
            assert len(rows) == 2

            # Alice: sum = -8, so -8 + 10 = 2
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["sum_plus_ten"] == 2

            # Bob: sum = 10, so 10 + 10 = 20
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["sum_plus_ten"] == 20
        finally:
            spark.stop()

    def test_arithmetic_with_zero(self):
        """Test arithmetic operations involving zero."""
        spark = SparkSession.builder.appName("issue-286").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 0},
                    {"Name": "Alice", "Value": 0},
                    {"Name": "Bob", "Value": 5},
                ]
            )

            result = df.groupBy("Name").agg((F.sum("Value") * 2).alias("sum_times_two"))

            rows = result.collect()
            assert len(rows) == 2

            # Alice: sum = 0, so 0 * 2 = 0
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["sum_times_two"] == 0

            # Bob: sum = 5, so 5 * 2 = 10
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["sum_times_two"] == 10
        finally:
            spark.stop()

    def test_division_by_zero_handling(self):
        """Test that division by zero returns None (PySpark behavior)."""
        spark = SparkSession.builder.appName("issue-286").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Alice", "Value": 20},
                ]
            )

            # Dividing by zero should return None
            result = df.groupBy("Name").agg(
                (F.sum("Value") / 0).alias("sum_divided_by_zero")
            )

            rows = result.collect()
            assert len(rows) == 1

            alice_row = rows[0]
            # In PySpark, division by zero returns None
            assert alice_row["sum_divided_by_zero"] is None
        finally:
            spark.stop()

    def test_modulo_by_zero_handling(self):
        """Test that modulo by zero returns None (PySpark behavior)."""
        spark = SparkSession.builder.appName("issue-286").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Alice", "Value": 20},
                ]
            )

            # Modulo by zero should return None
            result = df.groupBy("Name").agg((F.sum("Value") % 0).alias("sum_mod_zero"))

            rows = result.collect()
            assert len(rows) == 1

            alice_row = rows[0]
            # In PySpark, modulo by zero returns None
            assert alice_row["sum_mod_zero"] is None
        finally:
            spark.stop()

    def test_min_function_arithmetic(self):
        """Test arithmetic operations on min aggregate function."""
        spark = SparkSession.builder.appName("issue-286").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 5},
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Bob", "Value": 3},
                ]
            )

            result = df.groupBy("Name").agg((F.min("Value") + 5).alias("min_plus_five"))

            rows = result.collect()
            assert len(rows) == 2

            # Alice: min = 5, so 5 + 5 = 10
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["min_plus_five"] == 10

            # Bob: min = 3, so 3 + 5 = 8
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["min_plus_five"] == 8
        finally:
            spark.stop()

    def test_stddev_arithmetic(self):
        """Test arithmetic operations on stddev aggregate function."""
        spark = SparkSession.builder.appName("issue-286").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Alice", "Value": 20},
                    {"Name": "Alice", "Value": 30},
                    {"Name": "Bob", "Value": 5},
                ]
            )

            result = df.groupBy("Name").agg(
                (F.stddev("Value") * 2).alias("stddev_times_two")
            )

            rows = result.collect()
            assert len(rows) == 2

            # Verify the result is a number (stddev calculation)
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["stddev_times_two"] is not None
            assert isinstance(alice_row["stddev_times_two"], (int, float))
        finally:
            spark.stop()

    def test_variance_arithmetic(self):
        """Test arithmetic operations on variance aggregate function."""
        spark = SparkSession.builder.appName("issue-286").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Alice", "Value": 20},
                    {"Name": "Alice", "Value": 30},
                    {"Name": "Bob", "Value": 5},
                ]
            )

            result = df.groupBy("Name").agg(
                (F.variance("Value") + 1).alias("variance_plus_one")
            )

            rows = result.collect()
            assert len(rows) == 2

            # Verify the result is a number (variance calculation)
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["variance_plus_one"] is not None
            assert isinstance(alice_row["variance_plus_one"], (int, float))
        finally:
            spark.stop()

    def test_complex_nested_operations(self):
        """Test deeply nested arithmetic operations."""
        spark = SparkSession.builder.appName("issue-286").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Alice", "Value": 20},
                    {"Name": "Bob", "Value": 5},
                ]
            )

            # Test: ((sum + 1) * 2) - 3
            result = df.groupBy("Name").agg(
                (((F.sum("Value") + 1) * 2) - 3).alias("complex_expr")
            )

            rows = result.collect()
            assert len(rows) == 2

            # Alice: ((30 + 1) * 2) - 3 = 59
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["complex_expr"] == 59

            # Bob: ((5 + 1) * 2) - 3 = 9
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["complex_expr"] == 9
        finally:
            spark.stop()

    def test_all_arithmetic_operators(self):
        """Test all arithmetic operators (+, -, *, /, %) in one test."""
        spark = SparkSession.builder.appName("issue-286").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Alice", "Value": 20},
                ]
            )

            result = df.groupBy("Name").agg(
                (F.sum("Value") + 5).alias("add"),
                (F.sum("Value") - 5).alias("sub"),
                (F.sum("Value") * 2).alias("mul"),
                (F.sum("Value") / 2).alias("div"),
                (F.sum("Value") % 7).alias("mod"),
            )

            rows = result.collect()
            assert len(rows) == 1

            alice_row = rows[0]
            assert alice_row["add"] == 35  # 30 + 5
            assert alice_row["sub"] == 25  # 30 - 5
            assert alice_row["mul"] == 60  # 30 * 2
            assert alice_row["div"] == 15.0  # 30 / 2
            assert alice_row["mod"] == 2  # 30 % 7
        finally:
            spark.stop()

    def test_reverse_all_operators(self):
        """Test all reverse arithmetic operators."""
        spark = SparkSession.builder.appName("issue-286").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 5},
                    {"Name": "Alice", "Value": 10},
                ]
            )

            result = df.groupBy("Name").agg(
                (10 + F.sum("Value")).alias("radd"),
                (100 - F.sum("Value")).alias("rsub"),
                (2 * F.sum("Value")).alias("rmul"),
                (60 / F.sum("Value")).alias("rtruediv"),
                (30 % F.sum("Value")).alias("rmod"),
            )

            rows = result.collect()
            assert len(rows) == 1

            alice_row = rows[0]
            assert alice_row["radd"] == 25  # 10 + 15
            assert alice_row["rsub"] == 85  # 100 - 15
            assert alice_row["rmul"] == 30  # 2 * 15
            assert abs(alice_row["rtruediv"] - 4.0) < 0.001  # 60 / 15
            assert alice_row["rmod"] == 0  # 30 % 15
        finally:
            spark.stop()

    def test_count_star_arithmetic(self):
        """Test arithmetic operations on count(*) aggregate function."""
        spark = SparkSession.builder.appName("issue-286").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Alice", "Value": 2},
                    {"Name": "Bob", "Value": 3},
                ]
            )

            result = df.groupBy("Name").agg(
                (F.count("*") - 1).alias("count_star_minus_one")
            )

            rows = result.collect()
            assert len(rows) == 2

            # Alice: count(*) = 2, so 2 - 1 = 1
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["count_star_minus_one"] == 1

            # Bob: count(*) = 1, so 1 - 1 = 0
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["count_star_minus_one"] == 0
        finally:
            spark.stop()

    def test_empty_group_handling(self):
        """Test arithmetic operations with empty groups (should still work)."""
        spark = SparkSession.builder.appName("issue-286").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Bob", "Value": 20},
                ]
            )

            # Filter to create an empty group
            filtered_df = df.filter(F.col("Name") == "Charlie")
            result = filtered_df.groupBy("Name").agg(
                (F.sum("Value") + 1).alias("sum_plus_one")
            )

            rows = result.collect()
            # Empty group should return empty result
            assert len(rows) == 0
        finally:
            spark.stop()

    def test_large_numbers(self):
        """Test arithmetic operations with large numbers."""
        spark = SparkSession.builder.appName("issue-286").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1000000},
                    {"Name": "Alice", "Value": 2000000},
                ]
            )

            result = df.groupBy("Name").agg((F.sum("Value") * 2).alias("sum_times_two"))

            rows = result.collect()
            assert len(rows) == 1

            alice_row = rows[0]
            assert alice_row["sum_times_two"] == 6000000  # (1000000 + 2000000) * 2
        finally:
            spark.stop()

    def test_mixed_aggregate_functions(self):
        """Test mixing different aggregate functions with arithmetic in one query."""
        spark = SparkSession.builder.appName("issue-286").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Alice", "Value": 20},
                    {"Name": "Alice", "Value": 30},
                    {"Name": "Bob", "Value": 5},
                ]
            )

            result = df.groupBy("Name").agg(
                (F.count("Value") + 1).alias("count_plus_one"),
                (F.sum("Value") - 5).alias("sum_minus_five"),
                (F.avg("Value") * 2).alias("avg_times_two"),
                (F.max("Value") / 2).alias("max_divided_by_two"),
                (F.min("Value") % 3).alias("min_mod_three"),
            )

            rows = result.collect()
            assert len(rows) == 2

            # Verify Alice's values
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["count_plus_one"] == 4  # 3 + 1
            assert alice_row["sum_minus_five"] == 55  # 60 - 5
            assert alice_row["avg_times_two"] == 40.0  # 20 * 2
            assert alice_row["max_divided_by_two"] == 15.0  # 30 / 2
            assert alice_row["min_mod_three"] == 1  # 10 % 3
        finally:
            spark.stop()

    def test_arithmetic_with_alias(self):
        """Test that aliases work correctly with arithmetic operations."""
        spark = SparkSession.builder.appName("issue-286").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Alice", "Value": 20},
                ]
            )

            result = df.groupBy("Name").agg((F.sum("Value") - 1).alias("custom_alias"))

            rows = result.collect()
            assert len(rows) == 1

            alice_row = rows[0]
            assert "custom_alias" in result.columns
            assert alice_row["custom_alias"] == 29  # 30 - 1
        finally:
            spark.stop()

    def test_arithmetic_precedence(self):
        """Test that arithmetic operations respect operator precedence."""
        spark = SparkSession.builder.appName("issue-286").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Alice", "Value": 20},
                ]
            )

            # Test: sum + 1 * 2 should be sum + (1 * 2) = 30 + 2 = 32
            # Not (sum + 1) * 2 = 62
            result = df.groupBy("Name").agg(
                (F.sum("Value") + 1 * 2).alias("precedence_test")
            )

            rows = result.collect()
            assert len(rows) == 1

            alice_row = rows[0]
            # Python operator precedence: multiplication before addition
            assert alice_row["precedence_test"] == 32  # 30 + (1 * 2)
        finally:
            spark.stop()

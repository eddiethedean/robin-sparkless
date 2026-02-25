"""
Tests for issue #288: CaseWhen arithmetic and logical operators.

PySpark supports arithmetic and logical operations on CaseWhen expressions
(e.g., F.when(...).otherwise(...) - F.when(...).otherwise(...)).
This test verifies that Sparkless supports the same operations.
"""

from sparkless.sql import SparkSession
import sparkless.sql.functions as F


class TestIssue288CaseWhenOperators:
    """Test arithmetic and logical operations on CaseWhen expressions."""

    def test_casewhen_subtraction(self):
        """Test subtracting two CaseWhen expressions (from issue example)."""
        spark = SparkSession.builder.appName("issue-288").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value1": 1, "Value2": 2},
                    {"Name": "Bob", "Value1": 3, "Value2": 4},
                ]
            )

            # Perform a math operation between two CaseWhen expressions
            result = df.withColumn(
                "result",
                F.when(F.col("Name") == "Alice", F.col("Value1")).otherwise(
                    F.col("Value2")
                )
                - F.when(F.col("Name") == "Alice", F.col("Value2")).otherwise(
                    F.col("Value1")
                ),
            )

            rows = result.collect()
            assert len(rows) == 2

            # Alice: Value1 (1) - Value2 (2) = -1
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["result"] == -1

            # Bob: Value2 (4) - Value1 (3) = 1
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["result"] == 1
        finally:
            spark.stop()

    def test_casewhen_addition(self):
        """Test adding two CaseWhen expressions."""
        spark = SparkSession.builder.appName("issue-288").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value1": 1, "Value2": 2},
                    {"Name": "Bob", "Value1": 3, "Value2": 4},
                ]
            )

            result = df.withColumn(
                "result",
                F.when(F.col("Name") == "Alice", F.col("Value1")).otherwise(
                    F.col("Value2")
                )
                + F.when(F.col("Name") == "Alice", F.col("Value2")).otherwise(
                    F.col("Value1")
                ),
            )

            rows = result.collect()
            assert len(rows) == 2

            # Alice: Value1 (1) + Value2 (2) = 3
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["result"] == 3

            # Bob: Value2 (4) + Value1 (3) = 7
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["result"] == 7
        finally:
            spark.stop()

    def test_casewhen_multiplication(self):
        """Test multiplying two CaseWhen expressions."""
        spark = SparkSession.builder.appName("issue-288").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value1": 2, "Value2": 3},
                    {"Name": "Bob", "Value1": 4, "Value2": 5},
                ]
            )

            result = df.withColumn(
                "result",
                F.when(F.col("Name") == "Alice", F.col("Value1")).otherwise(
                    F.col("Value2")
                )
                * F.when(F.col("Name") == "Alice", F.col("Value2")).otherwise(
                    F.col("Value1")
                ),
            )

            rows = result.collect()
            assert len(rows) == 2

            # Alice: Value1 (2) * Value2 (3) = 6
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["result"] == 6

            # Bob: Value2 (5) * Value1 (4) = 20
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["result"] == 20
        finally:
            spark.stop()

    def test_casewhen_division(self):
        """Test dividing two CaseWhen expressions."""
        spark = SparkSession.builder.appName("issue-288").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value1": 10, "Value2": 2},
                    {"Name": "Bob", "Value1": 20, "Value2": 4},
                ]
            )

            result = df.withColumn(
                "result",
                F.when(F.col("Name") == "Alice", F.col("Value1")).otherwise(
                    F.col("Value2")
                )
                / F.when(F.col("Name") == "Alice", F.col("Value2")).otherwise(
                    F.col("Value1")
                ),
            )

            rows = result.collect()
            assert len(rows) == 2

            # Alice: Value1 (10) / Value2 (2) = 5.0
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["result"] == 5.0

            # Bob: Value2 (4) / Value1 (20) = 0.2
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["result"] == 0.2
        finally:
            spark.stop()

    def test_casewhen_modulo(self):
        """Test modulo operation on two CaseWhen expressions."""
        spark = SparkSession.builder.appName("issue-288").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value1": 10, "Value2": 3},
                    {"Name": "Bob", "Value1": 20, "Value2": 7},
                ]
            )

            result = df.withColumn(
                "result",
                F.when(F.col("Name") == "Alice", F.col("Value1")).otherwise(
                    F.col("Value2")
                )
                % F.when(F.col("Name") == "Alice", F.col("Value2")).otherwise(
                    F.col("Value1")
                ),
            )

            rows = result.collect()
            assert len(rows) == 2

            # Alice: Value1 (10) % Value2 (3) = 1
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["result"] == 1

            # Bob: Value2 (7) % Value1 (20) = 7
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["result"] == 7
        finally:
            spark.stop()

    def test_casewhen_bitwise_or(self):
        """Test bitwise OR operation on two CaseWhen expressions."""
        spark = SparkSession.builder.appName("issue-288").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value1": 5, "Value2": 3},
                    {"Name": "Bob", "Value1": 10, "Value2": 6},
                ]
            )

            result = df.withColumn(
                "result",
                F.when(F.col("Name") == "Alice", F.col("Value1")).otherwise(
                    F.col("Value2")
                )
                | F.when(F.col("Name") == "Alice", F.col("Value2")).otherwise(
                    F.col("Value1")
                ),
            )

            rows = result.collect()
            assert len(rows) == 2

            # Alice: Value1 (5) | Value2 (3) = 7
            # 5 = 101, 3 = 011, 5 | 3 = 111 = 7
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["result"] == 7

            # Bob: Value2 (6) | Value1 (10) = 14
            # 6 = 110, 10 = 1010, 6 | 10 = 1110 = 14
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["result"] == 14
        finally:
            spark.stop()

    def test_casewhen_bitwise_not(self):
        """Test bitwise NOT operation (unary ~) on CaseWhen expression."""
        spark = SparkSession.builder.appName("issue-288").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value1": 5},
                    {"Name": "Bob", "Value1": 10},
                ]
            )

            result = df.withColumn(
                "result",
                ~F.when(F.col("Name") == "Alice", F.col("Value1")).otherwise(F.lit(0)),
            )

            rows = result.collect()
            assert len(rows) == 2

            # Alice: ~Value1 (5) = ~5 = -6 (in two's complement)
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["result"] == -6

            # Bob: ~0 = -1
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["result"] == -1
        finally:
            spark.stop()

    def test_casewhen_with_literal(self):
        """Test CaseWhen expression with literal value."""
        spark = SparkSession.builder.appName("issue-288").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 5},
                    {"Name": "Bob", "Value": 10},
                ]
            )

            # CaseWhen + literal
            result = df.withColumn(
                "result",
                F.when(F.col("Name") == "Alice", F.col("Value")).otherwise(F.lit(0))
                + 10,
            )

            rows = result.collect()
            assert len(rows) == 2

            # Alice: Value (5) + 10 = 15
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["result"] == 15

            # Bob: 0 + 10 = 10
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["result"] == 10
        finally:
            spark.stop()

    def test_casewhen_reverse_operations(self):
        """Test reverse operations (literal op CaseWhen)."""
        spark = SparkSession.builder.appName("issue-288").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 5},
                    {"Name": "Bob", "Value": 10},
                ]
            )

            # Literal - CaseWhen (reverse subtraction)
            result = df.withColumn(
                "result",
                100
                - F.when(F.col("Name") == "Alice", F.col("Value")).otherwise(F.lit(0)),
            )

            rows = result.collect()
            assert len(rows) == 2

            # Alice: 100 - Value (5) = 95
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["result"] == 95

            # Bob: 100 - 0 = 100
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["result"] == 100
        finally:
            spark.stop()

    def test_casewhen_chained_operations(self):
        """Test chained arithmetic operations on CaseWhen expressions."""
        spark = SparkSession.builder.appName("issue-288").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value1": 2, "Value2": 3},
                    {"Name": "Bob", "Value1": 4, "Value2": 5},
                ]
            )

            # (CaseWhen1 - CaseWhen2) * 2
            result = df.withColumn(
                "result",
                (
                    F.when(F.col("Name") == "Alice", F.col("Value1")).otherwise(
                        F.col("Value2")
                    )
                    - F.when(F.col("Name") == "Alice", F.col("Value2")).otherwise(
                        F.col("Value1")
                    )
                )
                * 2,
            )

            rows = result.collect()
            assert len(rows) == 2

            # Alice: (Value1 (2) - Value2 (3)) * 2 = -2
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["result"] == -2

            # Bob: (Value2 (5) - Value1 (4)) * 2 = 2
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["result"] == 2
        finally:
            spark.stop()

    def test_casewhen_with_nulls(self):
        """Test CaseWhen operations with null values."""
        spark = SparkSession.builder.appName("issue-288").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value1": 1, "Value2": None},
                    {"Name": "Bob", "Value1": None, "Value2": 4},
                ]
            )

            result = df.withColumn(
                "result",
                F.when(F.col("Name") == "Alice", F.col("Value1")).otherwise(
                    F.col("Value2")
                )
                + F.when(F.col("Name") == "Alice", F.col("Value2")).otherwise(
                    F.col("Value1")
                ),
            )

            rows = result.collect()
            assert len(rows) == 2

            # Alice: Value1 (1) + Value2 (None) = None
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["result"] is None

            # Bob: Value2 (4) + Value1 (None) = None
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["result"] is None
        finally:
            spark.stop()

    def test_casewhen_multiple_when_conditions(self):
        """Test CaseWhen operations with multiple WHEN conditions."""
        spark = SparkSession.builder.appName("issue-288").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Bob", "Value": 2},
                    {"Name": "Charlie", "Value": 3},
                ]
            )

            # CaseWhen with multiple when conditions
            case1 = (
                F.when(F.col("Name") == "Alice", F.lit(10))
                .when(F.col("Name") == "Bob", F.lit(20))
                .otherwise(F.lit(30))
            )
            case2 = (
                F.when(F.col("Name") == "Alice", F.lit(5))
                .when(F.col("Name") == "Bob", F.lit(15))
                .otherwise(F.lit(25))
            )

            result = df.withColumn("result", case1 - case2)

            rows = result.collect()
            assert len(rows) == 3

            # Alice: 10 - 5 = 5
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["result"] == 5

            # Bob: 20 - 15 = 5
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["result"] == 5

            # Charlie: 30 - 25 = 5
            charlie_row = next((r for r in rows if r["Name"] == "Charlie"), None)
            assert charlie_row is not None
            assert charlie_row["result"] == 5
        finally:
            spark.stop()

    def test_casewhen_nested_expressions(self):
        """Test nested CaseWhen expressions in operations."""
        spark = SparkSession.builder.appName("issue-288").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value1": 10, "Value2": 5},
                    {"Name": "Bob", "Value1": 20, "Value2": 10},
                ]
            )

            # Nested: (CaseWhen1 + CaseWhen2) - CaseWhen3
            result = df.withColumn(
                "result",
                (
                    F.when(F.col("Name") == "Alice", F.col("Value1")).otherwise(
                        F.col("Value2")
                    )
                    + F.when(F.col("Name") == "Alice", F.col("Value2")).otherwise(
                        F.col("Value1")
                    )
                )
                - F.when(F.col("Name") == "Alice", F.lit(2)).otherwise(F.lit(1)),
            )

            rows = result.collect()
            assert len(rows) == 2

            # Alice: (Value1 (10) + Value2 (5)) - 2 = 13
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["result"] == 13

            # Bob: (Value2 (10) + Value1 (20)) - 1 = 29
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["result"] == 29
        finally:
            spark.stop()

    def test_casewhen_division_by_zero(self):
        """Test division by zero returns None (PySpark behavior)."""
        spark = SparkSession.builder.appName("issue-288").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value1": 10, "Value2": 0},
                    {"Name": "Bob", "Value1": 20, "Value2": 5},
                ]
            )

            result = df.withColumn(
                "result",
                F.when(F.col("Name") == "Alice", F.col("Value1")).otherwise(
                    F.col("Value2")
                )
                / F.when(F.col("Name") == "Alice", F.col("Value2")).otherwise(
                    F.col("Value1")
                ),
            )

            rows = result.collect()
            assert len(rows) == 2

            # Alice: Value1 (10) / Value2 (0) = None (division by zero)
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["result"] is None

            # Bob: Value2 (5) / Value1 (20) = 0.25
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["result"] == 0.25
        finally:
            spark.stop()

    def test_casewhen_modulo_by_zero(self):
        """Test modulo by zero returns None (PySpark behavior)."""
        spark = SparkSession.builder.appName("issue-288").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value1": 10, "Value2": 0},
                    {"Name": "Bob", "Value1": 20, "Value2": 7},
                ]
            )

            result = df.withColumn(
                "result",
                F.when(F.col("Name") == "Alice", F.col("Value1")).otherwise(
                    F.col("Value2")
                )
                % F.when(F.col("Name") == "Alice", F.col("Value2")).otherwise(
                    F.col("Value1")
                ),
            )

            rows = result.collect()
            assert len(rows) == 2

            # Alice: Value1 (10) % Value2 (0) = None (modulo by zero)
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["result"] is None

            # Bob: Value2 (7) % Value1 (20) = 7
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["result"] == 7
        finally:
            spark.stop()

    def test_casewhen_with_floats(self):
        """Test CaseWhen operations with floating point numbers."""
        spark = SparkSession.builder.appName("issue-288").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value1": 1.5, "Value2": 2.5},
                    {"Name": "Bob", "Value1": 3.5, "Value2": 4.5},
                ]
            )

            result = df.withColumn(
                "result",
                F.when(F.col("Name") == "Alice", F.col("Value1")).otherwise(
                    F.col("Value2")
                )
                * F.when(F.col("Name") == "Alice", F.col("Value2")).otherwise(
                    F.col("Value1")
                ),
            )

            rows = result.collect()
            assert len(rows) == 2

            # Alice: Value1 (1.5) * Value2 (2.5) = 3.75
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert abs(alice_row["result"] - 3.75) < 0.001

            # Bob: Value2 (4.5) * Value1 (3.5) = 15.75
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert abs(bob_row["result"] - 15.75) < 0.001
        finally:
            spark.stop()

    def test_casewhen_with_zero_and_negative(self):
        """Test CaseWhen operations with zero and negative numbers."""
        spark = SparkSession.builder.appName("issue-288").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value1": -5, "Value2": 0},
                    {"Name": "Bob", "Value1": 10, "Value2": -3},
                ]
            )

            result = df.withColumn(
                "result",
                F.when(F.col("Name") == "Alice", F.col("Value1")).otherwise(
                    F.col("Value2")
                )
                + F.when(F.col("Name") == "Alice", F.col("Value2")).otherwise(
                    F.col("Value1")
                ),
            )

            rows = result.collect()
            assert len(rows) == 2

            # Alice: Value1 (-5) + Value2 (0) = -5
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["result"] == -5

            # Bob: Value2 (-3) + Value1 (10) = 7
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["result"] == 7
        finally:
            spark.stop()

    def test_casewhen_bitwise_and(self):
        """Test bitwise AND operation on two CaseWhen expressions."""
        spark = SparkSession.builder.appName("issue-288").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value1": 5, "Value2": 3},
                    {"Name": "Bob", "Value1": 10, "Value2": 6},
                ]
            )

            result = df.withColumn(
                "result",
                F.when(F.col("Name") == "Alice", F.col("Value1")).otherwise(
                    F.col("Value2")
                )
                & F.when(F.col("Name") == "Alice", F.col("Value2")).otherwise(
                    F.col("Value1")
                ),
            )

            rows = result.collect()
            assert len(rows) == 2

            # Alice: Value1 (5) & Value2 (3) = 1
            # 5 = 101, 3 = 011, 5 & 3 = 001 = 1
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["result"] == 1

            # Bob: Value2 (6) & Value1 (10) = 2
            # 6 = 110, 10 = 1010, 6 & 10 = 0010 = 2
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["result"] == 2
        finally:
            spark.stop()

    def test_casewhen_complex_nested_operations(self):
        """Test complex nested operations with multiple CaseWhen expressions."""
        spark = SparkSession.builder.appName("issue-288").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value1": 2, "Value2": 3, "Value3": 4},
                    {"Name": "Bob", "Value1": 5, "Value2": 6, "Value3": 7},
                ]
            )

            # (CaseWhen1 + CaseWhen2) * CaseWhen3 - 1
            result = df.withColumn(
                "result",
                (
                    (
                        F.when(F.col("Name") == "Alice", F.col("Value1")).otherwise(
                            F.col("Value2")
                        )
                        + F.when(F.col("Name") == "Alice", F.col("Value2")).otherwise(
                            F.col("Value1")
                        )
                    )
                    * F.when(F.col("Name") == "Alice", F.col("Value3")).otherwise(
                        F.col("Value3")
                    )
                )
                - 1,
            )

            rows = result.collect()
            assert len(rows) == 2

            # Alice: (Value1 (2) + Value2 (3)) * Value3 (4) - 1 = 19
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["result"] == 19

            # Bob: (Value2 (6) + Value1 (5)) * Value3 (7) - 1 = 76
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["result"] == 76
        finally:
            spark.stop()

    def test_casewhen_all_reverse_operators(self):
        """Test all reverse operators (literal op CaseWhen)."""
        spark = SparkSession.builder.appName("issue-288").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 5},
                    {"Name": "Bob", "Value": 10},
                ]
            )

            case_when = F.when(F.col("Name") == "Alice", F.col("Value")).otherwise(
                F.lit(0)
            )

            # Test all reverse operators in a single select
            result = df.select(
                F.col("Name"),
                (100 + case_when).alias("add"),
                (100 - case_when).alias("sub"),
                (2 * case_when).alias("mul"),
                (100 / case_when).alias("div"),
                (17 % case_when).alias("mod"),
            )

            rows = result.collect()
            assert len(rows) == 2

            # Alice: Value = 5
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["add"] == 105  # 100 + 5
            assert alice_row["sub"] == 95  # 100 - 5
            assert alice_row["mul"] == 10  # 2 * 5
            assert alice_row["div"] == 20.0  # 100 / 5
            assert alice_row["mod"] == 2  # 17 % 5

            # Bob: Value = 0
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["add"] == 100  # 100 + 0
            assert bob_row["sub"] == 100  # 100 - 0
            assert bob_row["mul"] == 0  # 2 * 0
            # Division by zero returns None
            assert bob_row["div"] is None  # 100 / 0
            # Modulo by zero returns None
            assert bob_row["mod"] is None  # 17 % 0
        finally:
            spark.stop()

    def test_casewhen_in_groupby_agg(self):
        """Test CaseWhen operations in groupBy aggregation context."""
        spark = SparkSession.builder.appName("issue-288").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Category": "A", "Value": 10},
                    {"Name": "Alice", "Category": "B", "Value": 20},
                    {"Name": "Bob", "Category": "A", "Value": 30},
                    {"Name": "Bob", "Category": "B", "Value": 40},
                ]
            )

            # First compute the CaseWhen expression per row, then sum
            # Use sum() aggregate function on the CaseWhen expression result
            result = df.groupBy("Name").agg(
                F.sum(
                    F.when(F.col("Category") == "A", F.col("Value")).otherwise(F.lit(0))
                    - F.when(F.col("Category") == "B", F.col("Value")).otherwise(
                        F.lit(0)
                    )
                ).alias("diff")
            )

            rows = result.collect()
            assert len(rows) == 2

            # Alice: Row 1: (10 - 0) = 10, Row 2: (0 - 20) = -20, Sum = -10
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["diff"] == -10

            # Bob: Row 1: (30 - 0) = 30, Row 2: (0 - 40) = -40, Sum = -10
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["diff"] == -10
        finally:
            spark.stop()

    def test_casewhen_operator_precedence(self):
        """Test operator precedence with CaseWhen expressions."""
        spark = SparkSession.builder.appName("issue-288").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value1": 2, "Value2": 3},
                    {"Name": "Bob", "Value1": 4, "Value2": 5},
                ]
            )

            case1 = F.when(F.col("Name") == "Alice", F.col("Value1")).otherwise(
                F.col("Value2")
            )
            case2 = F.when(F.col("Name") == "Alice", F.col("Value2")).otherwise(
                F.col("Value1")
            )

            # Test: case1 + case2 * 2 (multiplication should happen first)
            result = df.withColumn("result", case1 + case2 * 2)

            rows = result.collect()
            assert len(rows) == 2

            # Alice: Value1 (2) + Value2 (3) * 2 = 2 + 6 = 8
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["result"] == 8

            # Bob: Value2 (5) + Value1 (4) * 2 = 5 + 8 = 13
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["result"] == 13
        finally:
            spark.stop()

    def test_casewhen_with_large_numbers(self):
        """Test CaseWhen operations with large numbers."""
        spark = SparkSession.builder.appName("issue-288").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value1": 1000000, "Value2": 2000000},
                    {"Name": "Bob", "Value1": 3000000, "Value2": 4000000},
                ]
            )

            result = df.withColumn(
                "result",
                F.when(F.col("Name") == "Alice", F.col("Value1")).otherwise(
                    F.col("Value2")
                )
                + F.when(F.col("Name") == "Alice", F.col("Value2")).otherwise(
                    F.col("Value1")
                ),
            )

            rows = result.collect()
            assert len(rows) == 2

            # Alice: Value1 (1000000) + Value2 (2000000) = 3000000
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["result"] == 3000000

            # Bob: Value2 (4000000) + Value1 (3000000) = 7000000
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["result"] == 7000000
        finally:
            spark.stop()

    def test_casewhen_empty_dataframe(self):
        """Test CaseWhen operations on empty DataFrame."""
        spark = SparkSession.builder.appName("issue-288").getOrCreate()
        try:
            df = spark.createDataFrame([], schema="Name string, Value1 int, Value2 int")

            result = df.withColumn(
                "result",
                F.when(F.col("Name") == "Alice", F.col("Value1")).otherwise(
                    F.col("Value2")
                )
                - F.when(F.col("Name") == "Alice", F.col("Value2")).otherwise(
                    F.col("Value1")
                ),
            )

            rows = result.collect()
            assert len(rows) == 0
        finally:
            spark.stop()

    def test_casewhen_with_aliases(self):
        """Test CaseWhen operations with aliased expressions."""
        spark = SparkSession.builder.appName("issue-288").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value1": 5, "Value2": 10},
                    {"Name": "Bob", "Value1": 15, "Value2": 20},
                ]
            )

            case1 = (
                F.when(F.col("Name") == "Alice", F.col("Value1"))
                .otherwise(F.col("Value2"))
                .alias("case1")
            )
            case2 = (
                F.when(F.col("Name") == "Alice", F.col("Value2"))
                .otherwise(F.col("Value1"))
                .alias("case2")
            )

            result = df.withColumn("result", case1 * case2)

            rows = result.collect()
            assert len(rows) == 2

            # Alice: Value1 (5) * Value2 (10) = 50
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["result"] == 50

            # Bob: Value2 (20) * Value1 (15) = 300
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["result"] == 300
        finally:
            spark.stop()

    def test_casewhen_all_arithmetic_operators(self):
        """Test all arithmetic operators in one comprehensive test."""
        spark = SparkSession.builder.appName("issue-288").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Bob", "Value": 5},
                ]
            )

            case_when = F.when(F.col("Name") == "Alice", F.col("Value")).otherwise(
                F.lit(2)
            )

            result = df.select(
                F.col("Name"),
                (case_when + 5).alias("add"),
                (case_when - 3).alias("sub"),
                (case_when * 2).alias("mul"),
                (case_when / 2).alias("div"),
                (case_when % 3).alias("mod"),
            )

            rows = result.collect()
            assert len(rows) == 2

            # Alice: Value = 10
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["add"] == 15  # 10 + 5
            assert alice_row["sub"] == 7  # 10 - 3
            assert alice_row["mul"] == 20  # 10 * 2
            assert alice_row["div"] == 5.0  # 10 / 2
            assert alice_row["mod"] == 1  # 10 % 3

            # Bob: Value = 2
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["add"] == 7  # 2 + 5
            assert bob_row["sub"] == -1  # 2 - 3
            assert bob_row["mul"] == 4  # 2 * 2
            assert bob_row["div"] == 1.0  # 2 / 2
            assert bob_row["mod"] == 2  # 2 % 3
        finally:
            spark.stop()

    def test_casewhen_mixed_with_columns(self):
        """Test CaseWhen operations mixed with regular column operations."""
        spark = SparkSession.builder.appName("issue-288").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value1": 5, "Value2": 3},
                    {"Name": "Bob", "Value1": 10, "Value2": 7},
                ]
            )

            # CaseWhen + Column - CaseWhen
            result = df.withColumn(
                "result",
                F.when(F.col("Name") == "Alice", F.col("Value1")).otherwise(
                    F.col("Value2")
                )
                + F.col("Value1")
                - F.when(F.col("Name") == "Alice", F.col("Value2")).otherwise(
                    F.col("Value1")
                ),
            )

            rows = result.collect()
            assert len(rows) == 2

            # Alice: Value1 (5) + Value1 (5) - Value2 (3) = 7
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["result"] == 7

            # Bob: Value2 (7) + Value1 (10) - Value1 (10) = 7
            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["result"] == 7
        finally:
            spark.stop()

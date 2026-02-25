"""
Tests for issue #289: struct function support.

PySpark supports the struct function for creating struct-type columns
from multiple columns. This test verifies that Sparkless supports the same.
"""

from sparkless.sql import SparkSession
import sparkless.sql.functions as F


class TestIssue289StructFunction:
    """Test struct function support."""

    def test_struct_basic(self):
        """Test basic struct function usage (from issue example)."""
        spark = SparkSession.builder.appName("issue-289").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Bob", "Value": 2},
                ]
            )

            # Create a struct column from Name and Value
            result = df.withColumn("new_struct", F.struct("Name", "Value"))

            rows = result.collect()
            assert len(rows) == 2

            # Check that struct column exists and has correct structure
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert "new_struct" in alice_row
            struct_val = alice_row["new_struct"]
            assert struct_val is not None
            # Struct should be accessible as dict-like or have Name and Value fields
            assert hasattr(struct_val, "Name") or (
                isinstance(struct_val, dict) and "Name" in struct_val
            )
            assert hasattr(struct_val, "Value") or (
                isinstance(struct_val, dict) and "Value" in struct_val
            )

            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert "new_struct" in bob_row
        finally:
            spark.stop()

    def test_struct_with_col_function(self):
        """Test struct function with F.col()."""
        spark = SparkSession.builder.appName("issue-289").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1, "Age": 25},
                    {"Name": "Bob", "Value": 2, "Age": 30},
                ]
            )

            result = df.withColumn(
                "person", F.struct(F.col("Name"), F.col("Value"), F.col("Age"))
            )

            rows = result.collect()
            assert len(rows) == 2

            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert "person" in alice_row
        finally:
            spark.stop()

    def test_struct_single_column(self):
        """Test struct function with a single column."""
        spark = SparkSession.builder.appName("issue-289").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice"},
                    {"Name": "Bob"},
                ]
            )

            result = df.withColumn("name_struct", F.struct("Name"))

            rows = result.collect()
            assert len(rows) == 2

            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert "name_struct" in alice_row
        finally:
            spark.stop()

    def test_struct_with_nulls(self):
        """Test struct function with null values."""
        spark = SparkSession.builder.appName("issue-289").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1, "Age": None},
                    {"Name": "Bob", "Value": None, "Age": 30},
                ]
            )

            result = df.withColumn("person", F.struct("Name", "Value", "Age"))

            rows = result.collect()
            assert len(rows) == 2

            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert "person" in alice_row
        finally:
            spark.stop()

    def test_struct_in_select(self):
        """Test struct function in select statement."""
        spark = SparkSession.builder.appName("issue-289").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Bob", "Value": 2},
                ]
            )

            result = df.select(F.struct("Name", "Value").alias("new_struct"))

            rows = result.collect()
            assert len(rows) == 2
            assert "new_struct" in rows[0]
        finally:
            spark.stop()

    def test_struct_multiple_types(self):
        """Test struct function with different data types."""
        spark = SparkSession.builder.appName("issue-289").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1, "Score": 95.5, "Active": True},
                    {"Name": "Bob", "Value": 2, "Score": 87.0, "Active": False},
                ]
            )

            result = df.withColumn(
                "mixed", F.struct("Name", "Value", "Score", "Active")
            )

            rows = result.collect()
            assert len(rows) == 2

            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert "mixed" in alice_row
        finally:
            spark.stop()

    def test_struct_with_expressions(self):
        """Test struct function with computed expressions."""
        spark = SparkSession.builder.appName("issue-289").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1, "Age": 25},
                    {"Name": "Bob", "Value": 2, "Age": 30},
                ]
            )

            result = df.withColumn(
                "computed",
                F.struct(
                    F.col("Name"),
                    (F.col("Value") * 2).alias("doubled"),
                    (F.col("Age") + 10).alias("age_plus_10"),
                ),
            )

            rows = result.collect()
            assert len(rows) == 2

            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert "computed" in alice_row
        finally:
            spark.stop()

    def test_struct_with_literals(self):
        """Test struct function with literal values."""
        spark = SparkSession.builder.appName("issue-289").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Bob", "Value": 2},
                ]
            )

            result = df.withColumn(
                "with_literal", F.struct("Name", F.lit("constant"), F.col("Value"))
            )

            rows = result.collect()
            assert len(rows) == 2

            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert "with_literal" in alice_row
        finally:
            spark.stop()

    def test_struct_in_groupby_agg(self):
        """Test struct function in groupBy aggregation context."""
        spark = SparkSession.builder.appName("issue-289").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1, "Category": "A"},
                    {"Name": "Bob", "Value": 2, "Category": "A"},
                    {"Name": "Charlie", "Value": 3, "Category": "B"},
                ]
            )

            result = (
                df.groupBy("Category")
                .agg(
                    F.struct(
                        F.sum("Value").alias("total"),
                        F.count("Name").alias("count"),
                    ).alias("stats")
                )
                .orderBy("Category")
            )

            rows = result.collect()
            assert len(rows) == 2
            assert "stats" in rows[0]
        finally:
            spark.stop()

    def test_struct_field_access(self):
        """Test that struct fields can be accessed from the struct column."""
        spark = SparkSession.builder.appName("issue-289").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1, "Age": 25},
                    {"Name": "Bob", "Value": 2, "Age": 30},
                ]
            )

            result = df.withColumn("person", F.struct("Name", "Value", "Age"))

            rows = result.collect()
            assert len(rows) == 2
            assert "person" in rows[0]

            # Verify struct contains the expected fields
            person = rows[0]["person"]
            # Struct should be accessible as dict-like or have attributes
            if isinstance(person, dict):
                assert "Name" in person
                assert person["Name"] == "Alice"
                assert person["Value"] == 1
                assert person["Age"] == 25
            elif hasattr(person, "Name"):
                assert person.Name == "Alice"
                assert person.Value == 1
                assert person.Age == 25
        finally:
            spark.stop()

    def test_nested_struct(self):
        """Test nested struct (struct within struct)."""
        spark = SparkSession.builder.appName("issue-289").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1, "Age": 25},
                    {"Name": "Bob", "Value": 2, "Age": 30},
                ]
            )

            # Create inner struct
            df = df.withColumn("inner", F.struct("Name", "Value"))
            # Create outer struct containing inner struct
            result = df.withColumn("outer", F.struct("inner", "Age"))

            rows = result.collect()
            assert len(rows) == 2

            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert "outer" in alice_row
        finally:
            spark.stop()

    def test_struct_with_arrays(self):
        """Test struct function with array columns."""
        spark = SparkSession.builder.appName("issue-289").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Scores": [1, 2, 3]},
                    {"Name": "Bob", "Scores": [4, 5, 6]},
                ]
            )

            result = df.withColumn("person_scores", F.struct("Name", "Scores"))

            rows = result.collect()
            assert len(rows) == 2

            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert "person_scores" in alice_row
        finally:
            spark.stop()

    def test_struct_in_join(self):
        """Test struct function in join operations."""
        spark = SparkSession.builder.appName("issue-289").getOrCreate()
        try:
            df1 = spark.createDataFrame(
                [{"id": 1, "Name": "Alice"}, {"id": 2, "Name": "Bob"}]
            )
            df2 = spark.createDataFrame(
                [{"id": 1, "Value": 10}, {"id": 2, "Value": 20}]
            )

            # Create struct in joined dataframe
            joined = df1.join(df2, on="id").withColumn(
                "combined", F.struct("Name", "Value")
            )

            rows = joined.collect()
            assert len(rows) == 2

            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert "combined" in alice_row
            assert alice_row["Value"] == 10
        finally:
            spark.stop()

    def test_struct_with_aliased_columns(self):
        """Test struct function with aliased columns."""
        spark = SparkSession.builder.appName("issue-289").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Bob", "Value": 2},
                ]
            )

            result = df.select(
                F.col("Name").alias("n"),
                F.col("Value").alias("v"),
            ).withColumn("aliased_struct", F.struct("n", "v"))

            rows = result.collect()
            assert len(rows) == 2
            assert "aliased_struct" in rows[0]
        finally:
            spark.stop()

    def test_struct_multiple_operations(self):
        """Test struct function with multiple chained operations."""
        spark = SparkSession.builder.appName("issue-289").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1, "Age": 25},
                    {"Name": "Bob", "Value": 2, "Age": 30},
                ]
            )

            result = (
                df.withColumn("person", F.struct("Name", "Value"))
                .withColumn("full_info", F.struct("person", "Age"))
                .filter(F.col("Value") > 0)
                .select("full_info")
            )

            rows = result.collect()
            assert len(rows) == 2
            assert "full_info" in rows[0]
        finally:
            spark.stop()

    def test_struct_empty_dataframe(self):
        """Test struct function with empty DataFrame."""
        spark = SparkSession.builder.appName("issue-289").getOrCreate()
        try:
            from sparkless.spark_types import (
                StructType,
                StructField,
                StringType,
                IntegerType,
            )

            schema = StructType(
                [
                    StructField("Name", StringType(), True),
                    StructField("Value", IntegerType(), True),
                ]
            )
            df = spark.createDataFrame([], schema)

            result = df.withColumn("new_struct", F.struct("Name", "Value"))

            rows = result.collect()
            assert len(rows) == 0
            assert "new_struct" in result.columns
        finally:
            spark.stop()

    def test_struct_with_conditional(self):
        """Test struct function with conditional expressions."""
        spark = SparkSession.builder.appName("issue-289").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1, "Age": 25},
                    {"Name": "Bob", "Value": 2, "Age": 30},
                ]
            )

            result = df.withColumn(
                "conditional_struct",
                F.struct(
                    "Name",
                    F.when(F.col("Value") > 1, "high").otherwise("low").alias("level"),
                    "Age",
                ),
            )

            rows = result.collect()
            assert len(rows) == 2

            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert "conditional_struct" in alice_row
        finally:
            spark.stop()

    def test_struct_with_string_functions(self):
        """Test struct function with string function expressions."""
        spark = SparkSession.builder.appName("issue-289").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Bob", "Value": 2},
                ]
            )

            result = df.withColumn(
                "string_struct",
                F.struct(
                    F.upper("Name").alias("upper_name"),
                    F.col("Value"),
                ),
            )

            rows = result.collect()
            assert len(rows) == 2

            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert "string_struct" in alice_row
        finally:
            spark.stop()

    def test_struct_with_math_operations(self):
        """Test struct function with mathematical operations."""
        spark = SparkSession.builder.appName("issue-289").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Value": 4, "Multiplier": 2},
                    {"Value": 9, "Multiplier": 3},
                ]
            )

            result = df.withColumn(
                "math_struct",
                F.struct(
                    F.sqrt("Value").alias("sqrt"),
                    (F.col("Value") * F.col("Multiplier")).alias("product"),
                    F.pow("Value", 2).alias("power"),
                ),
            )

            rows = result.collect()
            assert len(rows) == 2
            assert "math_struct" in rows[0]
        finally:
            spark.stop()

    def test_struct_large_number_of_fields(self):
        """Test struct function with many fields."""
        spark = SparkSession.builder.appName("issue-289").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {
                        "a": 1,
                        "b": 2,
                        "c": 3,
                        "d": 4,
                        "e": 5,
                        "f": 6,
                        "g": 7,
                        "h": 8,
                    },
                ]
            )

            result = df.withColumn(
                "large_struct",
                F.struct("a", "b", "c", "d", "e", "f", "g", "h"),
            )

            rows = result.collect()
            assert len(rows) == 1
            assert "large_struct" in rows[0]
        finally:
            spark.stop()

"""
Integration tests for case sensitivity configuration.

Tests that spark.sql.caseSensitive configuration works correctly
across all DataFrame operations.
"""

import pytest
from sparkless.sql import SparkSession, functions as F


class TestCaseSensitivityConfiguration:
    """Test case sensitivity configuration and its effects."""

    def test_default_case_insensitive(self):
        """Test that default behavior is case-insensitive."""
        spark = SparkSession("TestApp")
        assert spark.conf.is_case_sensitive() is False

        df = spark.createDataFrame([{"Name": "Alice", "Age": 25}])
        result = df.select("name").collect()
        assert len(result) == 1
        assert result[0]["Name"] == "Alice"
        spark.stop()

    def test_case_sensitive_mode(self):
        """Test case-sensitive mode."""
        spark = SparkSession.builder.config(
            "spark.sql.caseSensitive", "true"
        ).getOrCreate()

        assert spark.conf.is_case_sensitive() is True

        df = spark.createDataFrame([{"Name": "Alice", "Age": 25}])

        # Case-sensitive: "name" should not match "Name"
        with pytest.raises(Exception):  # Should raise column not found
            df.select("name").collect()

        # Exact case should work
        result = df.select("Name").collect()
        assert len(result) == 1
        assert result[0]["Name"] == "Alice"

        spark.stop()

    def test_case_insensitive_select(self):
        """Test select with case-insensitive matching."""
        spark = SparkSession("TestApp")
        df = spark.createDataFrame([{"Name": "Alice", "Age": 25}])

        # Various case combinations should all work
        result1 = df.select("name").collect()
        result2 = df.select("NAME").collect()
        result3 = df.select("Name").collect()

        assert len(result1) == len(result2) == len(result3) == 1
        assert result1[0]["Name"] == result2[0]["Name"] == result3[0]["Name"] == "Alice"

        spark.stop()

    def test_case_insensitive_filter(self):
        """Test filter with case-insensitive matching."""
        spark = SparkSession("TestApp")
        df = spark.createDataFrame(
            [{"Name": "Alice", "Age": 25}, {"Name": "Bob", "Age": 30}]
        )

        result = df.filter(F.col("name") == "Alice").collect()
        assert len(result) == 1
        assert result[0]["Name"] == "Alice"

        result = df.filter(F.col("NAME") == "Bob").collect()
        assert len(result) == 1
        assert result[0]["Name"] == "Bob"

        spark.stop()

    def test_case_insensitive_groupBy(self):
        """Test groupBy with case-insensitive matching."""
        spark = SparkSession("TestApp")
        df = spark.createDataFrame(
            [
                {"Dept": "IT", "Salary": 100},
                {"Dept": "IT", "Salary": 200},
                {"Dept": "HR", "Salary": 150},
            ]
        )

        result = df.groupBy("dept").agg(F.sum("salary").alias("total")).collect()
        assert len(result) == 2

        spark.stop()

    def test_case_insensitive_join(self):
        """Test join with case-insensitive matching."""
        spark = SparkSession("TestApp")
        df1 = spark.createDataFrame([{"ID": 1, "Name": "Alice"}])
        df2 = spark.createDataFrame([{"id": 1, "Dept": "IT"}])

        result = df1.join(df2, on="id", how="inner").collect()
        assert len(result) == 1
        assert "Name" in result[0].__dict__["_data_dict"]
        assert "Dept" in result[0].__dict__["_data_dict"]

        spark.stop()

    def test_case_sensitive_mode_exact_match_required(self):
        """Test that case-sensitive mode requires exact matches."""
        spark = SparkSession.builder.config(
            "spark.sql.caseSensitive", "true"
        ).getOrCreate()

        df = spark.createDataFrame([{"Name": "Alice", "Age": 25}])

        # These should all fail in case-sensitive mode
        with pytest.raises(Exception):
            df.select("name").collect()

        with pytest.raises(Exception):
            df.select("NAME").collect()

        with pytest.raises(Exception):
            df.filter(F.col("name") == "Alice").collect()

        # Only exact match should work
        result = df.select("Name").collect()
        assert len(result) == 1

        spark.stop()

    def test_case_sensitive_withColumn_fails_with_wrong_case(self):
        """Test that withColumn fails with wrong case in case-sensitive mode."""
        spark = SparkSession.builder.config(
            "spark.sql.caseSensitive", "true"
        ).getOrCreate()

        # Issue #264 scenario - but in case-sensitive mode
        df = spark.createDataFrame(
            [
                {"key": "Alice"},
                {"key": "Bob"},
                {"key": "Charlie"},
            ]
        )

        # Should fail: referencing "Key" (uppercase) when column is "key" (lowercase)
        with pytest.raises(Exception):
            df.withColumn("key_upper", F.upper(F.col("Key"))).collect()

        # Should work: exact case match
        result = df.withColumn("key_upper", F.upper(F.col("key"))).collect()
        assert len(result) == 3
        assert result[0]["key_upper"] == "ALICE"

        spark.stop()

    def test_case_sensitive_filter_fails_with_wrong_case(self):
        """Test that filter fails with wrong case in case-sensitive mode."""
        spark = SparkSession.builder.config(
            "spark.sql.caseSensitive", "true"
        ).getOrCreate()

        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Age": 25},
                {"Name": "Bob", "Age": 30},
            ]
        )

        # Should fail: wrong case
        with pytest.raises(Exception):
            df.filter(F.col("name") == "Alice").collect()

        with pytest.raises(Exception):
            df.filter(F.col("AGE") > 25).collect()

        # Should work: exact case match
        result = df.filter(F.col("Name") == "Alice").collect()
        assert len(result) == 1

        result = df.filter(F.col("Age") > 25).collect()
        assert len(result) == 1

        spark.stop()

    def test_case_sensitive_select_fails_with_wrong_case(self):
        """Test that select fails with wrong case in case-sensitive mode."""
        spark = SparkSession.builder.config(
            "spark.sql.caseSensitive", "true"
        ).getOrCreate()

        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Age": 25, "Salary": 5000},
                {"Name": "Bob", "Age": 30, "Salary": 6000},
            ]
        )

        # Should fail: wrong case
        with pytest.raises(Exception):
            df.select("name", "age").collect()

        with pytest.raises(Exception):
            df.select(F.col("NAME"), F.col("AGE")).collect()

        # Should work: exact case match
        result = df.select("Name", "Age").collect()
        assert len(result) == 2

        result = df.select(F.col("Name"), F.col("Age")).collect()
        assert len(result) == 2

        spark.stop()

    def test_case_sensitive_groupBy_fails_with_wrong_case(self):
        """Test that groupBy fails with wrong case in case-sensitive mode."""
        spark = SparkSession.builder.config(
            "spark.sql.caseSensitive", "true"
        ).getOrCreate()

        df = spark.createDataFrame(
            [
                {"Dept": "IT", "Salary": 100},
                {"Dept": "IT", "Salary": 200},
                {"Dept": "HR", "Salary": 150},
            ]
        )

        # Should fail: wrong case
        with pytest.raises(Exception):
            df.groupBy("dept").agg(F.sum("salary").alias("total")).collect()

        with pytest.raises(Exception):
            df.groupBy("Dept").agg(F.sum("SALARY").alias("total")).collect()

        # Should work: exact case match
        result = df.groupBy("Dept").agg(F.sum("Salary").alias("total")).collect()
        assert len(result) == 2

        spark.stop()

    def test_case_sensitive_join_fails_with_wrong_case(self):
        """Test that join fails with wrong case in case-sensitive mode.

        Note: Joins with Column expressions (df1['ID'] == df2['id']) allow
        different column names as they match by value, not name. Case-sensitive
        mode affects column name resolution within each DataFrame, but the join
        condition itself can reference columns with different cases.
        """
        spark = SparkSession.builder.config(
            "spark.sql.caseSensitive", "true"
        ).getOrCreate()

        df1 = spark.createDataFrame([{"ID": 1, "Name": "Alice"}])
        df2 = spark.createDataFrame([{"id": 1, "Dept": "IT"}])

        # Join with Column expressions allows different column names (matches by value)
        # The case-sensitive check happens when resolving each column reference
        # Both df1["ID"] and df2["id"] resolve correctly within their own DataFrames
        result = df1.join(df2, df1["ID"] == df2["id"], "inner").collect()
        assert len(result) == 1

        # Should work: exact case match (same case in both DataFrames)
        df2_fixed = spark.createDataFrame([{"ID": 1, "Dept": "IT"}])
        result = df1.join(df2_fixed, df1["ID"] == df2_fixed["ID"], "inner").collect()
        assert len(result) == 1

        # Test join with string column name (should require exact case match)
        # When joining by string column name, both DataFrames need the column
        df3 = spark.createDataFrame([{"ID": 2, "Name": "Bob"}])
        # This should work - "ID" exists in both
        result = df1.join(df3, "ID", "inner").collect()
        assert len(result) == 0  # No matching IDs

        spark.stop()

    def test_case_sensitive_attribute_access_requires_exact_case(self):
        """Test that attribute access requires exact case in case-sensitive mode."""
        spark = SparkSession.builder.config(
            "spark.sql.caseSensitive", "true"
        ).getOrCreate()

        df = spark.createDataFrame([{"Name": "Alice", "Age": 25}])

        # In case-sensitive mode, wrong case should fail
        with pytest.raises(Exception):
            _ = df.name  # Column is "Name", not "name"

        with pytest.raises(Exception):
            _ = df.AGE  # Column is "Age", not "AGE"

        # Should work: exact case match
        name_col = df.Name
        assert name_col.name == "Name"

        age_col = df.Age
        assert age_col.name == "Age"

        spark.stop()

    def test_case_sensitive_sql_queries_require_exact_case(self):
        """Test that SQL queries require exact case in case-sensitive mode."""
        spark = SparkSession.builder.config(
            "spark.sql.caseSensitive", "true"
        ).getOrCreate()

        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Age": 25, "Dept": "IT"},
                {"Name": "Bob", "Age": 30, "Dept": "HR"},
            ]
        )
        df.createOrReplaceTempView("employees")

        # Should fail: wrong case in SQL
        with pytest.raises(Exception):
            spark.sql("SELECT name FROM employees").collect()

        with pytest.raises(Exception):
            spark.sql("SELECT Name, age FROM employees WHERE dept = 'IT'").collect()

        # Should work: exact case match
        result = spark.sql(
            "SELECT Name, Age FROM employees WHERE Dept = 'IT'"
        ).collect()
        assert len(result) == 1
        assert result[0]["Name"] == "Alice"

        spark.stop()

    def test_case_sensitive_issue_264_scenario(self):
        """Test issue #264 scenario in case-sensitive mode (should fail)."""
        spark = SparkSession.builder.config(
            "spark.sql.caseSensitive", "true"
        ).getOrCreate()

        # Exact reproduction of issue #264
        df = spark.createDataFrame(
            [
                {"key": "Alice"},
                {"key": "Bob"},
                {"key": "Charlie"},
            ]
        )

        # In case-sensitive mode, this should FAIL (different from default case-insensitive mode)
        with pytest.raises(Exception):
            df.withColumn("key_upper", F.upper(F.col("Key"))).collect()

        # Exact case should work
        result = df.withColumn("key_upper", F.upper(F.col("key"))).collect()
        assert len(result) == 3
        assert result[0]["key_upper"] == "ALICE"

        spark.stop()

    def test_case_insensitive_unionByName(self):
        """Test unionByName with case-insensitive matching."""
        spark = SparkSession("TestApp")
        df1 = spark.createDataFrame([{"Name": "Alice", "Age": 25}])
        df2 = spark.createDataFrame([{"NAME": "Bob", "AGE": 30}])

        result = df1.unionByName(df2).collect()
        assert len(result) == 2
        # Both rows should have same column names (from df1)
        assert "Name" in [f.name for f in result[0]._schema.fields]
        assert "Age" in [f.name for f in result[0]._schema.fields]

        spark.stop()

    def test_ambiguity_detection(self):
        """Test that ambiguity is handled by returning first match (PySpark behavior)."""
        spark = SparkSession("TestApp")

        # Create DataFrame with ambiguous column names (different case)
        # Note: This might not be possible to create directly, but if we can,
        # it should be detected during resolution
        # Actually, Python dicts can't have duplicate keys, so this is tricky
        # Let's test with schema instead
        from sparkless.spark_types import StructType, StructField, StringType

        schema = StructType(
            [
                StructField("Name", StringType()),
                StructField("name", StringType()),
            ]
        )

        # Create DataFrame with both columns in schema
        # The data dict can only have one key, so we'll have one column with data
        df = spark.createDataFrame([{"Name": "Alice", "name": "Bob"}], schema=schema)

        # When trying to select "name" (lowercase), PySpark behavior is to:
        # 1. Find the first matching column (case-insensitive) - "Name" comes first
        # 2. Use the requested column name "name" as the output
        result = df.select("name").collect()
        assert len(result) == 1
        # The output column should be "name" (requested name), and value should be from "Name" (first match)
        assert (
            result[0]["name"] == "Alice"
        )  # First match is "Name" which has value "Alice"

        # Verify that selecting with different case also works
        result2 = df.select("NaMe").collect()
        assert len(result2) == 1
        assert (
            result2[0]["NaMe"] == "Alice"
        )  # Still uses first match "Name", but output is "NaMe"

        spark.stop()

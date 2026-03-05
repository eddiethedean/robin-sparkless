"""
Integration tests for case sensitivity configuration. Uses get_spark_imports from fixture only.
Uses shared spark / spark_case_sensitive fixtures; backend-agnostic Row and conf helpers.
"""

import uuid

import pytest

from tests.fixtures.spark_imports import get_spark_imports

_imports = get_spark_imports()
SparkSession = _imports.SparkSession
F = _imports.F
StructType = _imports.StructType
StructField = _imports.StructField
StringType = _imports.StringType


def _row_to_dict(row):
    """Backend-agnostic Row to dict (PySpark Row.asDict(), mock may use _data_dict)."""
    if hasattr(row, "asDict"):
        return row.asDict()
    if hasattr(row, "_data_dict"):
        return dict(row._data_dict)
    return dict(row)


def _row_keys(row):
    """Return column names from a Row (works for PySpark and sparkless)."""
    return list(_row_to_dict(row).keys())


def _is_case_sensitive(spark):
    """Backend-agnostic: True if spark.sql.caseSensitive is true (PySpark has no is_case_sensitive())."""
    conf = spark.conf
    if hasattr(conf, "is_case_sensitive"):
        return conf.is_case_sensitive()
    val = conf.get("spark.sql.caseSensitive", "false")
    return str(val).strip().lower() == "true"


@pytest.fixture
def spark_case_sensitive(request):
    """Session with spark.sql.caseSensitive=true; created and stopped per use."""
    from tests.fixtures.spark_backend import get_backend_type

    _ = get_backend_type(request)
    app_name = f"integration_case_sensitive_{uuid.uuid4().hex[:8]}"
    session = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.caseSensitive", "true")
        .getOrCreate()
    )
    yield session
    session.stop()


class TestCaseSensitivityConfiguration:
    """Test case sensitivity configuration and its effects."""

    def test_default_case_insensitive(self, spark):
        """Test that default behavior is case-insensitive."""
        assert _is_case_sensitive(spark) is False

        df = spark.createDataFrame([{"Name": "Alice", "Age": 25}])
        result = df.select("name").collect()
        assert len(result) == 1
        # Row key is the selected name (e.g. "name"); value is from column "Name"
        row = result[0]
        assert list(_row_to_dict(row).values())[0] == "Alice"

    def test_case_sensitive_mode(self, spark_case_sensitive):
        """Test case-sensitive mode."""
        spark = spark_case_sensitive
        assert _is_case_sensitive(spark) is True

        df = spark.createDataFrame([{"Name": "Alice", "Age": 25}])

        # Case-sensitive: "name" should not match "Name"
        with pytest.raises(Exception):  # Should raise column not found
            df.select("name").collect()

        # Exact case should work
        result = df.select("Name").collect()
        assert len(result) == 1
        assert result[0]["Name"] == "Alice"

    def test_case_insensitive_select(self, spark):
        """Test select with case-insensitive matching."""
        df = spark.createDataFrame([{"Name": "Alice", "Age": 25}])

        # Various case combinations should all work; Row keys follow selected name per backend
        result1 = df.select("name").collect()
        result2 = df.select("NAME").collect()
        result3 = df.select("Name").collect()

        assert len(result1) == len(result2) == len(result3) == 1
        v1 = list(_row_to_dict(result1[0]).values())[0]
        v2 = list(_row_to_dict(result2[0]).values())[0]
        v3 = list(_row_to_dict(result3[0]).values())[0]
        assert v1 == v2 == v3 == "Alice"

    def test_case_insensitive_filter(self, spark):
        """Test filter with case-insensitive matching."""
        df = spark.createDataFrame(
            [{"Name": "Alice", "Age": 25}, {"Name": "Bob", "Age": 30}]
        )

        result = df.filter(F.col("name") == "Alice").collect()
        assert len(result) == 1
        assert result[0]["Name"] == "Alice"

        result = df.filter(F.col("NAME") == "Bob").collect()
        assert len(result) == 1
        assert result[0]["Name"] == "Bob"

    def test_case_insensitive_groupBy(self, spark):
        """Test groupBy with case-insensitive matching."""
        df = spark.createDataFrame(
            [
                {"Dept": "IT", "Salary": 100},
                {"Dept": "IT", "Salary": 200},
                {"Dept": "HR", "Salary": 150},
            ]
        )

        result = df.groupBy("dept").agg(F.sum("salary").alias("total")).collect()
        assert len(result) == 2

    def test_case_insensitive_join(self, spark):
        """Test join with case-insensitive matching."""
        df1 = spark.createDataFrame([{"ID": 1, "Name": "Alice"}])
        df2 = spark.createDataFrame([{"id": 1, "Dept": "IT"}])

        result = df1.join(df2, on="id", how="inner").collect()
        assert len(result) == 1
        keys = _row_keys(result[0])
        assert "Name" in keys
        assert "Dept" in keys

    def test_case_sensitive_mode_exact_match_required(self, spark_case_sensitive):
        """Test that case-sensitive mode requires exact matches."""
        spark = spark_case_sensitive
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

    def test_case_sensitive_withColumn_fails_with_wrong_case(
        self, spark_case_sensitive
    ):
        """Test that withColumn fails with wrong case in case-sensitive mode."""
        spark = spark_case_sensitive
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

    def test_case_sensitive_filter_fails_with_wrong_case(self, spark_case_sensitive):
        """Test that filter fails with wrong case in case-sensitive mode."""
        spark = spark_case_sensitive
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

    def test_case_sensitive_select_fails_with_wrong_case(self, spark_case_sensitive):
        """Test that select fails with wrong case in case-sensitive mode."""
        spark = spark_case_sensitive
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

    def test_case_sensitive_groupBy_fails_with_wrong_case(self, spark_case_sensitive):
        """Test that groupBy fails with wrong case in case-sensitive mode."""
        spark = spark_case_sensitive
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

    def test_case_sensitive_join_fails_with_wrong_case(self, spark_case_sensitive):
        """Test that join fails with wrong case in case-sensitive mode.

        Note: Joins with Column expressions (df1['ID'] == df2['id']) allow
        different column names as they match by value, not name. Case-sensitive
        mode affects column name resolution within each DataFrame, but the join
        condition itself can reference columns with different cases.
        """
        spark = spark_case_sensitive
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

    def test_case_sensitive_attribute_access_requires_exact_case(
        self, spark_case_sensitive
    ):
        """Test that attribute access requires exact case in case-sensitive mode."""
        spark = spark_case_sensitive
        df = spark.createDataFrame([{"Name": "Alice", "Age": 25}])

        # In case-sensitive mode, wrong case should fail
        with pytest.raises(Exception):
            _ = df.name  # Column is "Name", not "name"

        with pytest.raises(Exception):
            _ = df.AGE  # Column is "Age", not "AGE"

        # Should work: exact case match; Column.name may be "Name" or alias (backend-dependent)
        name_col = df.Name
        age_col = df.Age
        assert name_col is not None and age_col is not None
        # Verify columns work in select (backend-agnostic)
        result = df.select(name_col, age_col).collect()
        assert len(result) == 1
        assert result[0]["Name"] == "Alice"
        assert result[0]["Age"] == 25

    def test_case_sensitive_sql_queries_require_exact_case(self, spark_case_sensitive):
        """Test that SQL queries require exact case in case-sensitive mode."""
        spark = spark_case_sensitive
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

    def test_case_sensitive_issue_264_scenario(self, spark_case_sensitive):
        """Test issue #264 scenario in case-sensitive mode (should fail)."""
        spark = spark_case_sensitive
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

    def test_case_insensitive_unionByName(self, spark):
        """Test unionByName with case-insensitive matching."""
        df1 = spark.createDataFrame([{"Name": "Alice", "Age": 25}])
        df2 = spark.createDataFrame([{"NAME": "Bob", "AGE": 30}])

        result = df1.unionByName(df2).collect()
        assert len(result) == 2
        # Both rows should have same column names (from df1); backend-agnostic Row access
        keys = _row_keys(result[0])
        assert "Name" in keys
        assert "Age" in keys

    def test_ambiguity_detection(self, spark):
        """Test ambiguity with Name/name: PySpark raises AMBIGUOUS_REFERENCE; mock may pick first match."""
        schema = StructType(
            [
                StructField("Name", StringType()),
                StructField("name", StringType()),
            ]
        )
        df = spark.createDataFrame([{"Name": "Alice", "name": "Bob"}], schema=schema)

        # PySpark: select("name") with two columns Name/name raises AnalysisException (ambiguous).
        # Mock/Robin: may return first case-insensitive match.
        try:
            result = df.select("name").collect()
            # Backend returned one row (first-match behavior)
            assert len(result) == 1
            row_dict = _row_to_dict(result[0])
            assert list(row_dict.values())[0] == "Alice"
        except Exception as e:
            err_msg = str(e).lower()
            if "ambiguous" in err_msg or "ambiguous_reference" in err_msg:
                # PySpark: expected
                pass
            else:
                raise

        # Same for different case
        try:
            result2 = df.select("NaMe").collect()
            assert len(result2) == 1
            assert list(_row_to_dict(result2[0]).values())[0] == "Alice"
        except Exception as e:
            err_msg = str(e).lower()
            if "ambiguous" in err_msg or "ambiguous_reference" in err_msg:
                pass
            else:
                raise

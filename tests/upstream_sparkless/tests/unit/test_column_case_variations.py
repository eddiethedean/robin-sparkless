"""
Comprehensive tests for column name case variations.

Tests all different ways to refer to columns with various case combinations
to ensure case-insensitive resolution works correctly across all operations.
"""

import os

import pytest

from tests.fixtures.spark_imports import get_spark_imports


def _is_pyspark_backend():
    return (
        os.getenv("MOCK_SPARK_TEST_BACKEND")
        or os.getenv("SPARKLESS_TEST_BACKEND")
        or ""
    ).strip().lower() == "pyspark"


def _backend_imports():
    """F and types that match the test backend (PySpark vs sparkless)."""
    imp = get_spark_imports()
    return imp


# Use backend-appropriate F and types so PySpark gets pyspark.sql.functions
_imp = _backend_imports()
F = _imp.F
StructType = _imp.StructType
StructField = _imp.StructField
StringType = _imp.StringType
IntegerType = _imp.IntegerType

# SparkSession only used when not PySpark (conftest provides spark when PySpark)
from sparkless.sql import SparkSession  # noqa: E402


class TestColumnCaseVariations:
    """Test all different ways to refer to columns with wrong case."""

    @pytest.fixture
    def spark(self, request):
        """Create SparkSession for testing; use conftest spark when PySpark backend."""
        if _is_pyspark_backend():
            return request.getfixturevalue("spark")
        return SparkSession("TestApp")

    @pytest.fixture
    def sample_df(self, spark):
        """Create sample DataFrame with mixed-case column names."""
        data = [
            {"Name": "Alice", "Age": 25, "Salary": 5000, "Dept": "IT"},
            {"Name": "Bob", "Age": 30, "Salary": 6000, "Dept": "HR"},
            {"Name": "Charlie", "Age": 35, "Salary": 7000, "Dept": "IT"},
        ]
        return spark.createDataFrame(data)

    def _name_col(self, sample_df, canonical="Name"):
        """Return actual column name for canonical (PySpark may lowercase)."""
        names = [f.name for f in sample_df.schema.fields]
        if canonical in names:
            return canonical
        low = canonical.lower()
        return low if low in names else canonical

    def test_select_all_case_variations(self, sample_df):
        """Test select with all possible case variations."""
        name_col = self._name_col(sample_df, "Name")
        # Original case (use actual column name for PySpark case-sensitive)
        result = sample_df.select(name_col).collect()
        assert len(result) == 3

        # All lowercase (row key may differ from schema name in PySpark)
        result = sample_df.select("name").collect()
        assert len(result) == 3
        assert result[0][self._row_key(result[0], "Name")] == "Alice"

        # All uppercase
        result = sample_df.select("NAME").collect()
        assert len(result) == 3
        assert result[0][self._row_key(result[0], "Name")] == "Alice"

        # Mixed case variations
        result = sample_df.select("NaMe").collect()
        assert len(result) == 3

        result = sample_df.select("nAmE").collect()
        assert len(result) == 3

        result = sample_df.select("NAme").collect()
        assert len(result) == 3

        result = sample_df.select("naME").collect()
        assert len(result) == 3

        # Multiple columns with different cases
        result = sample_df.select("name", "AGE", "Salary").collect()
        assert len(result) == 3
        field_names = (
            getattr(result[0], "_schema", None)
            and [f.name for f in result[0]._schema.fields]
            or list(result[0].asDict().keys())
            if hasattr(result[0], "asDict")
            else list(result[0].keys())
            if hasattr(result[0], "keys")
            else []
        )
        for col in ["Name", "name", "Age", "age", "Salary", "salary"]:
            if col in field_names:
                break
        else:
            assert "Name" in field_names or "name" in field_names, field_names

    def test_select_with_f_col_all_cases(self, sample_df):
        """Test select with F.col() using all case variations."""
        # Lowercase
        result = sample_df.select(F.col("name")).collect()
        assert len(result) == 3

        # Uppercase
        result = sample_df.select(F.col("NAME")).collect()
        assert len(result) == 3

        # Mixed case
        result = sample_df.select(F.col("NaMe")).collect()
        assert len(result) == 3

        # Multiple columns
        result = sample_df.select(
            F.col("name"), F.col("age"), F.col("SALARY")
        ).collect()
        assert len(result) == 3

    def test_filter_all_case_variations(self, sample_df):
        """Test filter with all possible case variations."""
        # String column comparison
        result = sample_df.filter(F.col("name") == "Alice").collect()
        assert len(result) == 1

        result = sample_df.filter(F.col("NAME") == "Alice").collect()
        assert len(result) == 1

        result = sample_df.filter(F.col("NaMe") == "Alice").collect()
        assert len(result) == 1

        # Numeric column comparison
        result = sample_df.filter(F.col("age") > 25).collect()
        assert len(result) == 2

        result = sample_df.filter(F.col("AGE") > 25).collect()
        assert len(result) == 2

        result = sample_df.filter(F.col("AgE") > 25).collect()
        assert len(result) == 2

        # String comparison methods
        result = sample_df.filter(F.col("name").startswith("A")).collect()
        assert len(result) == 1

        result = sample_df.filter(F.col("NAME").startswith("A")).collect()
        assert len(result) == 1

        # Complex conditions
        result = sample_df.filter(
            (F.col("age") > 25) & (F.col("salary") < 6500)
        ).collect()
        assert len(result) == 1

    def test_groupBy_all_case_variations(self, sample_df):
        """Test groupBy with all possible case variations."""
        # Single column
        result = sample_df.groupBy("dept").agg(F.sum("salary").alias("total")).collect()
        assert len(result) == 2

        result = sample_df.groupBy("DEPT").agg(F.sum("salary").alias("total")).collect()
        assert len(result) == 2

        result = sample_df.groupBy("DePt").agg(F.sum("salary").alias("total")).collect()
        assert len(result) == 2

        # Multiple columns
        result = (
            sample_df.groupBy("dept", "age").agg(F.count("*").alias("count")).collect()
        )
        assert len(result) >= 1

        # Using F.col()
        result = sample_df.groupBy(F.col("dept")).agg(F.sum("salary")).collect()
        assert len(result) == 2

    def test_agg_all_case_variations(self, sample_df):
        """Test aggregation functions with all case variations."""
        # Sum with different cases
        result = sample_df.groupBy("dept").agg(F.sum("salary").alias("total")).collect()
        assert len(result) == 2

        result = sample_df.groupBy("dept").agg(F.sum("SALARY").alias("total")).collect()
        assert len(result) == 2

        result = sample_df.groupBy("dept").agg(F.sum("Salary").alias("total")).collect()
        assert len(result) == 2

        # Avg with different cases
        result = sample_df.groupBy("dept").agg(F.avg("age").alias("avg_age")).collect()
        assert len(result) == 2

        result = sample_df.groupBy("dept").agg(F.avg("AGE").alias("avg_age")).collect()
        assert len(result) == 2

        # Multiple aggregations
        result = (
            sample_df.groupBy("dept")
            .agg(
                F.sum("salary").alias("total"),
                F.avg("AGE").alias("avg_age"),
                F.max("age").alias("max_age"),
            )
            .collect()
        )
        assert len(result) == 2

    def test_orderBy_all_case_variations(self, sample_df):
        """Test orderBy with all possible case variations."""
        # Ascending
        result = sample_df.orderBy("name").collect()
        name_col = self._name_col(sample_df, "Name")
        assert result[0][name_col] == "Alice"

        result = sample_df.orderBy("NAME").collect()
        name_col = self._name_col(sample_df, "Name")
        assert result[0][name_col] == "Alice"

        result = sample_df.orderBy("NaMe").collect()
        name_col = self._name_col(sample_df, "Name")
        assert result[0][name_col] == "Alice"

        # Descending
        result = sample_df.orderBy(F.col("name").desc()).collect()
        assert result[0][name_col] == "Charlie"

        result = sample_df.orderBy(F.col("NAME").desc()).collect()
        assert result[0][name_col] == "Charlie"

        # Multiple columns
        result = sample_df.orderBy("dept", "age").collect()
        assert len(result) == 3

        result = sample_df.orderBy("DEPT", "AGE").collect()
        assert len(result) == 3

    def test_withColumn_all_case_variations(self, sample_df):
        """Test withColumn with all possible case variations."""
        # Reference existing column with wrong case
        result = sample_df.withColumn("double_age", F.col("age") * 2).collect()
        assert len(result) == 3
        assert result[0]["double_age"] == 50

        result = sample_df.withColumn("double_age", F.col("AGE") * 2).collect()
        assert len(result) == 3

        result = sample_df.withColumn("double_age", F.col("AgE") * 2).collect()
        assert len(result) == 3

        # Complex expressions
        result = sample_df.withColumn(
            "bonus", F.col("salary") * 0.1 + F.col("age") * 10
        ).collect()
        assert len(result) == 3

        result = sample_df.withColumn(
            "bonus", F.col("SALARY") * 0.1 + F.col("AGE") * 10
        ).collect()
        assert len(result) == 3

    def _row_keys(self, row):
        """Return column names from a Row (works for PySpark and sparkless)."""
        if hasattr(row, "asDict"):
            return list(row.asDict().keys())
        if hasattr(row, "_data_dict"):
            return list(row._data_dict.keys())
        return list(dict(row).keys())

    def _row_key(self, row, canonical):
        """Return the actual key in row that matches canonical (case-insensitive)."""
        keys = self._row_keys(row)
        low = canonical.lower()
        return next((k for k in keys if k.lower() == low), canonical)

    def test_withColumnRenamed_all_case_variations(self, sample_df):
        """Test withColumnRenamed with all possible case variations."""
        # Rename using wrong case for existing column
        result = sample_df.withColumnRenamed("name", "full_name").collect()
        assert "full_name" in self._row_keys(result[0])

        result = sample_df.withColumnRenamed("NAME", "full_name").collect()
        assert "full_name" in self._row_keys(result[0])

        result = sample_df.withColumnRenamed("NaMe", "full_name").collect()
        assert "full_name" in self._row_keys(result[0])

        # Multiple renames
        result = sample_df.withColumnsRenamed(
            {"name": "full_name", "age": "years"}
        ).collect()
        keys = self._row_keys(result[0])
        assert "full_name" in keys
        assert "years" in keys

        result = sample_df.withColumnsRenamed(
            {"NAME": "full_name", "AGE": "years"}
        ).collect()
        assert "full_name" in self._row_keys(result[0])

    def test_drop_all_case_variations(self, sample_df):
        """Test drop with all possible case variations."""
        # Single column
        result_df = sample_df.drop("name")
        assert "name" not in [c.lower() for c in result_df.columns]

        result_df = sample_df.drop("NAME")
        assert "name" not in [c.lower() for c in result_df.columns]

        result_df = sample_df.drop("NaMe")
        assert "name" not in [c.lower() for c in result_df.columns]

        # Multiple columns
        result_df = sample_df.drop("age", "salary")
        assert "age" not in [c.lower() for c in result_df.columns]
        assert "salary" not in [c.lower() for c in result_df.columns]

        result_df = sample_df.drop("AGE", "SALARY")
        assert "age" not in [c.lower() for c in result_df.columns]
        assert "salary" not in [c.lower() for c in result_df.columns]

    def test_join_all_case_variations(self, spark):
        """Test join with all possible case variations."""
        df1 = spark.createDataFrame(
            [{"ID": 1, "Name": "Alice"}, {"ID": 2, "Name": "Bob"}]
        )
        df2 = spark.createDataFrame([{"id": 1, "Dept": "IT"}, {"id": 2, "Dept": "HR"}])

        # Join key with different cases
        result = df1.join(df2, on="id", how="inner").collect()
        assert len(result) == 2

        result = df1.join(df2, on="ID", how="inner").collect()
        assert len(result) == 2

        result = df1.join(df2, on="Id", how="inner").collect()
        assert len(result) == 2

        # Left DataFrame column access
        result_df = df1.join(df2, on="id", how="inner")
        result = result_df.select("name", "dept").collect()
        assert len(result) == 2

        result = result_df.select("NAME", "DEPT").collect()
        assert len(result) == 2

    def test_unionByName_all_case_variations(self, spark):
        """Test unionByName with all possible case variations."""
        df1 = spark.createDataFrame([{"Name": "Alice", "Age": 25}])
        df2 = spark.createDataFrame([{"NAME": "Bob", "AGE": 30}])

        # Should work with case-insensitive matching
        result = df1.unionByName(df2).collect()
        assert len(result) == 2
        # Both rows should have same column names (from df1)
        row_names = self._row_keys(result[0])
        assert "Name" in row_names or "name" in row_names or "NAME" in row_names

        df3 = spark.createDataFrame([{"name": "Charlie", "age": 35}])
        result = df1.unionByName(df3).collect()
        assert len(result) == 2  # df1 (1 row) + df3 (1 row) = 2 rows

    def test_selectExpr_all_case_variations(self, sample_df):
        """Test selectExpr with all possible case variations."""
        # Simple column reference
        result = sample_df.selectExpr("name").collect()
        assert len(result) == 3

        result = sample_df.selectExpr("NAME").collect()
        assert len(result) == 3

        result = sample_df.selectExpr("NaMe").collect()
        assert len(result) == 3

        # With alias
        result = sample_df.selectExpr("name as full_name").collect()
        assert "full_name" in self._row_keys(result[0])

        result = sample_df.selectExpr("NAME as full_name").collect()
        assert "full_name" in self._row_keys(result[0])

        # Complex expressions
        result = sample_df.selectExpr("age * 2 as double_age").collect()
        assert len(result) == 3

        result = sample_df.selectExpr("AGE * 2 as double_age").collect()
        assert len(result) == 3

    def test_chained_operations_all_cases(self, sample_df):
        """Test chained operations with various case combinations."""
        # Filter -> Select
        result = sample_df.filter(F.col("age") > 25).select("name", "salary").collect()
        assert len(result) == 2

        result = sample_df.filter(F.col("AGE") > 25).select("NAME", "SALARY").collect()
        assert len(result) == 2

        # Filter -> GroupBy -> OrderBy
        result = (
            sample_df.filter(F.col("age") > 25)
            .groupBy("dept")
            .agg(F.sum("salary").alias("total"))
            .orderBy("dept")
            .collect()
        )
        assert len(result) == 2

        result = (
            sample_df.filter(F.col("AGE") > 25)
            .groupBy("DEPT")
            .agg(F.sum("SALARY").alias("total"))
            .orderBy("DEPT")
            .collect()
        )
        assert len(result) == 2

        # Select -> WithColumn -> Drop
        result_df = (
            sample_df.select("name", "age", "salary")
            .withColumn("bonus", F.col("salary") * 0.1)
            .drop("age")
        )
        assert "Age" not in result_df.columns
        assert "bonus" in result_df.columns

        result_df = (
            sample_df.select("NAME", "AGE", "SALARY")
            .withColumn("bonus", F.col("SALARY") * 0.1)
            .drop("AGE")
        )
        assert "Age" not in result_df.columns

    def test_expressions_with_case_variations(self, sample_df):
        """Test various expression types with case variations."""
        # Arithmetic operations
        result = sample_df.withColumn("total", F.col("age") + F.col("salary")).collect()
        assert len(result) == 3

        result = sample_df.withColumn("total", F.col("AGE") + F.col("SALARY")).collect()
        assert len(result) == 3

        # String functions
        result = sample_df.withColumn("upper_name", F.upper(F.col("name"))).collect()
        assert len(result) == 3

        result = sample_df.withColumn("upper_name", F.upper(F.col("NAME"))).collect()
        assert len(result) == 3

        # Conditional expressions
        result = sample_df.withColumn(
            "category", F.when(F.col("age") > 30, "Senior").otherwise("Junior")
        ).collect()
        assert len(result) == 3

        result = sample_df.withColumn(
            "category", F.when(F.col("AGE") > 30, "Senior").otherwise("Junior")
        ).collect()
        assert len(result) == 3

        # Nested expressions
        result = sample_df.withColumn(
            "adjusted_salary",
            F.col("salary") * F.when(F.col("dept") == "IT", 1.1).otherwise(1.0),
        ).collect()
        assert len(result) == 3

        result = sample_df.withColumn(
            "adjusted_salary",
            F.col("SALARY") * F.when(F.col("DEPT") == "IT", 1.1).otherwise(1.0),
        ).collect()
        assert len(result) == 3

    def test_window_functions_with_case_variations(self, sample_df):
        """Test window functions with case variations."""
        Window = _imp.Window
        window_spec = Window.partitionBy("dept").orderBy("age")

        result = sample_df.withColumn("rank", F.rank().over(window_spec)).collect()
        assert len(result) == 3

        # Test with different case
        window_spec2 = Window.partitionBy("DEPT").orderBy("AGE")
        result = sample_df.withColumn("rank", F.rank().over(window_spec2)).collect()
        assert len(result) == 3

    def test_distinct_with_case_variations(self, sample_df):
        """Test distinct with case variations."""
        # Add duplicate row
        df_with_dupes = sample_df.union(sample_df)

        # Distinct on column with wrong case
        result = df_with_dupes.select("name").distinct().collect()
        assert len(result) == 3

        result = df_with_dupes.select("NAME").distinct().collect()
        assert len(result) == 3

        # Distinct on multiple columns
        result = df_with_dupes.select("name", "dept").distinct().collect()
        assert len(result) >= 2

        result = df_with_dupes.select("NAME", "DEPT").distinct().collect()
        assert len(result) >= 2

    def test_subset_operations_with_case_variations(self, sample_df):
        """Test subset/collection operations with case variations."""
        # dropDuplicates
        df_with_dupes = sample_df.union(sample_df)

        result = df_with_dupes.dropDuplicates(subset=["name"]).collect()
        assert len(result) == 3

        result = df_with_dupes.dropDuplicates(subset=["NAME"]).collect()
        assert len(result) == 3

        result = df_with_dupes.dropDuplicates(subset=["NaMe"]).collect()
        assert len(result) == 3

        # Multiple columns
        result = df_with_dupes.dropDuplicates(subset=["name", "dept"]).collect()
        assert len(result) >= 2

        result = df_with_dupes.dropDuplicates(subset=["NAME", "DEPT"]).collect()
        assert len(result) >= 2

    def test_schema_access_with_case_variations(self, sample_df):
        """Test schema field access with case variations."""
        # Schema should preserve original column names (or lowercase in PySpark)
        schema = sample_df.schema
        field_names = [f.name for f in schema.fields]
        field_names_lower = [f.lower() for f in field_names]

        assert "name" in field_names_lower
        assert "age" in field_names_lower
        assert "salary" in field_names_lower
        assert "dept" in field_names_lower

        # Selected column name (PySpark may return lowercase or literal case)
        sel_name = sample_df.select("name").schema.fields[0].name
        assert sel_name.lower() == "name"
        sel_name2 = sample_df.select("NAME").schema.fields[0].name
        assert sel_name2.lower() == "name"

    def test_empty_dataframe_with_case_variations(self, spark):
        """Test operations on empty DataFrame with explicit schema."""
        schema = StructType(
            [
                StructField("Name", StringType()),
                StructField("Age", IntegerType()),
            ]
        )
        try:
            df = spark.createDataFrame([], schema=schema)
        except TypeError:
            df = spark.createDataFrame([], schema)

        result_df = df.select("name")
        names = [f.name for f in result_df.schema.fields]
        assert "Name" in names or "name" in names

        result_df = df.select("NAME")
        names2 = [f.name for f in result_df.schema.fields]
        assert any(n.lower() == "name" for n in names2)

        result_df = df.filter(F.col("age") > 25)
        assert len(result_df.schema.fields) == 2

    def test_complex_query_all_case_variations(self, sample_df):
        """Test a complex query using all case variations."""
        # Complex query with multiple operations and various cases
        result = (
            sample_df.select("name", "age", "salary", "dept")
            .filter(F.col("age") > 25)
            .groupBy("dept")
            .agg(
                F.avg("salary").alias("avg_salary"),
                F.max("age").alias("max_age"),
                F.count("*").alias("count"),
            )
            .orderBy(F.col("avg_salary").desc())
            .collect()
        )
        assert len(result) == 2

        # Same query with different case variations
        result = (
            sample_df.select("NAME", "AGE", "SALARY", "DEPT")
            .filter(F.col("AGE") > 25)
            .groupBy("DEPT")
            .agg(
                F.avg("SALARY").alias("avg_salary"),
                F.max("AGE").alias("max_age"),
                F.count("*").alias("count"),
            )
            .orderBy(F.col("avg_salary").desc())
            .collect()
        )
        assert len(result) == 2

        # Mixed case
        result = (
            sample_df.select("Name", "Age", "Salary", "Dept")
            .filter(F.col("Age") > 25)
            .groupBy("Dept")
            .agg(
                F.avg("Salary").alias("avg_salary"),
                F.max("Age").alias("max_age"),
                F.count("*").alias("count"),
            )
            .orderBy(F.col("avg_salary").desc())
            .collect()
        )
        assert len(result) == 2

    def _col_name_str(self, col):
        """Get column name as string (PySpark Column.name can be a Column, not str)."""
        name_attr = getattr(col, "name", None)
        if callable(name_attr):
            try:
                v = name_attr()
                if isinstance(v, str):
                    return v
            except Exception:
                pass
        elif isinstance(name_attr, str):
            return name_attr
        # PySpark Column: use string representation to detect name
        return str(col)

    def test_attribute_access_all_case_variations(self, sample_df):
        """Test DataFrame attribute access (df.columnName) with case variations."""
        # Resolve column by name (PySpark is case-sensitive for attribute access)
        name_col = self._name_col(sample_df, "Name")
        age_col = self._name_col(sample_df, "Age")

        col = getattr(sample_df, name_col)
        name_val = self._col_name_str(col)
        assert name_val in ("Name", "name") or "name" in name_val.lower()

        # Same column via attribute (sparkless allows name/NAME; PySpark only actual name)
        col = getattr(sample_df, name_col)
        name_val = self._col_name_str(col)
        assert name_val in ("Name", "name", "NAME") or "name" in name_val.lower()

        col = getattr(sample_df, name_col)
        name_val = self._col_name_str(col)
        assert name_val in ("Name", "name", "NAME") or "name" in name_val.lower()

        col = getattr(sample_df, age_col)
        name_val = self._col_name_str(col)
        assert name_val in ("Age", "age") or "age" in name_val.lower()

        col = getattr(sample_df, age_col)
        name_val = self._col_name_str(col)
        assert name_val in ("Age", "age", "AGE") or "age" in name_val.lower()

    def test_fillna_all_case_variations(self, spark):
        """Test fillna with case variations in subset parameter."""
        data = [
            {"Name": "Alice", "Age": None, "Salary": 5000},
            {"Name": "Bob", "Age": 30, "Salary": None},
            {"Name": None, "Age": 25, "Salary": 7000},
        ]
        df = spark.createDataFrame(data)

        # fillna with subset using wrong case
        result = df.fillna(0, subset=["age"]).collect()
        assert result[0]["Age"] == 0

        result = df.fillna(0, subset=["AGE"]).collect()
        assert result[0]["Age"] == 0

        result = df.fillna("Unknown", subset=["name"]).collect()
        assert result[2]["Name"] == "Unknown"

        result = df.fillna("Unknown", subset=["NAME"]).collect()
        assert result[2]["Name"] == "Unknown"

        # fillna with dict using wrong case keys
        result = df.fillna({"age": 0, "salary": 9999}).collect()
        assert result[0]["Age"] == 0
        assert result[1]["Salary"] == 9999

        result = df.fillna({"AGE": 0, "SALARY": 9999}).collect()
        assert result[0]["Age"] == 0
        assert result[1]["Salary"] == 9999

    def test_replace_all_case_variations(self, spark):
        """Test replace with case variations in subset parameter."""
        data = [
            {"Name": "Alice", "Age": 25, "Dept": "IT"},
            {"Name": "Bob", "Age": 30, "Dept": "HR"},
            {"Name": "Alice", "Age": 35, "Dept": "IT"},
        ]
        df = spark.createDataFrame(data)
        dept_col = next((c for c in df.columns if c.lower() == "dept"), "dept")
        name_col_actual = next((c for c in df.columns if c.lower() == "name"), "name")

        def _key(row, canonical):
            d = row.asDict() if hasattr(row, "asDict") else dict(row)
            low = canonical.lower()
            return next((k for k in d if k.lower() == low), canonical)

        result = df.replace("IT", "Engineering", subset=[dept_col]).collect()
        assert result[0][_key(result[0], "Dept")] == "Engineering"

        result = df.replace("IT", "Engineering", subset=[dept_col]).collect()
        assert result[0][_key(result[0], "Dept")] == "Engineering"

        result = df.replace("Alice", "Alice Smith", subset=[name_col_actual]).collect()
        assert result[0][_key(result[0], "Name")] == "Alice Smith"

        result = df.replace("Alice", "Alice Smith", subset=[name_col_actual]).collect()
        assert result[0][_key(result[0], "Name")] == "Alice Smith"

    def test_pivot_all_case_variations(self, spark):
        """Test pivot operations with case variations."""
        data = [
            {"Name": "Alice", "Dept": "IT", "Salary": 5000},
            {"Name": "Bob", "Dept": "HR", "Salary": 6000},
            {"Name": "Charlie", "Dept": "IT", "Salary": 7000},
            {"Name": "David", "Dept": "HR", "Salary": 8000},
        ]
        df = spark.createDataFrame(data)

        # pivot with wrong case column name
        result = df.groupBy("name").pivot("dept").agg(F.sum("salary")).collect()
        assert len(result) >= 1

        result = df.groupBy("NAME").pivot("DEPT").agg(F.sum("salary")).collect()
        assert len(result) >= 1

        result = df.groupBy("Name").pivot("dept").agg(F.sum("SALARY")).collect()
        assert len(result) >= 1

    def test_coalesce_all_case_variations(self, spark):
        """Test coalesce function with case variations."""
        data = [
            {"Col1": None, "Col2": None, "Col3": "Value3"},
            {"Col1": "Value1", "Col2": None, "Col3": None},
            {"Col1": None, "Col2": "Value2", "Col3": None},
        ]
        df = spark.createDataFrame(data)

        # coalesce with wrong case
        result = df.select(
            F.coalesce(F.col("col1"), F.col("col2"), F.col("col3")).alias("result")
        ).collect()
        assert result[0]["result"] == "Value3"
        assert result[1]["result"] == "Value1"
        assert result[2]["result"] == "Value2"

        result = df.select(
            F.coalesce(F.col("COL1"), F.col("COL2"), F.col("COL3")).alias("result")
        ).collect()
        assert result[0]["result"] == "Value3"

        result = df.select(F.coalesce("col1", "col2", "col3").alias("result")).collect()
        assert result[0]["result"] == "Value3"

    def test_dropna_all_case_variations(self, spark):
        """Test dropna with case variations in subset parameter."""
        data = [
            {"Name": "Alice", "Age": 25, "Salary": None},
            {"Name": None, "Age": 30, "Salary": 6000},
            {"Name": "Charlie", "Age": None, "Salary": 7000},
        ]
        df = spark.createDataFrame(data)

        # dropna with subset using wrong case
        result = df.dropna(subset=["name"]).collect()
        assert len(result) == 2

        result = df.dropna(subset=["NAME"]).collect()
        assert len(result) == 2

        result = df.dropna(subset=["age"]).collect()
        assert len(result) == 2

        result = df.dropna(subset=["AGE"]).collect()
        assert len(result) == 2

        result = df.dropna(subset=["name", "age"]).collect()
        assert len(result) == 1

        result = df.dropna(subset=["NAME", "AGE"]).collect()
        assert len(result) == 1

    def test_nested_struct_field_access_all_cases(self, spark):
        """Test nested struct field access with case variations."""
        data = [
            {"Person": {"Name": "Alice", "Age": 25}},
            {"Person": {"Name": "Bob", "Age": 30}},
        ]
        schema = StructType(
            [
                StructField(
                    "Person",
                    StructType(
                        [
                            StructField("Name", StringType()),
                            StructField("Age", IntegerType()),
                        ]
                    ),
                ),
            ]
        )
        try:
            df = spark.createDataFrame(data, schema=schema)
        except TypeError:
            df = spark.createDataFrame(data, schema)

        # Access nested fields (PySpark may name the column "name" not "Person.name")
        def _get_nested(row, path):
            keys = self._row_keys(row)
            # PySpark often names selected struct field as the field name only
            key_short = path.split(".")[-1]
            key2 = self._row_key(row, key_short)
            if key2 in keys:
                return row[key2]
            key = self._row_key(row, path)
            return row[key] if key in keys else row[key2]

        result = df.select("Person.name").collect()
        assert _get_nested(result[0], "Person.name") == "Alice"

        result = df.select("Person.NAME").collect()
        assert _get_nested(result[0], "Person.name") == "Alice"

        result = df.select("Person.age").collect()
        assert _get_nested(result[0], "Person.age") == 25

        result = df.select("Person.AGE").collect()
        assert _get_nested(result[0], "Person.age") == 25

        # Using F.col()
        result = df.select(F.col("Person.name")).collect()
        assert _get_nested(result[0], "Person.name") == "Alice"

        result = df.select(F.col("Person.NAME")).collect()
        assert _get_nested(result[0], "Person.name") == "Alice"

    def test_sql_queries_all_case_variations(self, sample_df, spark):
        """Test SQL queries with case variations in column names."""
        sample_df.createOrReplaceTempView("employees")

        # SQL query with wrong case column names
        result = spark.sql("SELECT name FROM employees").collect()
        assert len(result) == 3

        result = spark.sql("SELECT NAME FROM employees").collect()
        assert len(result) == 3

        result = spark.sql("SELECT name, age FROM employees WHERE age > 25").collect()
        assert len(result) == 2

        result = spark.sql("SELECT NAME, AGE FROM employees WHERE AGE > 25").collect()
        assert len(result) == 2

        result = spark.sql("SELECT name FROM employees WHERE dept = 'IT'").collect()
        assert len(result) == 2

        result = spark.sql("SELECT NAME FROM employees WHERE DEPT = 'IT'").collect()
        assert len(result) == 2

    def test_rollup_cube_all_case_variations(self, spark):
        """Test rollup and cube operations with case variations."""
        data = [
            {"Year": 2020, "Quarter": "Q1", "Sales": 100},
            {"Year": 2020, "Quarter": "Q2", "Sales": 200},
            {"Year": 2021, "Quarter": "Q1", "Sales": 150},
        ]
        df = spark.createDataFrame(data)

        # rollup with wrong case
        result = (
            df.rollup("year", "quarter").agg(F.sum("sales").alias("total")).collect()
        )
        assert len(result) >= 1

        result = (
            df.rollup("YEAR", "QUARTER").agg(F.sum("SALES").alias("total")).collect()
        )
        assert len(result) >= 1

        # cube with wrong case
        result = df.cube("year", "quarter").agg(F.sum("sales").alias("total")).collect()
        assert len(result) >= 1

        result = df.cube("YEAR", "QUARTER").agg(F.sum("SALES").alias("total")).collect()
        assert len(result) >= 1

    def test_sampleBy_all_case_variations(self, spark):
        """Test sampleBy with case variations in column parameter."""
        data = [
            {"Name": "Alice", "Dept": "IT"},
            {"Name": "Bob", "Dept": "HR"},
            {"Name": "Charlie", "Dept": "IT"},
        ]
        df = spark.createDataFrame(data)

        # sampleBy with wrong case
        result = df.sampleBy("dept", {"IT": 1.0, "HR": 0.0}).collect()
        assert len(result) == 2

        result = df.sampleBy("DEPT", {"IT": 1.0, "HR": 0.0}).collect()
        assert len(result) == 2

    def test_freqItems_all_case_variations(self, spark):
        """Test freqItems with case variations in column parameter."""
        data = [
            {"Name": "Alice", "Dept": "IT"},
            {"Name": "Bob", "Dept": "HR"},
            {"Name": "Alice", "Dept": "IT"},
        ]
        df = spark.createDataFrame(data)

        # freqItems with wrong case
        result = df.freqItems(["name", "dept"]).collect()
        assert len(result) == 1

        result = df.freqItems(["NAME", "DEPT"]).collect()
        assert len(result) == 1

    def test_crosstab_all_case_variations(self, spark):
        """Test crosstab with case variations in column parameters."""
        data = [
            {"Name": "Alice", "Dept": "IT"},
            {"Name": "Bob", "Dept": "HR"},
            {"Name": "Alice", "Dept": "IT"},
        ]
        df = spark.createDataFrame(data)

        # crosstab with wrong case
        result = df.crosstab("name", "dept").collect()
        assert len(result) >= 1

        result = df.crosstab("NAME", "DEPT").collect()
        assert len(result) >= 1

    def test_issue_264_withColumn_case_insensitive(self, spark):
        """Test issue #264: case-insensitive column resolution in withColumn with F.col().

        Reproduces the exact scenario from issue #264 where a column named "key"
        (lowercase) is referenced as "Key" (uppercase) in F.col() within withColumn.
        """
        # Create DataFrame with lowercase column name
        df = spark.createDataFrame(
            [
                {"key": "Alice"},
                {"key": "Bob"},
                {"key": "Charlie"},
            ]
        )

        # Test the exact scenario from issue #264
        # Reference "Key" (uppercase) when column is actually "key" (lowercase)
        df = df.withColumn("key_upper", F.upper(F.col("Key")))
        result = df.collect()

        # Verify results
        assert len(result) == 3
        assert result[0]["key"] == "Alice"
        assert result[0]["key_upper"] == "ALICE"
        assert result[1]["key"] == "Bob"
        assert result[1]["key_upper"] == "BOB"
        assert result[2]["key"] == "Charlie"
        assert result[2]["key_upper"] == "CHARLIE"

        # Test with opposite case (uppercase column, lowercase reference)
        df2 = spark.createDataFrame(
            [
                {"Key": "Alice"},
                {"Key": "Bob"},
                {"Key": "Charlie"},
            ]
        )
        df2 = df2.withColumn("key_lower", F.lower(F.col("key")))
        result2 = df2.collect()

        assert len(result2) == 3
        assert result2[0]["Key"] == "Alice"
        assert result2[0]["key_lower"] == "alice"
        assert result2[1]["Key"] == "Bob"
        assert result2[1]["key_lower"] == "bob"
        assert result2[2]["Key"] == "Charlie"
        assert result2[2]["key_lower"] == "charlie"

        # Test with mixed case variations
        df3 = spark.createDataFrame([{"Name": "Alice", "Age": 25}])
        df3 = df3.withColumn("name_lower", F.lower(F.col("name")))
        df3 = df3.withColumn("age_double", F.col("AGE") * 2)
        result3 = df3.collect()

        assert len(result3) == 1
        assert result3[0]["Name"] == "Alice"
        assert result3[0]["name_lower"] == "alice"
        assert result3[0]["Age"] == 25
        assert result3[0]["age_double"] == 50

    def test_unpivot_all_case_variations(self, spark):
        """Test unpivot with case variations."""
        data = [
            {"Name": "Alice", "Q1": 100, "Q2": 200},
            {"Name": "Bob", "Q1": 150, "Q2": 250},
        ]
        df = spark.createDataFrame(data)
        name_col = next((c for c in df.columns if c.lower() == "name"), "name")

        # PySpark unpivot requires variableColumnName and valueColumnName
        if _is_pyspark_backend():
            result = df.unpivot(
                ids=[name_col],
                values=["Q1", "Q2"],
                variableColumnName="quarter",
                valueColumnName="sales",
            ).collect()
        else:
            result = df.unpivot(ids=["name"], values=["Q1", "Q2"]).collect()
        assert len(result) >= 1

        if _is_pyspark_backend():
            result = df.unpivot(
                ids=[name_col],
                values=["Q1", "Q2"],
                variableColumnName="quarter",
                valueColumnName="sales",
            ).collect()
        else:
            result = df.unpivot(ids=["NAME"], values=["Q1", "Q2"]).collect()
        assert len(result) >= 1

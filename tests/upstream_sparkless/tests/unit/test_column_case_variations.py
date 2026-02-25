"""
Comprehensive tests for column name case variations.

Tests all different ways to refer to columns with various case combinations
to ensure case-insensitive resolution works correctly across all operations.
"""

import pytest
from sparkless.sql import SparkSession, functions as F
from sparkless.spark_types import StructType, StructField, StringType, IntegerType


class TestColumnCaseVariations:
    """Test all different ways to refer to columns with wrong case."""

    @pytest.fixture
    def spark(self):
        """Create SparkSession for testing."""
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

    def test_select_all_case_variations(self, sample_df):
        """Test select with all possible case variations."""
        # Original case
        result = sample_df.select("Name").collect()
        assert len(result) == 3

        # All lowercase
        result = sample_df.select("name").collect()
        assert len(result) == 3
        assert result[0]["Name"] == "Alice"

        # All uppercase
        result = sample_df.select("NAME").collect()
        assert len(result) == 3
        assert result[0]["Name"] == "Alice"

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
        assert "Name" in [f.name for f in result[0]._schema.fields]
        assert "Age" in [f.name for f in result[0]._schema.fields]
        assert "Salary" in [f.name for f in result[0]._schema.fields]

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
        assert result[0]["Name"] == "Alice"

        result = sample_df.orderBy("NAME").collect()
        assert result[0]["Name"] == "Alice"

        result = sample_df.orderBy("NaMe").collect()
        assert result[0]["Name"] == "Alice"

        # Descending
        result = sample_df.orderBy(F.col("name").desc()).collect()
        assert result[0]["Name"] == "Charlie"

        result = sample_df.orderBy(F.col("NAME").desc()).collect()
        assert result[0]["Name"] == "Charlie"

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

    def test_withColumnRenamed_all_case_variations(self, sample_df):
        """Test withColumnRenamed with all possible case variations."""
        # Rename using wrong case for existing column
        result = sample_df.withColumnRenamed("name", "full_name").collect()
        assert "full_name" in result[0].__dict__["_data_dict"]

        result = sample_df.withColumnRenamed("NAME", "full_name").collect()
        assert "full_name" in result[0].__dict__["_data_dict"]

        result = sample_df.withColumnRenamed("NaMe", "full_name").collect()
        assert "full_name" in result[0].__dict__["_data_dict"]

        # Multiple renames
        result = sample_df.withColumnsRenamed(
            {"name": "full_name", "age": "years"}
        ).collect()
        assert "full_name" in result[0].__dict__["_data_dict"]
        assert "years" in result[0].__dict__["_data_dict"]

        result = sample_df.withColumnsRenamed(
            {"NAME": "full_name", "AGE": "years"}
        ).collect()
        assert "full_name" in result[0].__dict__["_data_dict"]

    def test_drop_all_case_variations(self, sample_df):
        """Test drop with all possible case variations."""
        # Single column
        result_df = sample_df.drop("name")
        assert "Name" not in result_df.columns

        result_df = sample_df.drop("NAME")
        assert "Name" not in result_df.columns

        result_df = sample_df.drop("NaMe")
        assert "Name" not in result_df.columns

        # Multiple columns
        result_df = sample_df.drop("age", "salary")
        assert "Age" not in result_df.columns
        assert "Salary" not in result_df.columns

        result_df = sample_df.drop("AGE", "SALARY")
        assert "Age" not in result_df.columns
        assert "Salary" not in result_df.columns

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
        assert "Name" in [f.name for f in result[0]._schema.fields]

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
        assert "full_name" in [f.name for f in result[0]._schema.fields]

        result = sample_df.selectExpr("NAME as full_name").collect()
        assert "full_name" in [f.name for f in result[0]._schema.fields]

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
        from sparkless.window import Window

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
        # Schema should preserve original column names
        schema = sample_df.schema
        field_names = [f.name for f in schema.fields]

        # Original names should be present
        assert "Name" in field_names
        assert "Age" in field_names
        assert "Salary" in field_names
        assert "Dept" in field_names

        # But case-insensitive access should work
        assert sample_df.select("name").schema.fields[0].name == "Name"
        assert sample_df.select("NAME").schema.fields[0].name == "Name"

    def test_empty_dataframe_with_case_variations(self, spark):
        """Test operations on empty DataFrame with explicit schema."""
        schema = StructType(
            [
                StructField("Name", StringType()),
                StructField("Age", IntegerType()),
            ]
        )
        df = spark.createDataFrame([], schema=schema)

        # Should work with case variations even on empty DataFrame
        result_df = df.select("name")
        assert "Name" in [f.name for f in result_df.schema.fields]

        result_df = df.select("NAME")
        assert "Name" in [f.name for f in result_df.schema.fields]

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

    def test_attribute_access_all_case_variations(self, sample_df):
        """Test DataFrame attribute access (df.columnName) with case variations."""
        # Attribute access with original case
        col = sample_df.Name
        assert col.name == "Name"

        # Attribute access with wrong case should work
        col = sample_df.name
        assert col.name == "Name"

        col = sample_df.NAME
        assert col.name == "Name"

        col = sample_df.age
        assert col.name == "Age"

        col = sample_df.AGE
        assert col.name == "Age"

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

        # replace with subset using wrong case
        result = df.replace("IT", "Engineering", subset=["dept"]).collect()
        assert result[0]["Dept"] == "Engineering"

        result = df.replace("IT", "Engineering", subset=["DEPT"]).collect()
        assert result[0]["Dept"] == "Engineering"

        result = df.replace("Alice", "Alice Smith", subset=["name"]).collect()
        assert result[0]["Name"] == "Alice Smith"

        result = df.replace("Alice", "Alice Smith", subset=["NAME"]).collect()
        assert result[0]["Name"] == "Alice Smith"

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
        from sparkless.spark_types import StructType, StructField

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
        df = spark.createDataFrame(data, schema=schema)

        # Access nested fields with wrong case
        result = df.select("Person.name").collect()
        assert result[0]["Person.name"] == "Alice"

        result = df.select("Person.NAME").collect()
        assert result[0]["Person.NAME"] == "Alice"

        result = df.select("Person.age").collect()
        assert result[0]["Person.age"] == 25

        result = df.select("Person.AGE").collect()
        assert result[0]["Person.AGE"] == 25

        # Using F.col()
        result = df.select(F.col("Person.name")).collect()
        assert result[0]["Person.name"] == "Alice"

        result = df.select(F.col("Person.NAME")).collect()
        assert result[0]["Person.NAME"] == "Alice"

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

        # unpivot with wrong case in ids parameter
        result = df.unpivot(ids=["name"], values=["Q1", "Q2"]).collect()
        assert len(result) >= 1

        result = df.unpivot(ids=["NAME"], values=["Q1", "Q2"]).collect()
        assert len(result) >= 1

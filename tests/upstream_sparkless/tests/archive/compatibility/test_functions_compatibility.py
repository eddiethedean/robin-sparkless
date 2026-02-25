"""
Compatibility tests for function operations using expected outputs.

This module validates that mock-spark functions produce the same results
as PySpark by comparing against pre-generated expected outputs.
"""

import pytest
from tests.tools.output_loader import load_expected_output
from tests.tools.comparison_utils import assert_dataframes_equal
from sparkless import F


class TestFunctionsCompatibility:
    """Test function operation compatibility against expected PySpark outputs."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        from sparkless import SparkSession

        session = SparkSession("functions_test")
        yield session
        session.stop()

    def test_string_upper(self, spark):
        """Test upper function against expected outputs."""
        expected = load_expected_output("functions", "string_upper")

        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.upper(df.name))

        assert_dataframes_equal(result, expected)

    def test_string_lower(self, spark):
        """Test lower function against expected outputs."""
        expected = load_expected_output("functions", "string_lower")

        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.lower(df.name))

        assert_dataframes_equal(result, expected)

    def test_string_length(self, spark):
        """Test length function against expected outputs."""
        expected = load_expected_output("functions", "string_length")

        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.length(df.name))

        assert_dataframes_equal(result, expected)

    def test_string_substring(self, spark):
        """Test substring function against expected outputs."""
        expected = load_expected_output("functions", "string_substring")

        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.substring(df.name, 1, 3))

        assert_dataframes_equal(result, expected)

    def test_string_concat(self, spark):
        """Test concat function against expected outputs."""
        expected = load_expected_output("functions", "string_concat")

        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.concat(df.name, F.lit(" - "), df.email))

        assert_dataframes_equal(result, expected)

    def test_string_split(self, spark):
        """Test split function against expected outputs."""
        expected = load_expected_output("functions", "string_split")

        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.split(df.email, "@"))

        assert_dataframes_equal(result, expected)

    def test_string_regexp_extract(self, spark):
        """Test regexp_extract function against expected outputs."""
        expected = load_expected_output("functions", "string_regexp_extract")

        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.regexp_extract(df.email, r"@(.+)", 1))

        assert_dataframes_equal(result, expected)

    def test_math_abs(self, spark):
        """Test abs function against expected outputs."""
        expected = load_expected_output("functions", "math_abs")

        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.abs(df.salary))

        assert_dataframes_equal(result, expected)

    def test_math_round(self, spark):
        """Test round function against expected outputs."""
        expected = load_expected_output("functions", "math_round")

        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.round(df.salary, -3))

        assert_dataframes_equal(result, expected)

    def test_math_sqrt(self, spark):
        """Test sqrt function against expected outputs."""
        expected = load_expected_output("functions", "math_sqrt")

        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.sqrt(df.salary))

        assert_dataframes_equal(result, expected)

    def test_math_pow(self, spark):
        """Test pow function against expected outputs."""
        expected = load_expected_output("functions", "math_pow")

        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.pow(df.age, 2))

        assert_dataframes_equal(result, expected)

    def test_math_log(self, spark):
        """Test log function against expected outputs."""
        expected = load_expected_output("functions", "math_log")

        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.log(df.salary))

        assert_dataframes_equal(result, expected)

    def test_math_exp(self, spark):
        """Test exp function against expected outputs."""
        expected = load_expected_output("functions", "math_exp")

        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.exp(F.lit(1)))

        assert_dataframes_equal(result, expected)

    def test_agg_sum(self, spark):
        """Test sum aggregation against expected outputs."""
        expected = load_expected_output("functions", "agg_sum")

        df = spark.createDataFrame(expected["input_data"])
        result = df.groupBy("active").agg(F.sum(df.salary))

        assert_dataframes_equal(result, expected)

    def test_agg_avg(self, spark):
        """Test avg aggregation against expected outputs."""
        expected = load_expected_output("functions", "agg_avg")

        df = spark.createDataFrame(expected["input_data"])
        result = df.groupBy("active").agg(F.avg(df.salary))

        assert_dataframes_equal(result, expected)

    def test_agg_count(self, spark):
        """Test count aggregation against expected outputs."""
        expected = load_expected_output("functions", "agg_count")

        df = spark.createDataFrame(expected["input_data"])
        result = df.groupBy("active").agg(F.count(df.id))

        assert_dataframes_equal(result, expected)

    def test_agg_max(self, spark):
        """Test max aggregation against expected outputs."""
        expected = load_expected_output("functions", "agg_max")

        df = spark.createDataFrame(expected["input_data"])
        result = df.groupBy("active").agg(F.max(df.salary))

        assert_dataframes_equal(result, expected)

    def test_agg_min(self, spark):
        """Test min aggregation against expected outputs."""
        expected = load_expected_output("functions", "agg_min")

        df = spark.createDataFrame(expected["input_data"])
        result = df.groupBy("active").agg(F.min(df.salary))

        assert_dataframes_equal(result, expected)

    # Additional comprehensive function tests

    def test_string_trim(self, spark):
        """Test trim function against expected outputs."""
        expected = load_expected_output("functions", "string_trim")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.trim(df.name))
        assert_dataframes_equal(result, expected)

    def test_string_ltrim(self, spark):
        """Test ltrim function against expected outputs."""
        expected = load_expected_output("functions", "string_ltrim")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.ltrim(df.name))
        assert_dataframes_equal(result, expected)

    def test_string_rtrim(self, spark):
        """Test rtrim function against expected outputs."""
        expected = load_expected_output("functions", "string_rtrim")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.rtrim(df.name))
        assert_dataframes_equal(result, expected)

    def test_string_lpad(self, spark):
        """Test lpad function against expected outputs."""
        expected = load_expected_output("functions", "string_lpad")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.lpad(df.name, 10, " "))
        assert_dataframes_equal(result, expected)

    def test_string_rpad(self, spark):
        """Test rpad function against expected outputs."""
        expected = load_expected_output("functions", "string_rpad")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.rpad(df.name, 10, " "))
        assert_dataframes_equal(result, expected)

    def test_string_like(self, spark):
        """Test like function against expected outputs."""
        expected = load_expected_output("functions", "string_like")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.col("name").like("%a%"))
        assert_dataframes_equal(result, expected)

    def test_string_rlike(self, spark):
        """Test rlike function against expected outputs."""
        expected = load_expected_output("functions", "string_rlike")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.col("name").rlike("^[A-Z]"))
        assert_dataframes_equal(result, expected)

    def test_math_sin(self, spark):
        """Test sin function against expected outputs."""
        expected = load_expected_output("functions", "math_sin")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.sin(df.angle))
        assert_dataframes_equal(result, expected)

    def test_math_cos(self, spark):
        """Test cos function against expected outputs."""
        expected = load_expected_output("functions", "math_cos")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.cos(df.angle))
        assert_dataframes_equal(result, expected)

    def test_math_tan(self, spark):
        """Test tan function against expected outputs."""
        expected = load_expected_output("functions", "math_tan")
        df = spark.createDataFrame(expected["input_data"])
        # Add id to maintain row order, then select only the function result
        result = df.select(df.id, F.tan(df.angle)).orderBy("id").select("TAN(angle)")
        assert_dataframes_equal(result, expected)

    def test_math_ceil(self, spark):
        """Test ceil function against expected outputs."""
        expected = load_expected_output("functions", "math_ceil")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.ceil(df.value))
        assert_dataframes_equal(result, expected)

    def test_math_floor(self, spark):
        """Test floor function against expected outputs."""
        expected = load_expected_output("functions", "math_floor")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.floor(df.value))
        assert_dataframes_equal(result, expected)

    def test_math_greatest(self, spark):
        """Test greatest function against expected outputs."""
        expected = load_expected_output("functions", "math_greatest")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.greatest(df.a, df.b, df.c))
        assert_dataframes_equal(result, expected)

    def test_math_least(self, spark):
        """Test least function against expected outputs."""
        expected = load_expected_output("functions", "math_least")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.least(df.a, df.b, df.c))
        assert_dataframes_equal(result, expected)

    def test_conditional_when_otherwise(self, spark):
        """Test when/otherwise function against expected outputs."""
        expected = load_expected_output("functions", "conditional_when_otherwise")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(
            F.when(df.age > 30, "Senior").otherwise("Junior").alias("level")
        )
        assert_dataframes_equal(result, expected)

    def test_conditional_coalesce(self, spark):
        """Test coalesce function against expected outputs."""
        expected = load_expected_output("functions", "conditional_coalesce")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(
            F.coalesce(df.col1, df.col2, df.col3).alias("first_non_null")
        )
        assert_dataframes_equal(result, expected)

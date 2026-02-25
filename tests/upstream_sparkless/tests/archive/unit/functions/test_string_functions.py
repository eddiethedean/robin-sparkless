"""
Unit tests for string functions.
"""

import pytest
from sparkless import SparkSession, F


@pytest.mark.unit
class TestStringFunctions:
    """Test string functions."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        return SparkSession("test")

    @pytest.fixture
    def sample_data(self):
        """Sample data for testing."""
        return [
            {"id": 1, "name": "Alice", "email": "alice@example.com"},
            {"id": 2, "name": "Bob", "email": "bob@test.com"},
            {"id": 3, "name": "Charlie", "email": "charlie@company.org"},
        ]

    def test_upper_function(self, spark, sample_data):
        """Test upper function."""
        df = spark.createDataFrame(sample_data)
        result = df.select(F.upper("name").alias("name_upper"))

        assert result.count() == 3
        assert len(result.columns) == 1
        assert "name_upper" in result.columns

    def test_lower_function(self, spark, sample_data):
        """Test lower function."""
        df = spark.createDataFrame(sample_data)
        result = df.select(F.lower("name").alias("name_lower"))

        assert result.count() == 3
        assert len(result.columns) == 1
        assert "name_lower" in result.columns

    def test_length_function(self, spark, sample_data):
        """Test length function."""
        df = spark.createDataFrame(sample_data)
        result = df.select(F.length("name").alias("name_length"))

        assert result.count() == 3
        assert len(result.columns) == 1
        assert "name_length" in result.columns

    def test_substring_function(self, spark, sample_data):
        """Test substring function."""
        df = spark.createDataFrame(sample_data)
        result = df.select(F.substring("name", 1, 3).alias("name_sub"))

        assert result.count() == 3
        assert len(result.columns) == 1
        assert "name_sub" in result.columns

    def test_concat_function(self, spark, sample_data):
        """Test concat function."""
        df = spark.createDataFrame(sample_data)
        result = df.select(F.concat("name", F.lit(" - "), "email").alias("full_info"))

        assert result.count() == 3
        assert len(result.columns) == 1
        assert "full_info" in result.columns

    def test_split_function(self, spark, sample_data):
        """Test split function."""
        df = spark.createDataFrame(sample_data)
        result = df.select(F.split("email", "@").alias("email_parts"))

        assert result.count() == 3
        assert len(result.columns) == 1
        assert "email_parts" in result.columns

    def test_regexp_extract_function(self, spark, sample_data):
        """Test regexp_extract function."""
        df = spark.createDataFrame(sample_data)
        result = df.select(F.regexp_extract("email", r"@(.+)", 1).alias("domain"))

        assert result.count() == 3
        assert len(result.columns) == 1
        assert "domain" in result.columns

    def test_contains_function(self, spark, sample_data):
        """Test contains function."""
        df = spark.createDataFrame(sample_data)
        result = df.select(F.contains("name", "li").alias("contains_li"))

        assert result.count() == 3
        assert len(result.columns) == 1
        assert "contains_li" in result.columns
        rows = result.collect()
        assert rows[0]["contains_li"] is True  # "Alice" contains "li"
        assert rows[1]["contains_li"] is False  # "Bob" does not contain "li"

    def test_left_function(self, spark, sample_data):
        """Test left function."""
        df = spark.createDataFrame(sample_data)
        result = df.select(F.left("name", 2).alias("name_left"))

        assert result.count() == 3
        assert len(result.columns) == 1
        assert "name_left" in result.columns
        rows = result.collect()
        assert rows[0]["name_left"] == "Al"  # First 2 chars of "Alice"

    def test_right_function(self, spark, sample_data):
        """Test right function."""
        df = spark.createDataFrame(sample_data)
        result = df.select(F.right("name", 2).alias("name_right"))

        assert result.count() == 3
        assert len(result.columns) == 1
        assert "name_right" in result.columns
        rows = result.collect()
        assert rows[0]["name_right"] == "ce"  # Last 2 chars of "Alice"

    def test_btrim_function(self, spark, sample_data):
        """Test btrim function."""
        df = spark.createDataFrame([{"text": "  hello  "}, {"text": "world"}])
        result = df.select(F.btrim("text").alias("text_trimmed"))

        assert result.count() == 2
        assert len(result.columns) == 1
        assert "text_trimmed" in result.columns
        rows = result.collect()
        assert rows[0]["text_trimmed"] == "hello"  # Trimmed whitespace

    def test_btrim_with_chars_function(self, spark):
        """Test btrim function with specific characters."""
        df = spark.createDataFrame([{"text": "xxxhelloxxx"}])
        result = df.select(F.btrim("text", "x").alias("text_trimmed"))

        assert result.count() == 1
        rows = result.collect()
        assert rows[0]["text_trimmed"] == "hello"  # Trimmed 'x' characters

    def test_bit_length_function(self, spark, sample_data):
        """Test bit_length function."""
        df = spark.createDataFrame(sample_data)
        result = df.select(F.bit_length("name").alias("name_bit_length"))

        assert result.count() == 3
        assert len(result.columns) == 1
        assert "name_bit_length" in result.columns
        rows = result.collect()
        # "Alice" is 5 characters, UTF-8 encoded = 5 bytes = 40 bits
        assert rows[0]["name_bit_length"] == 40

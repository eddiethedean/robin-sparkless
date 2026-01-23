"""
Basic tests for robin-sparkless PySpark compatibility
"""

import pytest
from robin_sparkless import SparkSession, functions as F


def test_spark_session_creation():
    """Test creating a SparkSession"""
    spark = SparkSession.builder.appName("test").getOrCreate()
    assert spark is not None


def test_create_dataframe():
    """Test creating a DataFrame from Python data"""
    spark = SparkSession.builder.appName("test").getOrCreate()
    
    # Create DataFrame from list of tuples
    data = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
    df = spark.createDataFrame(data, ["id", "name"])
    
    assert df is not None
    assert df.count() == 3
    assert "id" in df.columns()
    assert "name" in df.columns()


def test_dataframe_filter():
    """Test DataFrame filtering"""
    spark = SparkSession.builder.appName("test").getOrCreate()
    data = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
    df = spark.createDataFrame(data, ["id", "name"])
    
    # Filter using column comparison
    filtered = df.filter(df["id"] > 1)
    assert filtered.count() == 2


def test_dataframe_select():
    """Test DataFrame column selection"""
    spark = SparkSession.builder.appName("test").getOrCreate()
    data = [(1, "Alice", 25), (2, "Bob", 30)]
    df = spark.createDataFrame(data, ["id", "name", "age"])
    
    selected = df.select(["id", "name"])
    assert "id" in selected.columns()
    assert "name" in selected.columns()
    assert "age" not in selected.columns()


def test_dataframe_groupby():
    """Test DataFrame groupBy operation"""
    spark = SparkSession.builder.appName("test").getOrCreate()
    data = [("A", 10), ("A", 20), ("B", 30)]
    df = spark.createDataFrame(data, ["category", "value"])
    
    grouped = df.groupBy(["category"])
    result = grouped.count()
    assert result is not None


def test_column_operations():
    """Test Column operations"""
    spark = SparkSession.builder.appName("test").getOrCreate()
    data = [(1, "Alice"), (2, "Bob")]
    df = spark.createDataFrame(data, ["id", "name"])
    
    # Test column access
    col = df["id"]
    assert col is not None
    
    # Test column comparison
    filtered_col = df["id"] > 1
    assert filtered_col is not None


def test_functions():
    """Test SQL functions"""
    from robin_sparkless import col, lit, count
    
    # Test col function
    c = col("test_column")
    assert c is not None
    
    # Test lit function
    l = lit(42)
    assert l is not None
    
    # Test count function
    cnt = count(col("test"))
    assert cnt is not None


def test_spark_session_builder():
    """Test SparkSession builder pattern"""
    builder = SparkSession.builder
    builder = builder.appName("MyApp")
    builder = builder.master("local")
    builder = builder.config("spark.sql.shuffle.partitions", "2")
    
    spark = builder.getOrCreate()
    assert spark is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

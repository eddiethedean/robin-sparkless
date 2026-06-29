"""Issue #1649: Column.substr() accepts Column expressions for position/length."""

from sparkless.testing import get_imports

imports = get_imports()
SparkSession = imports.SparkSession
F = imports.F


def test_substr_column_start_and_length(spark) -> None:
    """substr(pos, length) with Column args from instr/length matches PySpark."""
    df = spark.createDataFrame([("hello world",)], ["col1"])
    pos = F.instr(F.col("col1"), "world")
    result = df.withColumn("col2", F.col("col1").substr(pos, F.lit(5)))
    rows = result.collect()
    assert rows[0]["col2"] == "world"


def test_substr_column_start_literal_length(spark) -> None:
    """substr with Column start and int length."""
    df = spark.createDataFrame([("hello world",)], ["col1"])
    pos = F.instr(F.col("col1"), "hello")
    rows = df.withColumn("col2", F.col("col1").substr(pos, 5)).collect()
    assert rows[0]["col2"] == "hello"


def test_substr_column_length_from_expr(spark) -> None:
    """substr with int start and Column length."""
    df = spark.createDataFrame([("abcdef",)], ["col1"])
    length = F.length(F.lit("abc"))
    rows = df.withColumn("col2", F.col("col1").substr(1, length)).collect()
    assert rows[0]["col2"] == "abc"

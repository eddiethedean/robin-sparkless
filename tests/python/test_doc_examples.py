"""Verify that code examples from the documentation run correctly."""

import pytest
import robin_sparkless as rs


def test_user_guide_filter() -> None:
    """USER_GUIDE: Filter example."""
    spark = rs.SparkSession.builder().app_name("doc_test").get_or_create()
    df = spark.create_dataframe([(1, 25, "Alice"), (2, 30, "Bob"), (3, 35, "Charlie")], ["id", "age", "name"])
    adults = df.filter(rs.col("age") > rs.lit(25))
    rows = adults.collect()
    assert len(rows) == 2
    assert rows[0]["age"] == 30
    assert rows[1]["age"] == 35


def test_user_guide_when_then_otherwise() -> None:
    """USER_GUIDE: when/then/otherwise nested example."""
    spark = rs.SparkSession.builder().app_name("doc_test").get_or_create()
    df = spark.create_dataframe([(1, 10, "a"), (2, 25, "b"), (3, 70, "c")], ["id", "age", "name"])
    df2 = df.with_column(
        "category",
        rs.when(rs.col("age") >= 65)
        .then(rs.lit("senior"))
        .otherwise(
            rs.when(rs.col("age") >= 18).then(rs.lit("adult")).otherwise(rs.lit("minor"))
        ),
    )
    rows = df2.collect()
    assert len(rows) == 3
    assert rows[0]["category"] == "minor"
    assert rows[1]["category"] == "adult"
    assert rows[2]["category"] == "senior"


def test_user_guide_na_fill_drop() -> None:
    """USER_GUIDE: na().fill() and na().drop()."""
    spark = rs.SparkSession.builder().app_name("doc_test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [{"x": 1, "y": None}, {"x": 2, "y": 5}, {"x": None, "y": 7}],
        [("x", "bigint"), ("y", "bigint")],
    )
    filled = df.na().fill(rs.lit(0))
    rows = filled.collect()
    assert rows[0]["y"] == 0
    assert rows[2]["x"] == 0
    dropped = df.na().drop(subset=["x"])  # Drop rows with null in "x" (removes row 2)
    assert len(dropped.collect()) == 2


def test_user_guide_create_dataframe_from_rows() -> None:
    """USER_GUIDE: _create_dataframe_from_rows."""
    spark = rs.SparkSession.builder().app_name("doc_test").get_or_create()
    schema = [("id", "bigint"), ("name", "string"), ("score", "double")]
    rows = [
        {"id": 1, "name": "Alice", "score": 95.5},
        {"id": 2, "name": "Bob", "score": 87.0},
    ]
    df = spark._create_dataframe_from_rows(rows, schema)
    result = df.collect()
    assert len(result) == 2
    assert result[0]["name"] == "Alice"
    assert result[1]["score"] == 87.0


def test_user_guide_persistence_temp_view() -> None:
    """USER_GUIDE: createOrReplaceTempView."""
    spark = rs.SparkSession.builder().app_name("doc_test").get_or_create()
    df = spark.create_dataframe([(1, 25, "Alice")], ["id", "age", "name"])
    df.createOrReplaceTempView("people")
    result = spark.sql("SELECT name, age FROM people WHERE age > 20")
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["name"] == "Alice"


def test_readme_python_quickstart() -> None:
    """README: Python quick start example."""
    spark = rs.SparkSession.builder().app_name("demo").get_or_create()
    df = spark.create_dataframe([(1, 25, "Alice"), (2, 30, "Bob")], ["id", "age", "name"])
    filtered = df.filter(rs.col("age").gt(rs.lit(26)))
    rows = filtered.collect()
    assert rows == [{"id": 2, "age": 30, "name": "Bob"}]

"""Verify that code examples from the documentation run correctly."""

import pytest

from sparkless.testing import get_imports


_imports = get_imports()
F = _imports.F


def test_user_guide_filter(spark) -> None:
    """USER_GUIDE: Filter example."""
    df = spark.createDataFrame(
        [(1, 25, "Alice"), (2, 30, "Bob"), (3, 35, "Charlie")], ["id", "age", "name"]
    )
    adults = df.filter(F.col("age") > F.lit(25))
    rows = adults.collect()
    assert len(rows) == 2
    assert rows[0]["age"] == 30
    assert rows[1]["age"] == 35


def test_user_guide_when_then_otherwise(spark) -> None:
    """USER_GUIDE: when/then/otherwise nested example."""
    df = spark.createDataFrame(
        [(1, 10, "a"), (2, 25, "b"), (3, 70, "c")], ["id", "age", "name"]
    )
    df2 = df.withColumn(
        "category",
        F.when(F.col("age") >= 65, "senior")
        .when(F.col("age") >= 18, "adult")
        .otherwise("minor"),
    )
    rows = df2.collect()
    assert len(rows) == 3
    assert rows[0]["category"] == "minor"
    assert rows[1]["category"] == "adult"
    assert rows[2]["category"] == "senior"


def test_user_guide_na_fill_drop(spark) -> None:
    """USER_GUIDE: na().fill() and na().drop()."""
    df = spark.createDataFrame(
        [{"x": 1, "y": None}, {"x": 2, "y": 5}, {"x": None, "y": 7}],
        ["x", "y"],
    )
    filled = df.na.fill(0)
    rows = filled.collect()
    assert rows[0]["y"] == 0
    assert rows[2]["x"] == 0
    dropped = df.na.drop(subset=["x"])  # Drop rows with null in "x" (removes row 2)
    assert len(dropped.collect()) == 2


def test_user_guide_create_dataframe_from_rows(spark) -> None:
    """USER_GUIDE: _create_dataframe_from_rows."""
    schema = ["id", "name", "score"]
    rows = [
        {"id": 1, "name": "Alice", "score": 95.5},
        {"id": 2, "name": "Bob", "score": 87.0},
    ]
    df = spark.createDataFrame(rows, schema)
    result = df.collect()
    assert len(result) == 2
    assert result[0]["name"] == "Alice"
    assert result[1]["score"] == 87.0


def test_user_guide_persistence_temp_view(spark) -> None:
    """USER_GUIDE: createOrReplaceTempView."""
    df = spark.createDataFrame([(1, 25, "Alice")], ["id", "age", "name"])
    try:
        df.createOrReplaceTempView("people")
        result = spark.sql("SELECT name, age FROM people WHERE age > 20")
    except AttributeError:
        pytest.skip("sql feature not built")
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["name"] == "Alice"


def test_readme_python_quickstart(spark) -> None:
    """README: Python quick start example (runs both main README and README-Python)."""
    from tests.utils import _row_to_dict, assert_rows_equal

    df = spark.createDataFrame(
        [(1, 25, "Alice"), (2, 30, "Bob"), (3, 35, "Charlie")],
        ["id", "age", "name"],
    )
    filtered = df.filter(F.col("age") > F.lit(26))
    rows = filtered.collect()
    actual = [_row_to_dict(r) for r in rows]
    expected = [
        {"id": 2, "age": 30, "name": "Bob"},
        {"id": 3, "age": 35, "name": "Charlie"},
    ]
    assert_rows_equal(actual, expected, order_matters=True)

"""
Ported Sparkless parity tests for DataFrame operations.

These tests use the same input and expected data as Sparkless
tests/parity/dataframe (expected_outputs from PySpark). Run with robin_sparkless.
"""

from __future__ import annotations
import pytest

from tests.fixtures.spark_imports import get_spark_imports
from tests.utils import assert_rows_equal

_imports = get_spark_imports()
F = _imports.F


# Shared input data (from Sparkless expected_outputs dataframe_operations)
INPUT_EMPLOYEES = [
    {"id": 1, "name": "Alice", "age": 25, "salary": 50000, "department": "IT"},
    {"id": 2, "name": "Bob", "age": 30, "salary": 60000, "department": "HR"},
    {"id": 3, "name": "Charlie", "age": 35, "salary": 70000, "department": "IT"},
    {"id": 4, "name": "David", "age": 40, "salary": 80000, "department": "Finance"},
]
INPUT_EMPLOYEES_TUPLES = [
    (1, "Alice", 25, 50000, "IT"),
    (2, "Bob", 30, 60000, "HR"),
    (3, "Charlie", 35, 70000, "IT"),
    (4, "David", 40, 80000, "Finance"),
]
SCHEMA_EMPLOYEES = [
    "id",
    "name",
    "age",
    "salary",
    "department",
]


def test_filter_salary_gt_60000(spark) -> None:
    """Ported from Sparkless test_filter_with_boolean: filter(salary > 60000)."""
    df = spark.createDataFrame(INPUT_EMPLOYEES_TUPLES, SCHEMA_EMPLOYEES)
    result = df.filter(F.col("salary") > F.lit(60000))
    rows = [r.asDict() for r in result.collect()]
    expected = [
        {"age": 35, "department": "IT", "id": 3, "name": "Charlie", "salary": 70000},
        {"age": 40, "department": "Finance", "id": 4, "name": "David", "salary": 80000},
    ]
    assert_rows_equal(rows, expected, order_matters=False)


@pytest.mark.skip(
    reason="Issue #1274: unskip when fixing collect String schema semantics"
)
def test_filter_and_operator(spark) -> None:
    """Ported from Sparkless test_filter_with_and_operator: (a > 1) & (b > 1)."""
    data = [{"a": 1, "b": 2}, {"a": 2, "b": 3}, {"a": 3, "b": 1}]
    schema = ["a", "b"]
    df = spark.createDataFrame(data, schema)
    result = df.filter((F.col("a") > F.lit(1)) & (F.col("b") > F.lit(1)))
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["a"] == 2 and rows[0]["b"] == 3


@pytest.mark.skip(reason="Issue #1174: unskip when fixing")
def test_filter_or_operator(spark) -> None:
    """Ported from Sparkless test_filter_with_or_operator: (a > 1) | (b > 1)."""
    data = [{"a": 1, "b": 2}, {"a": 2, "b": 3}, {"a": 3, "b": 1}]
    schema = ["a", "b"]
    df = spark.createDataFrame(data, schema)
    result = df.filter((F.col("a") > F.lit(1)) | (F.col("b") > F.lit(1)))
    rows = result.collect()
    assert len(rows) == 3


def test_basic_select(spark) -> None:
    """Ported from Sparkless test_basic_select: select id, name, age."""
    df = spark.createDataFrame(INPUT_EMPLOYEES_TUPLES, SCHEMA_EMPLOYEES)
    result = df.select("id", "name", "age")
    rows = [r.asDict() for r in result.collect()]
    expected = [
        {"id": 1, "name": "Alice", "age": 25},
        {"id": 2, "name": "Bob", "age": 30},
        {"id": 3, "name": "Charlie", "age": 35},
        {"id": 4, "name": "David", "age": 40},
    ]
    assert_rows_equal(rows, expected, order_matters=True)


def test_select_with_alias(spark) -> None:
    """Ported from Sparkless test_select_with_alias: select id as user_id, name as full_name."""
    df = spark.createDataFrame(INPUT_EMPLOYEES_TUPLES, SCHEMA_EMPLOYEES)
    result = df.select(F.col("id").alias("user_id"), F.col("name").alias("full_name"))
    rows = [r.asDict() for r in result.collect()]
    expected = [
        {"user_id": 1, "full_name": "Alice"},
        {"user_id": 2, "full_name": "Bob"},
        {"user_id": 3, "full_name": "Charlie"},
        {"user_id": 4, "full_name": "David"},
    ]
    assert_rows_equal(rows, expected, order_matters=True)


def test_aggregation_avg_count(spark) -> None:
    """Ported from Sparkless test_aggregation: groupBy(department).agg(avg(salary), count(id))."""
    df = spark.createDataFrame(INPUT_EMPLOYEES_TUPLES, SCHEMA_EMPLOYEES)
    result = df.groupBy("department").agg(
        F.avg("salary").alias("avg_salary"),
        F.count("id").alias("count"),
    )
    rows = [r.asDict() for r in result.collect()]
    expected = [
        {"department": "Finance", "avg_salary": 80000.0, "count": 1},
        {"department": "HR", "avg_salary": 60000.0, "count": 1},
        {"department": "IT", "avg_salary": 60000.0, "count": 2},
    ]
    assert_rows_equal(rows, expected, order_matters=False)


def test_inner_join(spark) -> None:
    """Ported from Sparkless test_inner_join: employees join departments on dept_id."""
    employees_data = [
        (1, "Alice", 10, 50000),
        (2, "Bob", 20, 60000),
        (3, "Charlie", 10, 70000),
        (4, "David", 30, 55000),
    ]
    departments_data = [
        (10, "IT", "NYC"),
        (20, "HR", "LA"),
        (40, "Finance", "Chicago"),
    ]
    emp_schema = [
        "id",
        "name",
        "dept_id",
        "salary",
    ]
    dept_schema = ["dept_id", "name", "location"]
    emp_df = spark.createDataFrame(employees_data, emp_schema)
    dept_df = spark.createDataFrame(departments_data, dept_schema)
    result = emp_df.join(dept_df, on="dept_id", how="inner")
    rows = result.collect()
    # Expected: 3 rows (dept_id 10 x2, 20 x1). Column order may differ; compare as set of rows.
    assert len(rows) == 3
    by_id = {r["id"]: r for r in rows}
    assert 1 in by_id and by_id[1]["dept_id"] == 10 and by_id[1]["salary"] == 50000
    assert 2 in by_id and by_id[2]["dept_id"] == 20
    assert 3 in by_id and by_id[3]["dept_id"] == 10 and by_id[3]["salary"] == 70000

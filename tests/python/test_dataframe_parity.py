"""
Ported Sparkless parity tests for DataFrame operations.

These tests use the same input and expected data as Sparkless
tests/parity/dataframe (expected_outputs from PySpark). Run with robin_sparkless.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Allow importing utils when running pytest from project root
sys.path.insert(0, str(Path(__file__).resolve().parent))
from utils import assert_rows_equal


# Shared input data (from Sparkless expected_outputs dataframe_operations)
INPUT_EMPLOYEES = [
    {"id": 1, "name": "Alice", "age": 25, "salary": 50000, "department": "IT"},
    {"id": 2, "name": "Bob", "age": 30, "salary": 60000, "department": "HR"},
    {"id": 3, "name": "Charlie", "age": 35, "salary": 70000, "department": "IT"},
    {"id": 4, "name": "David", "age": 40, "salary": 80000, "department": "Finance"},
]
SCHEMA_EMPLOYEES = [
    ("id", "bigint"),
    ("name", "string"),
    ("age", "bigint"),
    ("salary", "double"),
    ("department", "string"),
]


def test_filter_salary_gt_60000(spark) -> None:
    """Ported from Sparkless test_filter_with_boolean: filter(salary > 60000)."""
    import robin_sparkless as rs

    df = spark._create_dataframe_from_rows(INPUT_EMPLOYEES, SCHEMA_EMPLOYEES)
    result = df.filter(rs.col("salary").gt(rs.lit(60000)))
    rows = result.collect()
    expected = [
        {"age": 35, "department": "IT", "id": 3, "name": "Charlie", "salary": 70000},
        {"age": 40, "department": "Finance", "id": 4, "name": "David", "salary": 80000},
    ]
    assert_rows_equal(rows, expected, order_matters=False)


def test_filter_and_operator(spark) -> None:
    """Ported from Sparkless test_filter_with_and_operator: (a > 1) & (b > 1)."""
    import robin_sparkless as rs

    data = [{"a": 1, "b": 2}, {"a": 2, "b": 3}, {"a": 3, "b": 1}]
    schema = [("a", "bigint"), ("b", "bigint")]
    df = spark._create_dataframe_from_rows(data, schema)
    expr1 = rs.col("a").gt(rs.lit(1))
    expr2 = rs.col("b").gt(rs.lit(1))
    result = df.filter(expr1.and_(expr2))
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["a"] == 2 and rows[0]["b"] == 3


def test_filter_or_operator(spark) -> None:
    """Ported from Sparkless test_filter_with_or_operator: (a > 1) | (b > 1)."""
    import robin_sparkless as rs

    data = [{"a": 1, "b": 2}, {"a": 2, "b": 3}, {"a": 3, "b": 1}]
    schema = [("a", "bigint"), ("b", "bigint")]
    df = spark._create_dataframe_from_rows(data, schema)
    expr1 = rs.col("a").gt(rs.lit(1))
    expr2 = rs.col("b").gt(rs.lit(1))
    result = df.filter(expr1.or_(expr2))
    rows = result.collect()
    assert len(rows) == 3


def test_basic_select(spark) -> None:
    """Ported from Sparkless test_basic_select: select id, name, age."""
    df = spark._create_dataframe_from_rows(INPUT_EMPLOYEES, SCHEMA_EMPLOYEES)
    result = df.select(["id", "name", "age"])
    rows = result.collect()
    expected = [
        {"id": 1, "name": "Alice", "age": 25},
        {"id": 2, "name": "Bob", "age": 30},
        {"id": 3, "name": "Charlie", "age": 35},
        {"id": 4, "name": "David", "age": 40},
    ]
    assert_rows_equal(rows, expected, order_matters=True)


def test_select_with_alias(spark) -> None:
    """Ported from Sparkless test_select_with_alias: select id as user_id, name as full_name."""
    import robin_sparkless as rs

    df = spark._create_dataframe_from_rows(INPUT_EMPLOYEES, SCHEMA_EMPLOYEES)
    # Robin select() takes column names; use with_column + select for aliasing
    result = (
        df.with_column("user_id", rs.col("id"))
        .with_column("full_name", rs.col("name"))
        .select(["user_id", "full_name"])
    )
    rows = result.collect()
    expected = [
        {"user_id": 1, "full_name": "Alice"},
        {"user_id": 2, "full_name": "Bob"},
        {"user_id": 3, "full_name": "Charlie"},
        {"user_id": 4, "full_name": "David"},
    ]
    assert_rows_equal(rows, expected, order_matters=True)


def test_aggregation_avg_count(spark) -> None:
    """Ported from Sparkless test_aggregation: groupBy(department).agg(avg(salary), count(id))."""
    import robin_sparkless as rs

    df = spark._create_dataframe_from_rows(INPUT_EMPLOYEES, SCHEMA_EMPLOYEES)
    grouped = df.group_by(["department"])
    result = grouped.agg(
        [
            rs.avg(rs.col("salary")).alias("avg_salary"),
            rs.count(rs.col("id")).alias("count"),
        ]
    )
    rows = result.collect()
    expected = [
        {"department": "Finance", "avg_salary": 80000.0, "count": 1},
        {"department": "HR", "avg_salary": 60000.0, "count": 1},
        {"department": "IT", "avg_salary": 60000.0, "count": 2},
    ]
    assert_rows_equal(rows, expected, order_matters=False)


def test_inner_join(spark) -> None:
    """Ported from Sparkless test_inner_join: employees join departments on dept_id."""
    employees_data = [
        {"id": 1, "name": "Alice", "dept_id": 10, "salary": 50000},
        {"id": 2, "name": "Bob", "dept_id": 20, "salary": 60000},
        {"id": 3, "name": "Charlie", "dept_id": 10, "salary": 70000},
        {"id": 4, "name": "David", "dept_id": 30, "salary": 55000},
    ]
    departments_data = [
        {"dept_id": 10, "name": "IT", "location": "NYC"},
        {"dept_id": 20, "name": "HR", "location": "LA"},
        {"dept_id": 40, "name": "Finance", "location": "Chicago"},
    ]
    emp_schema = [
        ("id", "bigint"),
        ("name", "string"),
        ("dept_id", "bigint"),
        ("salary", "bigint"),
    ]
    dept_schema = [("dept_id", "bigint"), ("name", "string"), ("location", "string")]
    emp_df = spark._create_dataframe_from_rows(employees_data, emp_schema)
    dept_df = spark._create_dataframe_from_rows(departments_data, dept_schema)
    result = emp_df.join(dept_df, ["dept_id"], "inner")
    rows = result.collect()
    # Expected: 3 rows (dept_id 10 x2, 20 x1). Column order may differ; compare as set of rows.
    assert len(rows) == 3
    by_id = {r["id"]: r for r in rows}
    assert 1 in by_id and by_id[1]["dept_id"] == 10 and by_id[1]["salary"] == 50000
    assert 2 in by_id and by_id[2]["dept_id"] == 20
    assert 3 in by_id and by_id[3]["dept_id"] == 10 and by_id[3]["salary"] == 70000

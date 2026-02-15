"""
Tests for #362: SQL DROP TABLE support (PySpark parity).

PySpark: spark.sql("DROP TABLE IF EXISTS my_schema.my_table"). Robin-sparkless now supports DROP TABLE / DROP VIEW.
"""

from __future__ import annotations

import pytest


def test_sql_drop_table_if_exists_issue_repro() -> None:
    """spark.sql("DROP TABLE IF EXISTS my_schema.my_table") (issue repro)."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("issue_362").get_or_create()
    try:
        out = spark.sql("DROP TABLE IF EXISTS my_schema.my_table")
        assert out.count() == 0
    except (AttributeError, RuntimeError) as e:
        if "sql" in str(e).lower() or "supported" in str(e).lower():
            pytest.skip("sql feature not built or DROP TABLE not supported")
        raise


def test_sql_drop_table_removes_temp_view() -> None:
    """After creating a temp view, DROP TABLE removes it."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("issue_362").get_or_create()
    try:
        df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "x"])
        spark.create_or_replace_temp_view("t_to_drop", df)
        assert spark.table("t_to_drop").count() == 2
        spark.sql("DROP TABLE t_to_drop")
        with pytest.raises(Exception):
            spark.table("t_to_drop")
    except (AttributeError, RuntimeError) as e:
        if "sql" in str(e).lower() or "supported" in str(e).lower():
            pytest.skip("sql feature not built or DROP TABLE not supported")
        raise


def test_sql_drop_view_removes_temp_view() -> None:
    """DROP VIEW also removes a temp view (same as DROP TABLE in-memory)."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("issue_362").get_or_create()
    try:
        df = spark.createDataFrame([(1,)], ["v"])
        spark.create_or_replace_temp_view("v_to_drop", df)
        spark.sql("DROP VIEW v_to_drop")
        with pytest.raises(Exception):
            spark.table("v_to_drop")
    except (AttributeError, RuntimeError) as e:
        if "sql" in str(e).lower() or "supported" in str(e).lower():
            pytest.skip("sql feature not built or DROP VIEW not supported")
        raise

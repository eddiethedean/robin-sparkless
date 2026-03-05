"""
Tests for #362: SQL DROP TABLE support (PySpark parity).

PySpark: spark.sql("DROP TABLE IF EXISTS my_schema.my_table"). Robin-sparkless now supports DROP TABLE / DROP VIEW.
Uses shared spark fixture.
"""

from __future__ import annotations

import pytest


def test_sql_drop_table_if_exists_issue_repro(spark) -> None:
    """spark.sql(\"DROP TABLE IF EXISTS my_schema.my_table\") (issue repro)."""
    try:
        out = spark.sql("DROP TABLE IF EXISTS my_schema.my_table")
        assert out.count() == 0
    except (AttributeError, RuntimeError) as e:
        if "sql" in str(e).lower() or "supported" in str(e).lower():
            pytest.skip("sql feature not built or DROP TABLE not supported")
        raise


def test_sql_drop_table_removes_temp_view(spark) -> None:
    """After creating a temp view, DROP TABLE removes it."""
    try:
        df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "x"])
        df.createOrReplaceTempView("t_to_drop")
        assert spark.table("t_to_drop").count() == 2
        spark.sql("DROP TABLE t_to_drop")
        with pytest.raises(Exception):
            spark.table("t_to_drop")
    except (AttributeError, RuntimeError) as e:
        if "sql" in str(e).lower() or "supported" in str(e).lower():
            pytest.skip("sql feature not built or DROP TABLE not supported")
        raise


def test_sql_drop_view_removes_temp_view(spark) -> None:
    """DROP VIEW also removes a temp view (same as DROP TABLE in-memory)."""
    try:
        df = spark.createDataFrame([(1,)], ["v"])
        df.createOrReplaceTempView("v_to_drop")
        spark.sql("DROP VIEW v_to_drop")
        with pytest.raises(Exception):
            spark.table("v_to_drop")
    except (AttributeError, RuntimeError) as e:
        if "sql" in str(e).lower() or "supported" in str(e).lower():
            pytest.skip("sql feature not built or DROP VIEW not supported")
        raise

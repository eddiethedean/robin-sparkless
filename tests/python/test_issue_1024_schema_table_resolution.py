"""
Tests for #1024: schema/table not found (test_schema.test_table).

PySpark accepts schema-qualified names: CREATE SCHEMA, saveAsTable("schema.table"),
spark.table("schema.table"). Robin-sparkless must resolve schema.table in the same session.
"""

from __future__ import annotations

import robin_sparkless as rs


def _spark() -> rs.SparkSession:
    return rs.SparkSession.builder().app_name("issue_1024").get_or_create()


def test_schema_qualified_table_resolution() -> None:
    """CREATE SCHEMA IF NOT EXISTS test_schema; saveAsTable('test_schema.test_table'); table('test_schema.test_table')."""
    spark = _spark()
    spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")
    data = [{"id": 1, "name": "a"}]
    schema = [("id", "int"), ("name", "string")]
    df = spark.createDataFrame(data, schema)
    df.write.mode("overwrite").saveAsTable("test_schema.test_table")
    result = spark.table("test_schema.test_table")
    assert result.count() == 1
    row = result.collect()[0]
    assert row["id"] == 1
    assert row["name"] == "a"


def test_schema_qualified_table_append_then_read() -> None:
    """saveAsTable(schema.table) overwrite then append; table(schema.table) sees both."""
    spark = _spark()
    spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")
    df1 = spark.createDataFrame(
        [{"id": 1, "name": "x"}], [("id", "int"), ("name", "string")]
    )
    df1.write.mode("overwrite").saveAsTable("test_schema.test_table")
    df2 = spark.createDataFrame(
        [{"id": 2, "name": "y"}], [("id", "int"), ("name", "string")]
    )
    df2.write.mode("append").saveAsTable("test_schema.test_table")
    result = spark.table("test_schema.test_table")
    assert result.count() == 2

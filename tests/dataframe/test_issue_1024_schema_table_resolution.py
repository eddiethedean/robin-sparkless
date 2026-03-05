"""
Tests for #1024: schema/table not found (test_schema.test_table).

PySpark accepts schema-qualified names: CREATE SCHEMA, saveAsTable("schema.table"),
spark.table("schema.table"). Robin-sparkless must resolve schema.table in the same session.
"""

from __future__ import annotations

from tests.utils import get_spark
import uuid


def _spark():
    return get_spark("issue_1024")


def test_schema_qualified_table_resolution() -> None:
    """CREATE SCHEMA; saveAsTable('schema.table'); table('schema.table')."""
    spark = _spark()
    schema = "test_schema_resolution"
    table = f"test_table_{uuid.uuid4().hex[:8]}"
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    data = [{"id": 1, "name": "a"}]
    df = spark.createDataFrame(data, ["id", "name"])
    df.write.mode("overwrite").saveAsTable(f"{schema}.{table}")
    result = spark.table(f"{schema}.{table}")
    assert result.count() == 1
    row = result.collect()[0]
    assert row["id"] == 1
    assert row["name"] == "a"


def test_schema_qualified_table_append_then_read() -> None:
    """saveAsTable(schema.table) overwrite then append; table(schema.table) sees both."""
    spark = _spark()
    schema = "test_schema_append"
    table = f"test_table_{uuid.uuid4().hex[:8]}"
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    df1 = spark.createDataFrame([{"id": 1, "name": "x"}], ["id", "name"])
    df1.write.mode("overwrite").saveAsTable(f"{schema}.{table}")
    df2 = spark.createDataFrame([{"id": 2, "name": "y"}], ["id", "name"])
    df2.write.mode("append").saveAsTable(f"{schema}.{table}")
    result = spark.table(f"{schema}.{table}")
    assert result.count() == 2

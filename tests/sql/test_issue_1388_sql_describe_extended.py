"""
Regression test for issue #1388: sql.describe_extended parity.

Assert PySpark behavior: DESCRIBE EXTENDED returns a DataFrame with schema
struct<col_name:string,data_type:string,comment:string>, column x with
bigint/long/int type, and explain() returns None (plan printed to stdout).
"""

import pytest


def test_issue_1388_sql_describe_extended_schema_data_and_explain(spark) -> None:
    """sql.describe_extended: schema, data, and explain match PySpark (issue #1388)."""
    df = spark.createDataFrame([(1,)], ["x"])
    df.createOrReplaceTempView("t")

    desc = spark.sql("DESCRIBE EXTENDED t")

    # PySpark schema: col_name, data_type, comment
    schema_str = desc.schema.simpleString()
    assert schema_str == "struct<col_name:string,data_type:string,comment:string>"

    # Data: column x with integer-like type (PySpark uses bigint)
    rows = desc.collect()
    row_by_name = {row["col_name"]: row["data_type"] for row in rows}
    assert "x" in row_by_name
    assert row_by_name["x"] in {"long", "bigint", "int"}

    # PySpark: explain() returns None and prints plan to stdout
    plan = desc.explain()
    assert plan is None

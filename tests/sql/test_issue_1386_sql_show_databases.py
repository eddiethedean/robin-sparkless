"""
Regression test for issue #1386: sql.show_databases parity.

Assert PySpark behavior: session.sql("SHOW DATABASES") returns a DataFrame with
schema struct<namespace:string>, at least the default namespace, and explain()
returns None (plan is printed to stdout).
"""

import pytest


def test_issue_1386_sql_show_databases_schema_data_and_explain(spark) -> None:
    """sql.show_databases: schema, data, and explain match PySpark (issue #1386)."""
    df = spark.sql("SHOW DATABASES")

    # PySpark schema: single column "namespace"
    schema_str = df.schema.simpleString()
    assert schema_str == "struct<namespace:string>"

    # Data: at least default namespace (PySpark may or may not include global_temp)
    rows = df.collect()
    names = [row["namespace"] for row in rows]
    assert len(names) >= 1
    assert "default" in names

    # PySpark: explain() returns None and prints plan to stdout
    plan = df.explain()
    assert plan is None

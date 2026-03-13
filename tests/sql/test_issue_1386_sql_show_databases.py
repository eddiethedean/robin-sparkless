"""
Regression test for issue #1386: sql.show_databases parity.

PySpark scenario (from the issue):

    def scenario_sql_show_databases(session):
        return session.sql("SHOW DATABASES")

This test exercises the same scenario against sparkless, ensuring that:

- ``session.sql("SHOW DATABASES")`` does not raise.
- The resulting schema's ``simpleString()`` matches the current struct form.
- The data includes both ``default`` and ``global_temp`` databases.
- ``explain()`` returns a non-empty plan string (no blank UI).
"""

import pytest


@pytest.mark.sparkless_only
def test_issue_1386_sql_show_databases_schema_data_and_explain(spark) -> None:
    """sql.show_databases: schema, data, and explain behavior (issue #1386)."""
    df = spark.sql("SHOW DATABASES")

    # Schema simpleString should match the current struct representation.
    schema_str = df.schema.simpleString()
    assert schema_str == "struct<databaseName:string>"

    # Data should include both default and global_temp namespaces.
    rows = df.collect()
    names = [row["databaseName"] for row in rows]
    assert "default" in names
    assert "global_temp" in names

    # explain() should produce a non-empty plan string.
    plan = df.explain()
    assert isinstance(plan, str)
    assert plan.strip() != ""

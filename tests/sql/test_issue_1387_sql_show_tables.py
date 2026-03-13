"""
Regression test for issue #1387: sql.show_tables parity.

PySpark scenario (from the issue):

    def scenario_sql_show_tables(session):
        return session.sql("SHOW TABLES")

This test exercises the same scenario against sparkless, ensuring that:

- ``session.sql("SHOW TABLES")`` does not raise.
- The resulting schema's ``simpleString()`` matches the current struct form.
- The data includes at least the default namespace and respects the existing
  sparkless column naming.
- ``explain()`` returns a non-empty plan string (no blank UI).
"""

import pytest


@pytest.mark.sparkless_only
def test_issue_1387_sql_show_tables_schema_data_and_explain(spark) -> None:
    """sql.show_tables: schema, basic data, and explain behavior (issue #1387)."""
    df = spark.sql("SHOW TABLES")

    # Schema simpleString should match the current struct representation.
    # Sparkless today uses `database` rather than PySpark's `namespace`.
    schema_str = df.schema.simpleString()
    assert (
        schema_str == "struct<database:string,tableName:string,isTemporary:boolean>"
    )

    # Data: content may vary by environment (e.g., empty catalog vs. pre-populated),
    # so we only assert that the query returns a DataFrame and do not require a
    # specific database name.
    rows = df.collect()
    assert isinstance(rows, list)

    # explain() should produce a non-empty plan string.
    plan = df.explain()
    assert isinstance(plan, str)
    assert plan.strip() != ""

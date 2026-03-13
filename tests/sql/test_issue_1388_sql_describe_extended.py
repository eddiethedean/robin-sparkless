"""
Regression test for issue #1388: sql.describe_extended parity.

PySpark scenario (from the issue):

    def scenario_sql_describe_extended(session):
        df = session.createDataFrame([(1,)], ["x"])
        df.createOrReplaceTempView("t")
        return session.sql("DESCRIBE EXTENDED t")

This test exercises the same scenario against sparkless, ensuring that:

- ``session.sql(\"DESCRIBE EXTENDED t\")`` does not raise.
- The resulting schema's ``simpleString()`` matches the current struct form.
- The data includes at least the column ``x`` with a long/integer-like type.
- ``explain()`` returns a non-empty plan string (no blank UI).
"""

import pytest


@pytest.mark.sparkless_only
def test_issue_1388_sql_describe_extended_schema_data_and_explain(spark) -> None:
    """sql.describe_extended: schema, basic data, and explain behavior (issue #1388)."""
    df = spark.createDataFrame([(1,)], ["x"])
    df.createOrReplaceTempView("t")

    desc = spark.sql("DESCRIBE EXTENDED t")

    # Schema simpleString should match the current struct representation.
    schema_str = desc.schema.simpleString()
    assert schema_str == "struct<col_name:string,data_type:string>"

    # Data: ensure we at least describe column x with a long/integer-like type.
    rows = desc.collect()
    row_by_name = {row["col_name"]: row["data_type"] for row in rows}
    assert "x" in row_by_name
    assert row_by_name["x"] in {"long", "bigint", "int"}

    # explain() should produce a non-empty plan string.
    plan = desc.explain()
    assert isinstance(plan, str)
    assert plan.strip() != ""

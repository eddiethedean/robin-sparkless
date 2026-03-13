"""
Regression test for issue #1382: df.select_star parity.

PySpark scenario (from the issue):

    def scenario_select_star(session):
        df = session.createDataFrame([(1, 2, 3)], ["a", "b", "c"])
        return df.select("*")

This test exercises the same scenario against sparkless, ensuring that:

- ``df.select(\"*\")`` does not raise.
- The resulting schema's ``simpleString()`` matches the expected struct form.
- ``df.explain()`` on the selected DataFrame returns a non-empty plan string.
"""

def test_issue_1382_select_star_schema_and_explain(spark) -> None:
    """df.select_star: select(\"*\") + schema + explain should behave sensibly (issue #1382)."""
    df = spark.createDataFrame([(1, 2, 3)], ["a", "b", "c"])

    # select("*") should not raise and should preserve all columns.
    result = df.select("*")

    # Schema simpleString should match the existing struct<long> representation.
    schema_str = result.schema.simpleString()
    assert schema_str == "struct<a:long,b:long,c:long>"

    # explain() should produce a non-empty plan string (no blank UI).
    plan = result.explain()
    assert isinstance(plan, str)
    assert plan.strip() != ""

    # show() should also work without raising for completeness.
    result.show()

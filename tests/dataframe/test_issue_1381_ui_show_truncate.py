"""
Regression test for issue #1381: ui.show_truncate parity.

PySpark scenario (from the issue):

    def scenario_show_truncate(session):
        df = session.createDataFrame([("x" * 200,)], ["s"])
        # show output differs across engines; we capture UI to compare.
        return df

This test exercises the same scenario against sparkless, ensuring that:

- ``df.show()`` does not raise.
- ``df.schema.simpleString()`` returns the expected struct<string> shape.
- ``df.explain()`` returns a non-empty plan string (no blank UI).
"""

from sparkless.sql import SparkSession


def test_issue_1381_show_truncate_schema_and_explain() -> None:
    """ui.show_truncate: show + schema + explain should behave sensibly (issue #1381)."""
    spark = SparkSession.builder.appName("issue_1381").getOrCreate()
    try:
        df = spark.createDataFrame([("x" * 200,)], ["s"])

        # show() should not raise, even with long strings.
        df.show()

        # Schema simpleString should match the existing struct<string> representation.
        schema_str = df.schema.simpleString()
        assert schema_str == "struct<s:string>"

        # explain() should produce a non-empty plan string (no blank UI).
        plan = df.explain()
        assert isinstance(plan, str)
        assert plan.strip() != ""
    finally:
        spark.stop()

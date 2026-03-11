"""
Regression test for issue #1384: window.last_default_frame parity.

PySpark scenario (from the issue):

    def scenario_window_last_default_frame(session):
        if _backend_is_pyspark(session):
            from pyspark.sql import functions as F  # type: ignore
            from pyspark.sql.window import Window  # type: ignore
        else:
            from sparkless.sql import functions as F  # type: ignore
            from sparkless.window import Window  # type: ignore

        df = session.createDataFrame(
            [(1, 100, "a"), (2, 90, "a"), (3, 80, "b")], ["id", "salary", "dept"]
        )
        win = Window.partitionBy("dept").orderBy("id")
        return df.withColumn("last_sal", F.last("salary").over(win)).orderBy("id")

This test exercises the same scenario against sparkless, ensuring that:

- The windowed last() expression with the default frame does not raise.
- The resulting schema's simpleString() matches the expected struct form.
- explain() returns a non-empty plan string (no blank UI).
- The computed last_sal values match the PySpark semantics.
"""

from sparkless.sql import SparkSession, functions as F
from sparkless.window import Window


def test_issue_1384_window_last_default_frame_schema_ui_and_data() -> None:
    """window.last_default_frame: last over partition with default frame (issue #1384)."""
    spark = SparkSession.builder.appName("issue_1384").getOrCreate()
    try:
        df = spark.createDataFrame(
            [(1, 100, "a"), (2, 90, "a"), (3, 80, "b")],
            ["id", "salary", "dept"],
        )
        win = Window.partitionBy("dept").orderBy("id")

        result = df.withColumn("last_sal", F.last("salary").over(win)).orderBy("id")

        # Schema simpleString should match the existing struct representation.
        schema_str = result.schema.simpleString()
        assert schema_str == "struct<id:long,salary:long,dept:string,last_sal:long>"

        # explain() should produce a non-empty plan string (no blank UI).
        plan = result.explain()
        assert isinstance(plan, str)
        assert plan.strip() != ""

        # Data values: for each partition ordered by id ASC, last() with the default
        # frame (unbounded preceding to current row) should yield the current salary.
        rows = result.collect()
        got = [(r["id"], r["salary"], r["dept"], r["last_sal"]) for r in rows]
        assert got == [
            (1, 100, "a", 100),
            (2, 90, "a", 90),
            (3, 80, "b", 80),
        ]
    finally:
        spark.stop()


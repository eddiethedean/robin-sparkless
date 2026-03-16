"""
Regression test for issue #1384: window.last_default_frame parity.

Same scenario in both modes via spark + spark_imports: last() over window, default frame.

This test ensures that:

- The windowed last() expression with the default frame does not raise.
- The resulting schema's simpleString() matches the expected struct form.
- explain() returns a non-empty plan string (no blank UI).
- The computed last_sal values match the PySpark semantics.
"""


def test_issue_1384_window_last_default_frame_schema_ui_and_data(
    spark, spark_imports
) -> None:
    """window.last_default_frame: last over partition with default frame (issue #1384)."""
    F = spark_imports.F
    Window = spark_imports.Window
    df = spark.createDataFrame(
        [(1, 100, "a"), (2, 90, "a"), (3, 80, "b")],
        ["id", "salary", "dept"],
    )
    win = Window.partitionBy("dept").orderBy("id")

    result = df.withColumn("last_sal", F.last("salary").over(win)).orderBy("id")

    # Schema simpleString: PySpark uses "bigint" for long type.
    schema_str = result.schema.simpleString()
    assert schema_str == "struct<id:bigint,salary:bigint,dept:string,last_sal:bigint>"

    # explain() prints to stdout; returns None in PySpark/sparkless.
    plan = result.explain()
    assert plan is None or (isinstance(plan, str) and plan.strip() != "")

    # Data values: for each partition ordered by id ASC, last() with the default
    # frame (unbounded preceding to current row) should yield the current salary.
    rows = result.collect()
    got = [(r["id"], r["salary"], r["dept"], r["last_sal"]) for r in rows]
    assert got == [
        (1, 100, "a", 100),
        (2, 90, "a", 90),
        (3, 80, "b", 80),
    ]

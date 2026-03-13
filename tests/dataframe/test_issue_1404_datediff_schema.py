"""
Regression test for issue #1404: datediff must return IntegerType (int), not LongType (long).

PySpark: struct<dd:int>
Sparkless (before fix): struct<dd:long>
"""

def test_datediff_returns_integer_type_not_long(spark, spark_imports):
    """Exact repro from issue #1404: datediff result schema must be int, not long."""
    F = spark_imports.F
    df = spark.createDataFrame(
        [("2020-01-10", "2020-01-02"), (None, "2020-01-02")],
        ["a", "b"],
    )
    result = df.select(F.datediff(F.col("a"), F.col("b")).alias("dd"))

    # PySpark returns struct<dd:int>; we must match (issue #1404).
    simple = result.schema.simpleString()
    assert "dd:int" in simple, (
        f"datediff result must be IntegerType (dd:int), got schema: {simple}"
    )
    assert "dd:long" not in simple, (
        f"datediff result must not be LongType (dd:long), got schema: {simple}"
    )

    rows = result.collect()
    assert len(rows) == 2
    assert rows[0]["dd"] == 8  # 2020-01-10 - 2020-01-02
    assert rows[1]["dd"] is None  # null - 2020-01-02

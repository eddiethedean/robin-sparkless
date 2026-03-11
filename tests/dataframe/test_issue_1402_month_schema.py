"""
Regression test for issue #1402: month must return IntegerType (int), not StringType (string).

PySpark: struct<m:int>, data [{'m': None}, {'m': 12}]
Sparkless (before fix): struct<m:string>, data [{'m': '12'}, {'m': None}]
"""

from sparkless.sql import SparkSession, functions as F


def test_month_returns_integer_type_not_string():
    """Exact repro from issue #1402: month result schema and data must be int, not string."""
    spark = SparkSession.builder.appName("issue_1402").getOrCreate()
    try:
        df = spark.createDataFrame([("2020-12-02",), (None,)], ["s"])
        result = df.select(F.month(F.col("s")).alias("m"))

        # PySpark returns struct<m:int>; we must match (issue #1402).
        simple = result.schema.simpleString()
        assert "m:int" in simple, (
            f"month result must be IntegerType (m:int), got schema: {simple}"
        )
        assert "m:string" not in simple, (
            f"month result must not be StringType (m:string), got schema: {simple}"
        )

        rows = result.collect()
        assert len(rows) == 2
        # Value for "2020-12-02" must be integer 12 (month), not string "12".
        values = [row["m"] for row in rows]
        assert 12 in values
        assert None in values
        for v in values:
            assert v is None or isinstance(v, int), (
                f"month values must be int or None, got {type(v).__name__}: {v!r}"
            )
    finally:
        spark.stop()

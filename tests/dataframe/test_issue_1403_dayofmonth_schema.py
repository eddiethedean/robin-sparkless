"""
Regression test for issue #1403: dayofmonth must return IntegerType (int), not StringType (string).

PySpark: struct<d:int>, data [{'d': None}, {'d': 2}]
Sparkless (before fix): struct<d:string>, data [{'d': '2'}, {'d': None}]
"""

from sparkless.sql import SparkSession, functions as F


def test_dayofmonth_returns_integer_type_not_string():
    """Exact repro from issue #1403: dayofmonth result schema and data must be int, not string."""
    spark = SparkSession.builder.appName("issue_1403").getOrCreate()
    try:
        df = spark.createDataFrame([("2020-12-02",), (None,)], ["s"])
        result = df.select(F.dayofmonth(F.col("s")).alias("d"))

        # PySpark returns struct<d:int>; we must match (issue #1403).
        simple = result.schema.simpleString()
        assert "d:int" in simple, (
            f"dayofmonth result must be IntegerType (d:int), got schema: {simple}"
        )
        assert "d:string" not in simple, (
            f"dayofmonth result must not be StringType (d:string), got schema: {simple}"
        )

        rows = result.collect()
        assert len(rows) == 2
        # Value for "2020-12-02" must be integer 2 (day of month), not string "2".
        values = [row["d"] for row in rows]
        assert 2 in values
        assert None in values
        for v in values:
            assert v is None or isinstance(v, int), (
                f"dayofmonth values must be int or None, got {type(v).__name__}: {v!r}"
            )
    finally:
        spark.stop()

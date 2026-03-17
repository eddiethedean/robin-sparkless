"""Parity test for #1408: array_contains(null_array, value) returns null (PySpark parity)."""

from tests.utils import assert_rows_equal, run_with_pyspark_expected, _row_to_dict


def test_array_contains_null_array_matches_pyspark(spark, spark_imports):
    F = spark_imports.F

    base = spark.range(0, 3).select(
        F.col("id"),
        F.lit(None).cast("array<string>").alias("null_arr"),
    )
    df = base.select(
        F.when(F.col("id") == F.lit(0), F.array(F.lit("a"), F.lit("b")))
        .when(F.col("id") == F.lit(1), F.array(F.lit("x")))
        .otherwise(F.col("null_arr"))
        .alias("arr")
    )
    # Use a value column to avoid scalar-literal broadcast quirks in some engines.
    df2 = df.select(F.col("arr"), F.lit("a").alias("v"))
    actual_df = df2.select(F.array_contains(F.col("arr"), F.col("v")).alias("out"))
    actual_rows = [_row_to_dict(r) for r in actual_df.collect()]

    def pyspark_fn(pyspark_spark, pyspark_F):
        base = pyspark_spark.range(0, 3).select(
            pyspark_F.col("id"),
            pyspark_F.lit(None).cast("array<string>").alias("null_arr"),
        )
        df = base.select(
            pyspark_F.when(
                pyspark_F.col("id") == pyspark_F.lit(0),
                pyspark_F.array(pyspark_F.lit("a"), pyspark_F.lit("b")),
            )
            .when(
                pyspark_F.col("id") == pyspark_F.lit(1),
                pyspark_F.array(pyspark_F.lit("x")),
            )
            .otherwise(pyspark_F.col("null_arr"))
            .alias("arr")
        )
        df2 = df.select(pyspark_F.col("arr"), pyspark_F.lit("a").alias("v"))
        return df2.select(
            pyspark_F.array_contains(pyspark_F.col("arr"), pyspark_F.col("v")).alias(
                "out"
            )
        ).collect()

    expected_rows = run_with_pyspark_expected(
        pyspark_fn,
        fallback_expected=[{"out": True}, {"out": False}, {"out": None}],
    )

    assert_rows_equal(actual_rows, expected_rows, order_matters=True)

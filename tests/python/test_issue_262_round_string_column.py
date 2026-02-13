"""
Tests for issue #262: F.round() on string column (PySpark parity).

PySpark supports F.round() on string columns that contain numeric values (implicit cast).
Robin previously raised RuntimeError: round can only be used on numeric types.
"""

from __future__ import annotations

import robin_sparkless as rs

F = rs


def test_round_string_column_implicit_cast() -> None:
    """with_column with F.round(F.col('val')) on string column succeeds; matches PySpark [10.0, 10.0]."""
    spark = F.SparkSession.builder().app_name("test_262").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    df = create_df(
        [{"val": "10.4"}, {"val": "9.6"}],
        [("val", "string")],
    )
    df = df.with_column("rounded", F.round(F.col("val")))
    out = df.collect()
    assert len(out) == 2
    assert out[0]["val"] == "10.4"
    assert out[0]["rounded"] == 10.0
    assert out[1]["val"] == "9.6"
    assert out[1]["rounded"] == 10.0


def test_round_string_column_with_scale() -> None:
    """round on string column with scale=1 yields one decimal place."""
    spark = F.SparkSession.builder().app_name("test_262").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    df = create_df(
        [{"val": "10.44"}, {"val": "9.66"}],
        [("val", "string")],
    )
    df = df.with_column("rounded", F.round(F.col("val"), 1))
    out = df.collect()
    assert len(out) == 2
    assert out[0]["rounded"] == 10.4
    assert out[1]["rounded"] == 9.7


def test_round_string_column_strips_whitespace() -> None:
    """round on string column with leading/trailing whitespace (PySpark strips then casts). #272."""
    spark = F.SparkSession.builder().app_name("test_272").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    df = create_df(
        [{"val": "  10.6  "}, {"val": "\t20.7"}],
        [("val", "str")],
    )
    df = df.with_column("rounded", F.round(F.col("val")))
    out = df.collect()
    assert len(out) == 2
    assert out[0]["rounded"] == 11.0
    assert out[1]["rounded"] == 21.0

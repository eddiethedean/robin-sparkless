"""Issue #1578: to_timestamp on strings with fractional seconds (default format)."""

from __future__ import annotations

from sparkless.testing import get_imports

_imports = get_imports()
SparkSession = _imports.SparkSession
F = _imports.F


def test_to_timestamp_default_format_fractional_seconds() -> None:
    spark = SparkSession.builder.appName("issue_1578").get_or_create()
    row = (
        spark.range(1)
        .select(F.to_timestamp(F.lit("2026-06-04 12:34:56.78")).alias("ts"))
        .collect()[0]
    )
    assert row["ts"] is not None
    assert "2026-06-04" in str(row["ts"])
    assert "12:34:56" in str(row["ts"])


def test_create_dataframe_after_fractional_to_timestamp() -> None:
    """Null timestamp from failed parse blocked schema inference; must parse successfully."""
    spark = SparkSession.builder.appName("issue_1578_cascade").get_or_create()
    result = (
        spark.range(1)
        .select(F.to_timestamp(F.lit("2026-06-04 12:34:56.78")).alias("ts"))
        .collect()[0]["ts"]
    )
    assert result is not None
    df = spark.createDataFrame([(result, "hello")], ["col_ts", "col_str"])
    rows = df.collect()
    assert len(rows) == 1
    assert rows[0]["col_str"] == "hello"

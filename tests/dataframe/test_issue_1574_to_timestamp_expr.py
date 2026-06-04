"""Issue #1574: to_timestamp(F.expr(...)) with regexp_replace/substring in expr()."""

from __future__ import annotations

from sparkless.testing import get_imports

_imports = get_imports()
SparkSession = _imports.SparkSession
F = _imports.F


def test_to_timestamp_expr_regexp_replace_substring() -> None:
    spark = SparkSession.builder.appName("issue_1574").get_or_create()
    df = spark.createDataFrame([{"ts": "2024-01-15 10:00:00"}])
    sql = "regexp_replace(ts, substring(ts, 1, 4), '1900')"
    out = df.withColumn("ts", F.to_timestamp(F.expr(sql)))
    row = out.collect()[0]
    assert row["ts"] is not None
    assert "1900-01-15" in str(row["ts"])

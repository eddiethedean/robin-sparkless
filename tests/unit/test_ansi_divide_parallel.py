"""H-8: ANSI divide-by-zero must be consistent when Polars runs on worker threads."""

from __future__ import annotations

import pytest

from sparkless.testing import get_imports

_imports = get_imports()
F = _imports.F


@pytest.fixture
def ansi_spark(monkeypatch):
    monkeypatch.setenv("POLARS_MAX_THREADS", "4")
    spark = _imports.SparkSession.builder.appName("ansi-parallel").getOrCreate()
    prev_ansi = spark.conf.get("spark.sql.ansi.enabled", "false")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try:
        yield spark
    finally:
        spark.conf.set("spark.sql.ansi.enabled", prev_ansi)


def test_divide_by_zero_errors_under_parallel_polars(ansi_spark) -> None:
    df = ansi_spark.createDataFrame([(1, 0), (2, 1)], ["num", "den"])
    with pytest.raises(
        Exception, match="DIVIDE_BY_ZERO|divide by zero|division by zero"
    ):
        df.select((F.col("num") / F.col("den")).alias("ratio")).collect()

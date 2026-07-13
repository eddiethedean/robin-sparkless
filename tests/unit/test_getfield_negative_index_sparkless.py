"""getField(-1) documented Sparkless divergence from PySpark.

PySpark returns null for negative array indices; Sparkless uses Polars
negative indexing (last element). See docs/PYSPARK_DIFFERENCES.md.
"""

from __future__ import annotations

import pytest

from sparkless.testing import get_imports

pytestmark = pytest.mark.sparkless_only

_imports = get_imports()
SparkSession = _imports.SparkSession
F = _imports.F


def test_getfield_negative_index_returns_last_element() -> None:
    """Sparkless/Polars: getField(-1) returns the last array element."""
    spark = SparkSession.builder.appName("getfield-negative").getOrCreate()
    try:
        df = spark.createDataFrame([{"arr": [10, 20, 30]}, {"arr": [40, 50, 60, 70]}])
        df = df.withColumn("last", F.col("arr").getField(-1))
        rows = df.collect()
        assert rows[0]["last"] == 30
        assert rows[1]["last"] == 70
    finally:
        spark.stop()

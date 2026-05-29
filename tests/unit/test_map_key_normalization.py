"""Map key -0.0 normalization (PySpark 4.0 compat profile)."""

from __future__ import annotations

import pytest

pytest_plugins = ["sparkless.testing"]


@pytest.mark.sparkless_only
def test_create_map_collapses_normalized_duplicate_float_keys(spark, spark_imports):
    """On compat=4.0, -0.0 and 0.0 keys normalize to one entry (last value wins)."""
    F = spark_imports.F

    spark.conf.set("sparkless.pyspark.compat", "4.0")
    df = spark.createDataFrame([(1,)], ["id"])
    out = df.select(
        F.create_map(
            F.lit(-0.0),
            F.lit("first"),
            F.lit(0.0),
            F.lit("second"),
        ).alias("m")
    )
    row = out.collect()[0]
    m = row["m"]
    assert len(m) == 1
    assert list(m.values())[0] == "second"

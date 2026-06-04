"""Regression: float NaN in createDataFrame rows must not become JSON string \"nan\"."""

from __future__ import annotations

import math


def test_create_dataframe_nan_collects_as_null(spark):
    df = spark.createDataFrame([{"x": float("nan")}], schema="x double")
    rows = df.collect()
    assert len(rows) == 1
    val = rows[0]["x"]
    assert val is None or (isinstance(val, float) and math.isnan(val))
    assert val != "nan"

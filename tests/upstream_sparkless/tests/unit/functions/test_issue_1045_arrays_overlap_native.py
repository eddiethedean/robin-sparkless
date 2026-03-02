"""
Tests for issue #1045: arrays_overlap native API.

The original bug was that ``sparkless._native`` did not expose an
``arrays_overlap`` function, causing ``AttributeError`` and breaking
array-contains-join style flows that rely on this native helper.

These tests verify that:

- the native symbol exists on ``sparkless._native``, and
- ``functions.arrays_overlap`` produces the expected boolean results.
"""

from __future__ import annotations

import sparkless._native as _native
from sparkless import functions as F
from sparkless.sql import SparkSession


def test_arrays_overlap_native_attribute_exists() -> None:
    """sparkless._native exposes arrays_overlap (no AttributeError)."""
    assert hasattr(_native, "arrays_overlap")


def test_arrays_overlap_native_basic_behavior() -> None:
    """arrays_overlap returns True when arrays share an element, else False."""
    spark = SparkSession.builder.appName("issue-1045-arrays-overlap").getOrCreate()
    try:
        df = spark.createDataFrame(
            [
                {"arr1": [1, 2], "arr2": [2, 3]},  # overlap on 2
                {"arr1": [1], "arr2": [3]},  # no overlap
            ]
        )

        result = df.select(F.arrays_overlap("arr1", "arr2").alias("ov")).collect()
        values = [row["ov"] for row in result]

        assert values == [True, False]
    finally:
        spark.stop()


"""
Tests for issue #1045: arrays_overlap native API. Uses get_imports from fixture only.
"""

from __future__ import annotations

from sparkless.testing import get_imports

_imports = get_imports()
F = _imports.F


def test_arrays_overlap_native_attribute_exists() -> None:
    """When using mock backend, _native exposes arrays_overlap (no AttributeError)."""
    native = getattr(_imports, "_native", None)
    if native is not None:
        assert hasattr(native, "arrays_overlap")


def test_arrays_overlap_native_basic_behavior(spark) -> None:
    """arrays_overlap returns True when arrays share an element, else False."""
    df = spark.createDataFrame(
        [
            {"arr1": [1, 2], "arr2": [2, 3]},  # overlap on 2
            {"arr1": [1], "arr2": [3]},  # no overlap
        ]
    )

    result = df.select(F.arrays_overlap("arr1", "arr2").alias("ov")).collect()
    values = [row["ov"] for row in result]

    assert values == [True, False]

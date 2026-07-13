"""H-2/H-3: resource limits for range() and cross join."""

from __future__ import annotations

import pytest

from sparkless.testing import get_imports

_imports = get_imports()
SparkSession = _imports.SparkSession
F = _imports.F


def test_range_rejects_huge_span(monkeypatch) -> None:
    monkeypatch.setenv("SPARKLESS_MAX_RANGE_ROWS", "1000")
    spark = SparkSession.builder.appName("range-limit").getOrCreate()
    try:
        with pytest.raises((RuntimeError, ValueError), match="exceeding limit"):
            spark.range(0, 10_000).collect()
    finally:
        spark.stop()


def test_cross_join_guard(monkeypatch) -> None:
    monkeypatch.setenv("SPARKLESS_MAX_CROSS_JOIN_ROWS", "10000")
    spark = SparkSession.builder.appName("cross-guard").getOrCreate()
    try:
        left = spark.range(0, 200)
        right = spark.range(0, 200)
        with pytest.raises((RuntimeError, ValueError), match="materialize"):
            left.join(right, left.id > right.id, how="inner").collect()
    finally:
        spark.stop()

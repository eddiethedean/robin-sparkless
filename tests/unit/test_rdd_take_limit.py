"""H-4: RDD take() uses limit without full collect."""

from __future__ import annotations

import pytest

from sparkless.testing import get_imports

_imports = get_imports()


def test_rdd_take_does_not_require_full_collect(spark) -> None:
    df = spark.range(0, 1000)
    taken = df.rdd.take(3)
    assert len(taken) == 3


def test_rdd_map_rejects_oversized_source(monkeypatch, spark) -> None:
    monkeypatch.setenv("SPARKLESS_RDD_MAX_ROWS", "50")
    df = spark.range(0, 100)
    with pytest.raises(Exception, match="RDD operations are limited"):
        df.rdd.map(lambda x: x).collect()

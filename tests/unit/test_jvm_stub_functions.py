"""JVM/runtime stub functions — documented divergences from PySpark.

See docs/PYSPARK_DIFFERENCES.md § JVM / runtime stubs.
"""

from __future__ import annotations

import pytest

from sparkless.testing import get_imports

pytestmark = pytest.mark.sparkless_only


def test_monotonically_increasing_id_stub_is_constant_zero(spark) -> None:
    """Documented stub: constant 0 for all rows (not strictly increasing)."""
    F = get_imports().F
    df = spark.createDataFrame([{"id": 1}, {"id": 2}, {"id": 3}])
    rows = df.select(F.monotonically_increasing_id().alias("mid")).collect()
    assert len(rows) == 3
    assert all(r["mid"] == 0 for r in rows)


def test_spark_partition_id_stub_is_constant_zero(spark) -> None:
    """Documented stub: spark_partition_id() returns 0 for all rows."""
    F = get_imports().F
    df = spark.createDataFrame([{"a": 1}, {"a": 2}])
    rows = df.select(F.spark_partition_id().alias("pid")).collect()
    assert all(r["pid"] == 0 for r in rows)

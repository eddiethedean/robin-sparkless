"""Regression test for issue #1400: numeric.pmod parity.

Scenario (from the issue, paraphrased):

    df = session.createDataFrame([(-5, 3), (5, 3), (None, 3)], ["a", "b"])
    df.select(F.pmod("a", "b").alias("out"))

Previously, Sparkless raised:

    AttributeError: module 'sparkless.sql.functions' has no attribute 'pmod'

This test locks in three expectations:
- F.pmod is available in sparkless.sql.functions.
- Value semantics match PySpark pmod:
  - pmod(-5, 3) = 1
  - pmod(5, 3) = 2
  - pmod(None, 3) = None
"""

from __future__ import annotations

import pytest

from sparkless.sql import functions as F


@pytest.mark.sparkless_only
def test_issue_1400_numeric_pmod_values(spark) -> None:
    df = spark.createDataFrame(
        [(-5, 3), (5, 3), (None, 3)],
        ["a", "b"],
    )
    out = df.select(F.pmod("a", "b").alias("out"))

    rows = [r["out"] for r in out.collect()]
    assert rows == [1, 2, None]

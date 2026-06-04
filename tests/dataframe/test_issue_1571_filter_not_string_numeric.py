"""
Issue #1571: filter(~(F.col("A") == 1)) on a string column must not raise
'cannot compare string with numeric type' — inner comparison coercion must
apply under logical NOT (~), which wraps the predicate in a map UDF.
"""

from __future__ import annotations

from sparkless.testing import get_imports

_imports = get_imports()
F = _imports.F


def test_filter_not_string_eq_numeric_literal(spark) -> None:
    df = spark.createDataFrame([{"A": "1"}])
    out = df.filter(~(F.col("A") == 1)).collect()
    assert out == []

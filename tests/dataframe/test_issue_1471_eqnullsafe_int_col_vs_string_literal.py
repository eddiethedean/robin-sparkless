"""
Tests for issue #1471: Column.eqNullSafe with integer column vs string literal.

Sparkless previously raised a type error when comparing an integer column with
string literal "123" via eqNullSafe, whereas PySpark coerces types and
performs the comparison successfully.
"""

from __future__ import annotations

from sparkless.testing import get_imports


_imports = get_imports()
F = _imports.F


def test_eqnullsafe_int_column_vs_string_literal(spark) -> None:
    """eqNullSafe on int column vs string literal matches PySpark behavior."""
    df = spark.createDataFrame(
        [
            {"val": 123},
            {"val": 456},
            {"val": None},
        ]
    )

    out = df.withColumn("match", F.col("val").eqNullSafe("123")).collect()

    # PySpark behavior:
    # +----+-----+
    # | val|match|
    # +----+-----+
    # | 123| true|
    # | 456|false|
    # |NULL|false|
    # +----+-----+
    assert [row["match"] for row in out] == [True, False, False]

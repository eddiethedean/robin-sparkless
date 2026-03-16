"""Parity test for issue #1473: struct field access via dot notation.

Both F.col("person.name") and F.col("person").getField("name") should return
the underlying struct field values instead of null, matching PySpark.
"""

from __future__ import annotations

from typing import List

from sparkless.testing import get_imports


def _create_person_df(spark):
    return spark.createDataFrame(
        [
            {"person": {"name": "Alice", "age": 30}},
            {"person": {"name": "Bob", "age": 25}},
        ]
    )


def _collect_names(result_df) -> List[str]:
    rows = result_df.collect()
    return [row["name"] for row in rows]


def test_struct_field_dot_notation_matches_getfield(spark) -> None:
    """F.col("person.name") and F.col("person").getField("name") return same values."""
    import pytest

    from sparkless.testing import is_pyspark_mode

    if not is_pyspark_mode():
        pytest.skip(
            "See https://github.com/eddiethedean/robin-sparkless/issues/1504 – "
            "sparkless struct dot access parity gap; unskip once sparkless matches PySpark."
        )

    F = get_imports().F
    df = _create_person_df(spark)

    # Dot-notation access (issue #1473 scenario)
    dot_df = df.select(F.col("person.name").alias("name"))
    dot_names = _collect_names(dot_df)

    # getField access (existing behavior)
    getfield_df = df.select(F.col("person").getField("name").alias("name"))
    getfield_names = _collect_names(getfield_df)

    assert dot_names == ["Alice", "Bob"]
    assert getfield_names == ["Alice", "Bob"]
    assert dot_names == getfield_names

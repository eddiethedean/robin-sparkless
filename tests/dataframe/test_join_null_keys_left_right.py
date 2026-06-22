"""H-5: left join with null keys must preserve left rows."""

from __future__ import annotations

from sparkless.testing import get_imports

_imports = get_imports()
F = _imports.F


def test_left_join_null_key_preserved(spark) -> None:
    left = spark.createDataFrame(
        [(1, "a"), (None, "b")],
        schema="key bigint, label string",
    )
    right = spark.createDataFrame(
        [(1, 10), (2, 20)],
        schema="key bigint, val bigint",
    )
    joined = left.join(right, left.key == right.key, how="left")
    rows = {r["label"]: r["val"] for r in joined.collect()}
    assert rows["a"] == 10
    assert rows["b"] is None

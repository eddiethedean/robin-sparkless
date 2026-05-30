"""Regression: infer array<int> when rows mix empty and typed arrays (createDataFrame)."""

from __future__ import annotations

import pytest
from sparkless.testing import get_imports

_imports = get_imports()
ArrayType = _imports.ArrayType
IntegerType = _imports.IntegerType
LongType = _imports.LongType


@pytest.fixture
def spark():
    imports = get_imports()
    s = imports.SparkSession.builder.appName("array_schema_inference").get_or_create()
    yield s
    s.stop()


def test_empty_and_int_array_infers_long_element_type(spark):
    F = get_imports().F
    df = spark.createDataFrame([{"id": 1, "arr": []}, {"id": 2, "arr": [10, 20]}])
    field = next(f for f in df.schema.fields if f.name == "arr")
    assert isinstance(field.dataType, ArrayType)
    assert isinstance(field.dataType.elementType, (IntegerType, LongType))
    rows = df.select("id", F.posexplode("arr").alias("pos", "val")).collect()
    assert len(rows) == 2
    assert all(isinstance(r["val"], int) for r in rows)
    assert sorted((r["pos"], r["val"]) for r in rows) == [(0, 10), (1, 20)]


def test_names_only_schema_infers_int_array_for_values(spark):
    F = get_imports().F
    data = [
        {"Name": "Alice", "Values": [10, 20]},
        {"Name": "Bob", "Values": [30, 40]},
    ]
    df = spark.createDataFrame(data, ["Name", "Values"])
    field = next(f for f in df.schema.fields if f.name == "Values")
    assert isinstance(field.dataType, ArrayType)
    assert isinstance(field.dataType.elementType, (IntegerType, LongType))
    out = df.select(F.posexplode("Values").alias("pos", "val")).collect()
    assert [r["val"] for r in out] == [10, 20, 30, 40]


def test_array_contains_join_empty_and_nonempty_ids(spark):
    F = get_imports().F
    df1 = spark.createDataFrame(
        [
            {"Name": "Alice", "IDs": []},
            {"Name": "Bob", "IDs": [1, 2, 3]},
        ]
    )
    df2 = spark.createDataFrame([{"Dept": "A", "ID": 1}])
    result = df1.join(df2, on=F.array_contains(df1.IDs, df2.ID), how="left")
    rows = {r["Name"]: r for r in result.collect()}
    assert rows["Alice"]["Dept"] is None
    assert rows["Bob"]["Dept"] == "A"
    assert rows["Bob"]["ID"] == 1

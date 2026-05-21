"""Tests for issue #1548: StructType __iter__ and __len__ (PySpark parity)."""

from __future__ import annotations


def test_dataframe_schema_is_iterable(spark) -> None:
    """list(df.schema) and for f in df.schema work like PySpark."""
    df = spark.createDataFrame([("Alice", 1)], ["name", "age"])
    schema = df.schema
    fields = list(schema)
    assert len(fields) == 2
    assert len(schema) == 2
    assert [f.name for f in fields] == ["name", "age"]
    assert [f.name for f in schema] == ["name", "age"]


def test_struct_type_iter_and_len_direct() -> None:
    """StructType constructed directly supports iteration and len."""
    from sparkless.testing import get_imports

    T = get_imports()
    schema = T.StructType(
        [
            T.StructField("name", T.StringType(), True),
            T.StructField("age", T.IntegerType(), True),
        ]
    )
    assert len(schema) == 2
    assert [f.name for f in schema] == ["name", "age"]

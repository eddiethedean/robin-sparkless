"""
Tests for issue #1590: unionByName with ArrayType vs lit(None).

PySpark treats untyped null as compatible with the other side's array type.
"""

from __future__ import annotations

from sparkless.testing import get_imports

F = get_imports().F
ArrayType = get_imports().ArrayType
StringType = get_imports().StringType
StructField = get_imports().StructField
StructType = get_imports().StructType


def test_union_by_name_array_vs_lit_none(spark) -> None:
    """Exact scenario from issue #1590."""
    schema_with_array = StructType(
        [
            StructField("A", StringType()),
            StructField("B", ArrayType(StringType())),
        ]
    )
    df1 = spark.createDataFrame([("A", ["x", "y"])], schema_with_array)
    df2 = spark.createDataFrame([("B",)], ["A"]).withColumn("B", F.lit(None))

    rows = df1.unionByName(df2).collect()
    assert len(rows) == 2
    by_a = {r["A"]: r["B"] for r in rows}
    assert by_a["A"] == ["x", "y"]
    assert by_a["B"] is None


def test_union_array_vs_lit_none(spark) -> None:
    """Regular union() should also accept array vs untyped null."""
    schema_with_array = StructType(
        [
            StructField("A", StringType()),
            StructField("B", ArrayType(StringType())),
        ]
    )
    df1 = spark.createDataFrame([("A", ["x"])], schema_with_array)
    df2 = spark.createDataFrame([("B",)], ["A"]).withColumn("B", F.lit(None))

    rows = df1.union(df2).collect()
    assert len(rows) == 2
    by_a = {r["A"]: r["B"] for r in rows}
    assert by_a["A"] == ["x"]
    assert by_a["B"] is None

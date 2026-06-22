"""
Tests for issue #1594: cast ArrayType column to StringType.

PySpark stringifies arrays as `[elem1, elem2]`; sparkless previously raised
"cannot cast List type".
"""

from __future__ import annotations

from sparkless.testing import get_imports

F = get_imports().F
ArrayType = get_imports().ArrayType
IntegerType = get_imports().IntegerType
StringType = get_imports().StringType
StructField = get_imports().StructField
StructType = get_imports().StructType


def test_array_string_cast_to_string(spark) -> None:
    """Exact scenario from issue #1594."""
    df = spark.createDataFrame(
        [("A", ["x", "y"]), ("B", ["z"])],
        StructType(
            [
                StructField("COL1", StringType()),
                StructField("COL2", ArrayType(StringType())),
            ]
        ),
    )
    result = df.withColumn("COL2_str", F.col("COL2").cast("string"))
    rows = {r["COL1"]: r["COL2_str"] for r in result.collect()}
    assert rows["A"] == "[x, y]"
    assert rows["B"] == "[z]"


def test_array_int_cast_to_string(spark) -> None:
    """Numeric array elements stringify without extra quotes."""
    df = spark.createDataFrame(
        [(1, [1, 2]), (2, [])],
        StructType(
            [
                StructField("id", IntegerType()),
                StructField("nums", ArrayType(IntegerType())),
            ]
        ),
    )
    result = df.withColumn("nums_str", F.col("nums").cast("string"))
    rows = {r["id"]: r["nums_str"] for r in result.collect()}
    assert rows[1] == "[1, 2]"
    assert rows[2] == "[]"


def test_array_null_cast_to_string(spark) -> None:
    """Null array values stay null when cast to string."""
    df = spark.createDataFrame(
        [(None,)],
        StructType([StructField("arr", ArrayType(StringType()))]),
    )
    row = df.withColumn("arr_str", F.col("arr").cast("string")).collect()[0]
    assert row["arr_str"] is None

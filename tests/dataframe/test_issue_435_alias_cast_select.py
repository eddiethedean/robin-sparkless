"""Test issue #435: alias().cast() in select.

F.col("ValueOld").alias("ValueNew").cast(IntegerType()) raised SparkColumnNotFoundError:
cannot resolve 'ValueNew' given input columns: [Name, ValueOld].
PySpark supports this combination - alias is output name, validation should use original column.

https://github.com/eddiethedean/sparkless/issues/435
"""

from tests.fixtures.spark_imports import get_spark_imports


def test_alias_cast_select_exact_issue_435(spark, spark_backend):
    """Exact scenario from issue #435 - alias().cast() in select."""
    imports = get_spark_imports(spark_backend)
    F = imports.F
    T = imports.IntegerType
    df = spark.createDataFrame(
        [
            {"Name": "Alice", "ValueOld": "123"},
            {"Name": "Bob", "ValueOld": "456"},
        ]
    )
    df = df.select(
        F.col("Name"),
        F.col("ValueOld").alias("ValueNew").cast(T()),
    )
    df.show()
    rows = df.collect()
    assert len(rows) == 2
    assert rows[0]["Name"] == "Alice" and rows[0]["ValueNew"] == 123
    assert rows[1]["Name"] == "Bob" and rows[1]["ValueNew"] == 456


def test_alias_without_cast_still_works(spark, spark_backend):
    """alias() without cast - regression check."""
    F = get_spark_imports(spark_backend).F
    df = spark.createDataFrame([{"a": 1, "b": 2}])
    result = df.select(
        F.col("a"),
        F.col("b").alias("B_renamed"),
    )
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["B_renamed"] == 2


def test_cast_without_alias_still_works(spark, spark_backend):
    """cast() without alias - regression check."""
    imports = get_spark_imports(spark_backend)
    F = imports.F
    T = imports.IntegerType
    df = spark.createDataFrame([{"s": "123"}])
    result = df.select(F.col("s").cast(T()))
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["s"] == 123


def test_alias_cast_string_type(spark, spark_backend):
    """alias().cast(StringType()) in select."""
    imports = get_spark_imports(spark_backend)
    F = imports.F
    T = imports.StringType
    df = spark.createDataFrame([{"num": 123}])
    result = df.select(F.col("num").alias("num_str").cast(T()))
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["num_str"] == "123"


def test_alias_cast_multiple_columns(spark, spark_backend):
    """Multiple alias().cast() in select."""
    imports = get_spark_imports(spark_backend)
    F = imports.F
    T = imports.IntegerType
    df = spark.createDataFrame([{"a": "1", "b": "2", "c": "3"}])
    result = df.select(
        F.col("a").alias("A_int").cast(T()),
        F.col("b").alias("B_int").cast(T()),
        F.col("c").alias("C_int").cast(T()),
    )
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["A_int"] == 1 and rows[0]["B_int"] == 2 and rows[0]["C_int"] == 3


# --- Robust edge-case tests ---


def test_alias_cast_double_type(spark, spark_backend):
    """alias().cast(DoubleType()) in select."""
    imports = get_spark_imports(spark_backend)
    F = imports.F
    T = imports.DoubleType
    df = spark.createDataFrame([{"s": "3.14"}])
    result = df.select(F.col("s").alias("dbl").cast(T()))
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["dbl"] == 3.14


def test_alias_cast_with_nulls(spark, spark_backend):
    """alias().cast() with null values in column."""
    imports = get_spark_imports(spark_backend)
    F = imports.F
    T = imports.IntegerType
    df = spark.createDataFrame(
        [
            {"a": "1"},
            {"a": None},
            {"a": "3"},
        ]
    )
    result = df.select(F.col("a").alias("a_int").cast(T()))
    rows = result.collect()
    assert len(rows) == 3
    assert rows[0]["a_int"] == 1
    assert rows[1]["a_int"] is None
    assert rows[2]["a_int"] == 3


def test_alias_cast_then_filter(spark, spark_backend):
    """alias().cast() in select then filter on new column."""
    imports = get_spark_imports(spark_backend)
    F = imports.F
    T = imports.IntegerType
    df = spark.createDataFrame(
        [
            {"name": "a", "val": "1"},
            {"name": "b", "val": "2"},
            {"name": "c", "val": "3"},
        ]
    )
    result = df.select(
        F.col("name"),
        F.col("val").alias("val_int").cast(T()),
    ).filter(F.col("val_int") > 1)
    rows = result.collect()
    assert len(rows) == 2
    names = {r["name"] for r in rows}
    assert names == {"b", "c"}


def test_alias_cast_column_with_underscore(spark, spark_backend):
    """alias().cast() on column with underscore in name."""
    imports = get_spark_imports(spark_backend)
    F = imports.F
    T = imports.IntegerType
    df = spark.createDataFrame([{"my_column": "42"}])
    result = df.select(F.col("my_column").alias("mc_int").cast(T()))
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["mc_int"] == 42


def test_alias_cast_then_select_subset(spark, spark_backend):
    """alias().cast() then select subset of columns."""
    imports = get_spark_imports(spark_backend)
    F = imports.F
    T = imports.IntegerType
    df = spark.createDataFrame([{"a": "1", "b": "2", "c": "3"}])
    result = df.select(
        F.col("a").alias("A").cast(T()),
        F.col("b").alias("B").cast(T()),
        F.col("c").alias("C").cast(T()),
    ).select("A", "C")
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["A"] == 1 and rows[0]["C"] == 3


def test_alias_cast_long_type(spark, spark_backend):
    """alias().cast(LongType()) in select."""
    imports = get_spark_imports(spark_backend)
    F = imports.F
    T = imports.LongType
    df = spark.createDataFrame([{"s": "9999999999"}])
    result = df.select(F.col("s").alias("lng").cast(T()))
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["lng"] == 9999999999


def test_alias_cast_mixed_with_plain_select(spark, spark_backend):
    """alias().cast() mixed with plain column select."""
    imports = get_spark_imports(spark_backend)
    F = imports.F
    T = imports.IntegerType
    df = spark.createDataFrame([{"id": 1, "score": "100", "name": "Alice"}])
    result = df.select(
        F.col("id"),
        F.col("score").alias("score_int").cast(T()),
        F.col("name"),
    )
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["id"] == 1
    assert rows[0]["score_int"] == 100
    assert rows[0]["name"] == "Alice"

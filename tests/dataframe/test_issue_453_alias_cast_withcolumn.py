"""Test issue #453: alias().cast() in withColumn raises SparkColumnNotFoundError.

F.col('y').alias('y_int').cast(IntegerType()) works in select() (fixed in #435) but
raised SparkColumnNotFoundError in withColumn(). ColumnValidator.validate_expression_columns
did not validate _original_column for aliased columns (same fix pattern as #435).

https://github.com/eddiethedean/sparkless/issues/453
"""

from tests.fixtures.spark_imports import get_spark_imports


def _row_val(row, key):
    """Get value from Row or dict."""
    if hasattr(row, "__getitem__"):
        return row[key]
    return getattr(row, key, None)


def test_alias_cast_withcolumn_exact_issue_453(spark, spark_backend):
    """Exact scenario from #453 - alias().cast() in withColumn."""
    imports = get_spark_imports(spark_backend)
    F = imports.F
    T = imports.IntegerType

    df = spark.createDataFrame([{"x": 1, "y": "2"}])

    result = df.withColumn("y_as_int", F.col("y").alias("y_int").cast(T()))
    rows = result.collect()

    assert len(rows) == 1
    assert _row_val(rows[0], "x") == 1
    assert _row_val(rows[0], "y") == "2"
    assert _row_val(rows[0], "y_as_int") == 2


def test_alias_cast_withcolumn_multiple_columns(spark, spark_backend):
    """Multiple alias().cast() in withColumn."""
    imports = get_spark_imports(spark_backend)
    F = imports.F
    T = imports.IntegerType

    df = spark.createDataFrame([{"a": "1", "b": "2"}])
    result = df.withColumn("a_int", F.col("a").alias("a_aliased").cast(T())).withColumn(
        "b_int", F.col("b").alias("b_aliased").cast(T())
    )
    rows = result.collect()

    assert len(rows) == 1
    assert _row_val(rows[0], "a_int") == 1
    assert _row_val(rows[0], "b_int") == 2


def test_alias_cast_select_still_works(spark, spark_backend):
    """alias().cast() in select - regression check (#435)."""
    imports = get_spark_imports(spark_backend)
    F = imports.F
    T = imports.IntegerType

    df = spark.createDataFrame([{"y": "2"}])
    result = df.select(F.col("y").alias("y_int").cast(T()))
    rows = result.collect()

    assert len(rows) == 1
    assert _row_val(rows[0], "y_int") == 2


def test_alias_cast_withcolumn_then_select(spark, spark_backend):
    """withColumn with alias().cast() then select."""
    imports = get_spark_imports(spark_backend)
    F = imports.F
    T = imports.IntegerType

    df = spark.createDataFrame([{"x": 1, "y": "10"}])
    result = df.withColumn("y_int", F.col("y").alias("y_renamed").cast(T())).select(
        "x", "y_int"
    )
    rows = result.collect()

    assert len(rows) == 1
    assert _row_val(rows[0], "x") == 1
    assert _row_val(rows[0], "y_int") == 10


# --- Robust edge-case tests ---


def test_alias_cast_withcolumn_string_type(spark, spark_backend):
    """alias().cast(StringType()) in withColumn."""
    imports = get_spark_imports(spark_backend)
    F = imports.F
    T = imports.StringType

    df = spark.createDataFrame([{"num": 123}])
    result = df.withColumn("num_str", F.col("num").alias("n").cast(T()))
    rows = result.collect()
    assert len(rows) == 1
    assert _row_val(rows[0], "num_str") == "123"


def test_alias_cast_withcolumn_double_type(spark, spark_backend):
    """alias().cast(DoubleType()) in withColumn."""
    imports = get_spark_imports(spark_backend)
    F = imports.F
    T = imports.DoubleType

    df = spark.createDataFrame([{"s": "3.14"}])
    result = df.withColumn("dbl", F.col("s").alias("str_val").cast(T()))
    rows = result.collect()
    assert len(rows) == 1
    assert abs(_row_val(rows[0], "dbl") - 3.14) < 1e-9


def test_alias_cast_withcolumn_long_type(spark, spark_backend):
    """alias().cast(LongType()) in withColumn."""
    imports = get_spark_imports(spark_backend)
    F = imports.F
    T = imports.LongType

    df = spark.createDataFrame([{"s": "9999999999"}])
    result = df.withColumn("lng", F.col("s").alias("l").cast(T()))
    rows = result.collect()
    assert len(rows) == 1
    assert _row_val(rows[0], "lng") == 9999999999


def test_alias_cast_withcolumn_with_nulls(spark, spark_backend):
    """alias().cast() in withColumn with null values."""
    imports = get_spark_imports(spark_backend)
    F = imports.F
    T = imports.IntegerType

    df = spark.createDataFrame([{"a": "1"}, {"a": None}, {"a": "3"}])
    result = df.withColumn("a_int", F.col("a").alias("a_aliased").cast(T()))
    rows = result.collect()
    assert len(rows) == 3
    assert _row_val(rows[0], "a_int") == 1
    assert _row_val(rows[1], "a_int") is None
    assert _row_val(rows[2], "a_int") == 3


def test_alias_cast_withcolumn_then_filter(spark, spark_backend):
    """withColumn alias().cast() then filter on new column."""
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
    result = df.withColumn("val_int", F.col("val").alias("v").cast(T())).filter(
        F.col("val_int") > 1
    )
    rows = result.collect()
    assert len(rows) == 2
    names = {_row_val(r, "name") for r in rows}
    assert names == {"b", "c"}


def test_alias_cast_withcolumn_column_with_underscore(spark, spark_backend):
    """alias().cast() in withColumn on column with underscore."""
    imports = get_spark_imports(spark_backend)
    F = imports.F
    T = imports.IntegerType

    df = spark.createDataFrame([{"my_column": "42"}])
    result = df.withColumn("mc_int", F.col("my_column").alias("mc").cast(T()))
    rows = result.collect()
    assert len(rows) == 1
    assert _row_val(rows[0], "mc_int") == 42


def test_alias_cast_withcolumn_mixed_with_plain(spark, spark_backend):
    """withColumn alias().cast() mixed with plain withColumn."""
    imports = get_spark_imports(spark_backend)
    F = imports.F
    T = imports.IntegerType

    df = spark.createDataFrame([{"id": 1, "score": "100", "name": "Alice"}])
    result = df.withColumn("score_int", F.col("score").alias("s").cast(T())).withColumn(
        "doubled", F.col("id") * 2
    )
    rows = result.collect()
    assert len(rows) == 1
    assert _row_val(rows[0], "id") == 1
    assert _row_val(rows[0], "score_int") == 100
    assert _row_val(rows[0], "name") == "Alice"
    assert _row_val(rows[0], "doubled") == 2


def test_alias_cast_withcolumn_replace_existing_column(spark, spark_backend):
    """withColumn alias().cast() - output name same as different input (replacement)."""
    imports = get_spark_imports(spark_backend)
    F = imports.F
    T = imports.IntegerType

    df = spark.createDataFrame([{"a": "1", "b": "x"}])
    result = df.withColumn("b", F.col("a").alias("a_as_int").cast(T()))
    rows = result.collect()
    assert len(rows) == 1
    assert _row_val(rows[0], "a") == "1"
    assert _row_val(rows[0], "b") == 1


def test_alias_cast_withcolumn_chain_three_ops(spark, spark_backend):
    """Chained withColumn with alias().cast() three times."""
    imports = get_spark_imports(spark_backend)
    F = imports.F
    T = imports.IntegerType

    df = spark.createDataFrame([{"x": "1", "y": "2", "z": "3"}])
    result = (
        df.withColumn("x_int", F.col("x").alias("x_a").cast(T()))
        .withColumn("y_int", F.col("y").alias("y_a").cast(T()))
        .withColumn("z_int", F.col("z").alias("z_a").cast(T()))
    )
    rows = result.collect()
    assert len(rows) == 1
    assert _row_val(rows[0], "x_int") == 1
    assert _row_val(rows[0], "y_int") == 2
    assert _row_val(rows[0], "z_int") == 3


def test_alias_cast_withcolumn_after_filter(spark, spark_backend):
    """withColumn alias().cast() after filter."""
    imports = get_spark_imports(spark_backend)
    F = imports.F
    T = imports.IntegerType

    df = spark.createDataFrame(
        [{"id": 1, "val": "10"}, {"id": 2, "val": "20"}, {"id": 3, "val": "30"}]
    )
    result = df.filter(F.col("id") > 1).withColumn(
        "val_int", F.col("val").alias("v").cast(T())
    )
    rows = result.collect()
    assert len(rows) == 2
    assert _row_val(rows[0], "val_int") == 20
    assert _row_val(rows[1], "val_int") == 30


def test_alias_cast_withcolumn_empty_dataframe(spark, spark_backend):
    """withColumn alias().cast() on empty DataFrame."""
    imports = get_spark_imports(spark_backend)
    F = imports.F
    T = imports.IntegerType

    df = spark.createDataFrame([], "a: string")
    result = df.withColumn("a_int", F.col("a").alias("a_alias").cast(T()))
    rows = result.collect()
    assert len(rows) == 0
    assert "a_int" in result.columns

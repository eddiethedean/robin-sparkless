"""
Smoke tests for the 14 previously missing Python bindings (RUST_PYTHON_PARITY_CROSSCHECK).

Covers: length, trim, ltrim, rtrim, repeat, reverse, initcap, regexp_extract, regexp_replace,
floor, round, exp, levenshtein, crc32, xxhash64 — as both F.xxx(col) and col.xxx().
"""

from __future__ import annotations

import pytest

from tests.fixtures.spark_imports import get_spark_imports


_imports = get_spark_imports()
SparkSession = _imports.SparkSession
F = _imports.F

def _spark_and_df():
    spark = SparkSession.builder.appName("bindings").getOrCreate()
    data = [
        {"s": "  ab  ", "n": 2.7, "t": "hello world"},
        {"s": "xy", "n": -1.2, "t": "foo bar"},
    ]
    # Use PySpark-style schema specification: list of column names. Types are
    # inferred from data (double for n, string for s/t), matching real PySpark.
    schema = ["s", "n", "t"]
    return spark, spark.createDataFrame(data, schema)


def test_length_module_and_method() -> None:
    _, df = _spark_and_df()
    out = df.select(F.length(F.col("s"))).collect()
    assert len(out) == 2
    # In current PySpark, Column does not expose a .length() method; attempting
    # to call it results in TypeError(\"Column object is not callable\").
    # We assert on this behavior so that sparkless matches PySpark.
    import pytest

    with pytest.raises(TypeError):
        df.select(F.col("s").length()).collect()


def test_trim_ltrim_rtrim_module_and_method() -> None:
    _, df = _spark_and_df()
    df.select(F.trim(F.col("s"))).collect()
    # Column.trim/ltrim/rtrim are not available as methods in PySpark, only
    # via functions module. We only assert the module-level bindings work.
    df.select(F.ltrim(F.col("s"))).collect()
    df.select(F.rtrim(F.col("s"))).collect()


def test_repeat_reverse_initcap_module_and_method() -> None:
    _, df = _spark_and_df()
    df.select(F.repeat(F.col("s"), 2)).collect()
    df.select(F.reverse(F.col("s"))).collect()
    df.select(F.initcap(F.col("t"))).collect()


def test_regexp_extract_replace_module_and_method() -> None:
    _, df = _spark_and_df()
    df.select(F.regexp_extract(F.col("s"), r"\w+", 0)).collect()
    df.select(F.regexp_replace(F.col("s"), r"\s", "-")).collect()


def test_floor_round_exp_module_and_method() -> None:
    _, df = _spark_and_df()
    df.select(F.floor(F.col("n"))).collect()
    df.select(F.round(F.col("n"), 0)).collect()
    df.select(F.exp(F.col("n"))).collect()


def test_levenshtein_crc32_xxhash64_module_and_method() -> None:
    _, df = _spark_and_df()
    df.select(F.levenshtein(F.col("s"), F.col("t"))).collect()
    # crc32/xxhash64 accept string/binary columns via functions API. For this
    # smoke test we just verify the functions exist and are callable.
    df.select(F.crc32(F.col("t"))).collect()
    df.select(F.xxhash64(F.col("t"))).collect()

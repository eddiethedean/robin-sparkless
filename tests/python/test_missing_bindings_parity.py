"""
Smoke tests for the 14 previously missing Python bindings (RUST_PYTHON_PARITY_CROSSCHECK).

Covers: length, trim, ltrim, rtrim, repeat, reverse, initcap, regexp_extract, regexp_replace,
floor, round, exp, levenshtein, crc32, xxhash64 — as both F.xxx(col) and col.xxx().
"""

from __future__ import annotations

import pytest

from tests.conftest import is_pyspark_backend
from tests.fixtures.spark_imports import get_spark_imports


_imports = get_spark_imports()
SparkSession = _imports.SparkSession
F = _imports.F

pytestmark = pytest.mark.skipif(
    is_pyspark_backend(),
    reason="binding parity checks rely on sparkless-specific Column methods",
)


def _spark_and_df():
    spark = SparkSession.builder.appName("bindings").getOrCreate()
    data = [
        {"s": "  ab  ", "n": 2.7, "t": "hello world"},
        {"s": "xy", "n": -1.2, "t": "foo bar"},
    ]
    schema = [("s", "string"), ("n", "double"), ("t", "string")]
    return spark, spark.createDataFrame(data, schema)


def test_length_module_and_method() -> None:
    _, df = _spark_and_df()
    out = df.select(F.length(F.col("s"))).collect()
    assert len(out) == 2
    out2 = df.select(F.col("s").length()).collect()
    assert out == out2


def test_trim_ltrim_rtrim_module_and_method() -> None:
    _, df = _spark_and_df()
    df.select(F.trim(F.col("s"))).collect()
    df.select(F.col("s").trim()).collect()
    df.select(F.ltrim(F.col("s"))).collect()
    df.select(F.col("s").ltrim()).collect()
    df.select(F.rtrim(F.col("s"))).collect()
    df.select(F.col("s").rtrim()).collect()


def test_repeat_reverse_initcap_module_and_method() -> None:
    _, df = _spark_and_df()
    df.select(F.repeat(F.col("s"), 2)).collect()
    df.select(F.col("s").repeat(2)).collect()
    df.select(F.reverse(F.col("s"))).collect()
    df.select(F.col("s").reverse()).collect()
    df.select(F.initcap(F.col("t"))).collect()
    df.select(F.col("t").initcap()).collect()


def test_regexp_extract_replace_module_and_method() -> None:
    _, df = _spark_and_df()
    df.select(F.regexp_extract(F.col("s"), r"\w+", 0)).collect()
    df.select(F.col("s").regexp_extract(r"\w+", 0)).collect()
    df.select(F.regexp_replace(F.col("s"), r"\s", "-")).collect()
    df.select(F.col("s").regexp_replace(r"\s", "-")).collect()


def test_floor_round_exp_module_and_method() -> None:
    _, df = _spark_and_df()
    df.select(F.floor(F.col("n"))).collect()
    df.select(F.col("n").floor()).collect()
    df.select(F.round(F.col("n"), 0)).collect()
    df.select(F.col("n").round(0)).collect()
    df.select(F.exp(F.col("n"))).collect()
    df.select(F.col("n").exp()).collect()


def test_levenshtein_crc32_xxhash64_module_and_method() -> None:
    _, df = _spark_and_df()
    df.select(F.levenshtein(F.col("s"), F.col("t"))).collect()
    df.select(F.col("s").levenshtein(F.col("t"))).collect()
    df.select(F.crc32(F.col("s"))).collect()
    df.select(F.col("s").crc32()).collect()
    df.select(F.xxhash64(F.col("s"))).collect()
    df.select(F.col("s").xxhash64()).collect()

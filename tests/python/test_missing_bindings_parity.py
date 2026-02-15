"""
Smoke tests for the 14 previously missing Python bindings (RUST_PYTHON_PARITY_CROSSCHECK).

Covers: length, trim, ltrim, rtrim, repeat, reverse, initcap, regexp_extract, regexp_replace,
floor, round, exp, levenshtein, crc32, xxhash64 — as both F.xxx(col) and col.xxx().
"""

from __future__ import annotations

import robin_sparkless as rs


def _spark_and_df():
    spark = rs.SparkSession.builder().app_name("bindings").get_or_create()
    data = [
        {"s": "  ab  ", "n": 2.7, "t": "hello world"},
        {"s": "xy", "n": -1.2, "t": "foo bar"},
    ]
    schema = [("s", "string"), ("n", "double"), ("t", "string")]
    return spark, spark.createDataFrame(data, schema)


def test_length_module_and_method() -> None:
    _, df = _spark_and_df()
    out = df.select(rs.length(rs.col("s"))).collect()
    assert len(out) == 2
    out2 = df.select(rs.col("s").length()).collect()
    assert out == out2


def test_trim_ltrim_rtrim_module_and_method() -> None:
    _, df = _spark_and_df()
    df.select(rs.trim(rs.col("s"))).collect()
    df.select(rs.col("s").trim()).collect()
    df.select(rs.ltrim(rs.col("s"))).collect()
    df.select(rs.col("s").ltrim()).collect()
    df.select(rs.rtrim(rs.col("s"))).collect()
    df.select(rs.col("s").rtrim()).collect()


def test_repeat_reverse_initcap_module_and_method() -> None:
    _, df = _spark_and_df()
    df.select(rs.repeat(rs.col("s"), 2)).collect()
    df.select(rs.col("s").repeat(2)).collect()
    df.select(rs.reverse(rs.col("s"))).collect()
    df.select(rs.col("s").reverse()).collect()
    df.select(rs.initcap(rs.col("t"))).collect()
    df.select(rs.col("t").initcap()).collect()


def test_regexp_extract_replace_module_and_method() -> None:
    _, df = _spark_and_df()
    df.select(rs.regexp_extract(rs.col("s"), r"\w+", 0)).collect()
    df.select(rs.col("s").regexp_extract(r"\w+", 0)).collect()
    df.select(rs.regexp_replace(rs.col("s"), r"\s", "-")).collect()
    df.select(rs.col("s").regexp_replace(r"\s", "-")).collect()


def test_floor_round_exp_module_and_method() -> None:
    _, df = _spark_and_df()
    df.select(rs.floor(rs.col("n"))).collect()
    df.select(rs.col("n").floor()).collect()
    df.select(rs.round(rs.col("n"), 0)).collect()
    df.select(rs.col("n").round(0)).collect()
    df.select(rs.exp(rs.col("n"))).collect()
    df.select(rs.col("n").exp()).collect()


def test_levenshtein_crc32_xxhash64_module_and_method() -> None:
    _, df = _spark_and_df()
    df.select(rs.levenshtein(rs.col("s"), rs.col("t"))).collect()
    df.select(rs.col("s").levenshtein(rs.col("t"))).collect()
    df.select(rs.crc32(rs.col("s"))).collect()
    df.select(rs.col("s").crc32()).collect()
    df.select(rs.xxhash64(rs.col("s"))).collect()
    df.select(rs.col("s").xxhash64()).collect()

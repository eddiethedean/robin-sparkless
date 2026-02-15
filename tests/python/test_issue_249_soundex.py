"""
Tests for issue #249: soundex() string function missing.

PySpark provides F.soundex(col) for phonetic encoding. Robin now exposes soundex
so parity tests and with_column(snd, F.soundex(F.col("name"))) work.
"""

from __future__ import annotations

import robin_sparkless as rs


def test_soundex_module_exists() -> None:
    """Module exposes soundex function."""
    assert hasattr(rs, "soundex")


def test_with_column_soundex_returns_three_rows() -> None:
    """df.with_column("snd", F.soundex(F.col("name"))) returns 3 rows with snd column."""
    spark = rs.SparkSession.builder().app_name("soundex").get_or_create()
    data = [{"name": "Alice"}, {"name": "Bob"}, {"name": "Robert"}]
    schema = [("name", "string")]
    df = spark.createDataFrame(data, schema)
    out = df.with_column("snd", rs.soundex(rs.col("name"))).collect()
    assert len(out) == 3
    for row in out:
        assert "name" in row
        assert "snd" in row
        assert isinstance(row["snd"], str)
        assert len(row["snd"]) == 4  # American Soundex is 4 chars


def test_soundex_phonetic_codes() -> None:
    """Soundex produces expected phonetic codes (Alice->A420, Robert->R163)."""
    spark = rs.SparkSession.builder().app_name("soundex").get_or_create()
    data = [{"name": "Alice"}, {"name": "Robert"}]
    schema = [("name", "string")]
    df = spark.createDataFrame(data, schema)
    out = df.with_column("snd", rs.soundex(rs.col("name"))).collect()
    names_to_snd = {r["name"]: r["snd"] for r in out}
    assert names_to_snd["Alice"] == "A420"
    assert names_to_snd["Robert"] == "R163"

import pytest

from sparkless.testing import get_imports

_imports = get_imports()
F = _imports.F


def test_regexp_extract_all_basic_groups(spark):
    df = spark.createDataFrame(
        [
            {"s": "a1 b22 c333"},
            {"s": "no-digits"},
            {"s": None},
        ]
    )

    # idx=0 returns whole matches; PySpark expects pattern as literal (F.lit)
    out0 = df.select(
        F.regexp_extract_all(F.col("s"), F.lit(r"\d+"), 0).alias("m")
    ).collect()
    assert out0[0]["m"] == ["1", "22", "333"]
    assert out0[1]["m"] == []
    assert out0[2]["m"] is None

    # idx=1 with capturing group
    out1 = df.select(
        F.regexp_extract_all(F.col("s"), F.lit(r"(\d+)"), 1).alias("g")
    ).collect()
    assert out1[0]["g"] == ["1", "22", "333"]
    assert out1[1]["g"] == []
    assert out1[2]["g"] is None


def test_regexp_extract_all_raw_string_pattern_errors_in_both_modes(spark) -> None:
    """Raw string pattern should raise UNRESOLVED_COLUMN-style error in both modes (#1501)."""
    df = spark.createDataFrame(
        [
            {"s": "a1 b22 c333"},
            {"s": "no-digits"},
            {"s": None},
        ]
    )

    with pytest.raises(Exception) as excinfo:
        df.select(
            F.regexp_extract_all(F.col("s"), r"\d+", 0).alias("m")
        ).collect()

    msg = str(excinfo.value)
    assert "UNRESOLVED_COLUMN" in msg
    assert "`\\d+` cannot be resolved" in msg

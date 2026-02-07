"""
Shared helpers for robin_sparkless Python tests.

Use assert_rows_equal() to compare collect() output to PySpark-derived expected
(list of dicts). Use get_session() for a SparkSession when not using the pytest
fixture from conftest.py.
"""

from __future__ import annotations


def assert_rows_equal(
    actual: list[dict],
    expected: list[dict],
    order_matters: bool = True,
) -> None:
    """Compare two lists of row dicts (e.g. from df.collect()).

    Args:
        actual: Result from robin_sparkless DataFrame.collect().
        expected: Expected rows (e.g. from PySpark or precomputed).
        order_matters: If False, sort both by a canonical key before comparing
            (e.g. for groupBy/join where order may differ).

    Raises:
        AssertionError: If lengths differ or any row differs.
    """
    if len(actual) != len(expected):
        raise AssertionError(
            f"Row count mismatch: got {len(actual)}, expected {len(expected)}"
        )
    if order_matters:
        for i, (a, e) in enumerate(zip(actual, expected)):
            _assert_row_equal(a, e, index=i)
    else:
        # Sort by stringified row for stable comparison
        def key_fn(r: dict) -> str:
            return str(sorted((k, _norm_val(v)) for k, v in r.items()))

        actual_sorted = sorted(actual, key=key_fn)
        expected_sorted = sorted(expected, key=key_fn)
        for i, (a, e) in enumerate(zip(actual_sorted, expected_sorted)):
            _assert_row_equal(a, e, index=i)


def _norm_val(v: object) -> object:
    """Normalize value for comparison (e.g. int/float)."""
    if isinstance(v, float) and not (v != v):  # not NaN
        return round(v, 10)
    return v


def _assert_row_equal(actual: dict, expected: dict, index: int = 0) -> None:
    """Compare two row dicts; raise AssertionError on first difference."""
    keys = set(actual) | set(expected)
    for k in sorted(keys):
        if k not in actual:
            raise AssertionError(f"Row {index}: missing key '{k}' in actual")
        if k not in expected:
            raise AssertionError(f"Row {index}: extra key '{k}' in actual")
        a, e = actual[k], expected[k]
        if isinstance(a, float) and isinstance(e, float):
            if a != a and e != e:
                continue  # both NaN
            if abs(a - e) > 1e-9:
                raise AssertionError(f"Row {index} key '{k}': {a!r} != {e!r}")
        elif a != e:
            raise AssertionError(f"Row {index} key '{k}': {a!r} != {e!r}")


def get_session():
    """Return a robin_sparkless SparkSession for programmatic tests."""
    import robin_sparkless as rs

    return rs.SparkSession.builder().app_name("test").get_or_create()


def _row_to_dict(r) -> dict:
    """Convert PySpark Row to plain Python dict (handles asDict, Java list->list)."""
    d = r.asDict() if hasattr(r, "asDict") else dict(r)
    out = {}
    for k, v in d.items():
        if (
            v is not None
            and hasattr(v, "__iter__")
            and not isinstance(v, (str, bytes, dict))
        ):
            try:
                out[k] = list(v)
            except (TypeError, ValueError):
                out[k] = v
        else:
            out[k] = v
    return out


def _try_pyspark():
    """Return (pyspark SparkSession, F) or (None, None) if PySpark unavailable."""
    try:
        from pyspark.sql import SparkSession as PySparkSession
        from pyspark.sql import functions as F

        spark = PySparkSession.builder.master("local[1]").appName("test").getOrCreate()
        return spark, F
    except Exception:
        return None, None


def run_with_pyspark_expected(
    pyspark_fn,
    fallback_expected: list[dict],
) -> list[dict]:
    """Run the same logic in PySpark if available; else return fallback expected.

    pyspark_fn(spark, F) should create a DataFrame and return list of row dicts.
    Used for parity tests where we want live PySpark comparison when available.
    """
    pyspark_spark, F = _try_pyspark()
    if pyspark_spark is not None and F is not None:
        try:
            rows = pyspark_fn(pyspark_spark, F)
            return [_row_to_dict(r) for r in rows]
        except Exception:
            pass
    return fallback_expected

# PySpark Version Notes (Python Tests in PySpark Mode)

When running Python tests with `MOCK_SPARK_TEST_BACKEND=pyspark`, we use **PySpark 3.5.x** and **delta-spark 3.x** (see `tests/requirements-pyspark.txt`). This file clarifies which test failures are due to **PySpark version** vs **API differences** (Sparkless extensions vs PySpark).

## Sparkless internal APIs (not in PySpark; prefixed with `_`)

These are available as **internal** APIs (e.g. for tests or migration) but are not part of the PySpark-aligned public API:

| Feature | Sparkless (internal) | PySpark |
|--------|----------------------|---------|
| **Pivot then** `.collect_list(col)` etc. | `PivotedGroupedData._collect_list()`, `_collect_set()`, `_first()`, `_last()`, `_stddev()`, `_variance()`, `_count_distinct()` | ❌ Use `.agg(F.collect_list(col))` etc. |
| **ArrayType** `_element_type` | Property and `__init__` kwarg `_element_type` in `sql/types.py` | ❌ Use `elementType` (camelCase) only |
| **array() with mixed column types** | Polars may coerce | ❌ Raises; same type required |

So the failing tests in PySpark mode are exercising **Sparkless-only APIs or semantics**. Sparkless implements them; PySpark does not (or behaves differently).

## Current setup

- **pyspark**: `>=3.5,<3.6` (3.5.x)
- **delta-spark**: `>=3.0,<4` (compatible with Spark 3.5)
- **Python**: 3.8+ (venv may be 3.9). PySpark 4.x requires Python ≥3.10 for 4.1.x.

## Failures that are *not* fixed by upgrading PySpark

These are **API/behavior differences**; PySpark (including 4.1.1) does not add these.

1. **Pivot after `groupBy().pivot()`**  
   In PySpark, `GroupedData` (including after `.pivot()`) only has: `agg`, `avg`, `count`, `max`, `min`, `sum`.  
   There are **no** methods: `.collect_list()`, `.collect_set()`, `.first()`, `.last()`, `.stddev()`, `.variance()`, `.count_distinct()`.  
   To match PySpark, use `.agg(F.collect_list("col"))` (and similarly for other aggs) instead of `.collect_list("col")`.  
   The public API is PySpark-aligned (no such methods). Use `.agg(F.collect_list("col"))` etc. Internal helpers are available as `._collect_list()`, `._first()`, etc.

2. **`array()` with mixed types**  
   Spark SQL’s `array()` requires all elements to be the same type (or cast to a common type). PySpark raises `AnalysisException` for mixed types in 3.5 and 4.x. This is by design, not an older-version limitation.

3. **`ArrayType` attribute name**  
   PySpark uses `elementType` (camelCase). Sparkless exposes that; internal `_element_type` is also available.

## Upgrading to PySpark 4.x

- **Latest on PyPI**: 4.1.1 (requires Python ≥3.10).
- **delta-spark**: For Spark 4 use delta-spark 4.x; for Spark 3.5 keep delta-spark 3.x.
- Upgrading to 4.x may fix or change a small number of behaviors (e.g. error messages, edge cases) but will **not** add the GroupedData pivot helpers or mixed-type `array()` above.  
- To try PySpark 4.x: use a Python 3.10+ env and `pyspark>=4.0,<4.2` and `delta-spark>=4.0` (if you need Delta with Spark 4).

## Summary

Most of the “missing” behavior in PySpark mode is due to **tests targeting Sparkless-only APIs or semantics**, not an older PySpark. Aligning tests with PySpark means using `.agg(F.collect_list(...))` (and similar) for pivot, same-type `array()` only, and backend-agnostic type attribute names; upgrading PySpark alone will not fix those.

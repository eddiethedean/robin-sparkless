# Sparkless vs PySpark: Parity Issues Reported Upstream

When comparing **Sparkless** (the Python PySpark drop-in at [github.com/eddiethedean/sparkless](https://github.com/eddiethedean/sparkless)) with PySpark, we found several parity issues and reported them on the Sparkless repo. This document lists those issues and the script used to find them.

## How to reproduce

From the repo root:

```bash
pip install sparkless
python scripts/sparkless_parity_check.py
```

The script runs a battery of PySpark-idiomatic operations against Sparkless and prints which checks fail.

## Reported issues (Sparkless GitHub)

| Issue | Title | Link |
|-------|--------|------|
| #412 | API: SparkSession.builder() causes TypeError (not callable) | https://github.com/eddiethedean/sparkless/issues/412 |
| #413 | union() fails with createDataFrame(data, column_names): column order/name mismatch | https://github.com/eddiethedean/sparkless/issues/413 |
| #414 | Window functions: row_number().over(WindowSpec) fails with 'over() got an unexpected keyword argument \'descending\'' | https://github.com/eddiethedean/sparkless/issues/414 |
| #415 | DataFrame.orderBy(list of column names) treats list as single column | https://github.com/eddiethedean/sparkless/issues/415 |

## Not reported (by design)

- **toPandas()** requires `pip install sparkless[pandas]` (or pandas). PySpark also requires pandas for `toPandas()`, so this is an optional-dependency choice, not a behavioral parity bug.

## Summary

- **builder()**: Use `SparkSession.builder.appName(...).getOrCreate()` (no parentheses on `builder`) when using Sparkless.
- **union()**: Use `unionByName()` if you hit column order mismatch with `createDataFrame(..., list_of_names)`.
- **Window + row_number()**: Avoid until fixed upstream, or use a workaround (e.g. order in a separate step).
- **orderBy([list])**: Use `orderBy("a", "b")` or `orderBy(*["a", "b"])` instead of `orderBy(["a", "b"])` in Sparkless.

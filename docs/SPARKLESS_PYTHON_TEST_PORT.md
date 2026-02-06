# Sparkless Python Test Port Tracker

This doc tracks which Sparkless Python tests have been **ported** to run against `robin_sparkless` in this repo, which were **skipped as duplicates** of existing fixtures or `tests/python/test_robin_sparkless.py`, and which are **deferred** (unsupported API or low priority).

**Ground truth:** Expected results for ported tests must come from **PySpark** (not Sparkless). Use the `spark` fixture from `tests/python/conftest.py` and `assert_rows_equal()` from `tests/python/utils.py` in ported tests.

## Why only 7 Python tests ported?

Sparkless has **hundreds** of tests (313+ test files, many test methods in `parity/dataframe/`, `parity/functions/`, `parity/sql/`, etc.). Only 7 were ported as **Python** tests because:

1. **No duplicates** – We skip any scenario already covered by (a) hand-written fixtures in `tests/fixtures/*.json`, (b) **226 converted fixtures** in `tests/fixtures/converted/` (from Sparkless `expected_outputs`), or (c) `test_robin_sparkless.py`. So e.g. filter (age > 30) and group_by count were not ported again.
2. **Fixture conversion covers many scenarios** – The 226 converted JSON fixtures are run as **Rust** parity tests (`cargo test pyspark_parity_fixtures`). Those are “ported” as fixture-driven parity, not as separate Python test functions. The 7 Python tests are extra coverage with inlined expectations.
3. **API gaps** – Many Sparkless tests depend on **PySpark** APIs Robin does not implement: **SQL** (`spark.sql`, DDL/DML, subqueries, CTEs, HAVING), **RDD** (`df.rdd`, `foreach`, `foreachPartition`, `mapInPandas`, `mapPartitions`), **UDFs** (`udf`, `pandas_udf`, UDTF), **catalog** (databases, tables, `writeTo`), **streaming** (`withWatermark`, `isStreaming`), and **built-in functions** (XML/XPath, sentences, sketch functions, etc.). All of these are standard PySpark APIs; see [ROBIN_SPARKLESS_MISSING.md](ROBIN_SPARKLESS_MISSING.md) (scoped to PySpark parity). Those tests are deferred until Robin supports them.
4. **Initial batch** – The port focused on a small set of DataFrame ops Robin fully supports (filter, select, join, groupBy+agg) that were not duplicate coverage. Expanding the port means adding more Python tests for supported ops and/or implementing more APIs and then porting the corresponding tests.

## Ported

| Sparkless test / scenario | Robin test / location | Notes |
|---------------------------|------------------------|-------|
| test_filter_with_boolean (filter salary > 60000) | test_dataframe_parity.py::test_filter_salary_gt_60000 | |
| test_filter_with_and_operator | test_dataframe_parity.py::test_filter_and_operator | |
| test_filter_with_or_operator | test_dataframe_parity.py::test_filter_or_operator | |
| test_basic_select | test_dataframe_parity.py::test_basic_select | |
| test_select_with_alias | test_dataframe_parity.py::test_select_with_alias | via with_column + select |
| test_aggregation (groupBy + avg, count) | test_dataframe_parity.py::test_aggregation_avg_count | |
| test_inner_join | test_dataframe_parity.py::test_inner_join | |

## Skipped (duplicate of existing coverage)

| Sparkless test / scenario | Existing coverage | Notes |
|---------------------------|-------------------|-------|
| test_filter_operations (age > 30) | fixture filter_age_gt_30, test_filter_and_select | Same scenario. |
| test_group_by (department count) | test_robin_sparkless.py::test_group_by_count, fixture groupby_count | Same scenario. |

## Deferred

| Sparkless test / scenario | Reason |
|---------------------------|--------|
| (unsupported API, RDD, streaming, etc.) | List here when a candidate is not ported due to missing API or low priority. |

## How to port (no duplicates)

1. **Discover** Sparkless tests under `tests/` (e.g. `parity/dataframe/`, `parity/functions/`, `unit/`) that use only APIs Robin implements (see plan: filter, select, groupBy, join, window, etc.).
2. **Deduplicate:** For each candidate, check (1) whether the same scenario is already covered by a parity fixture in `tests/fixtures/*.json` or `tests/fixtures/converted/*.json`, and (2) whether `tests/python/test_robin_sparkless.py` already has an equivalent test. If yes, add to "Skipped" above and do not port.
3. **Port:** Add a new test in `tests/python/` (e.g. `test_dataframe_ops.py`, `test_functions_parity.py`) using `import robin_sparkless as rs`, the `spark` fixture, and expected from PySpark (run same scenario in PySpark in the test, or use precomputed expected). Use `assert_rows_equal(actual, expected, order_matters=...)` from `tests/python/utils.py`.
4. **Update this table:** Add the ported test under "Ported" or the skipped one under "Skipped (duplicate)".

## Related

- [CONVERTER_STATUS.md](CONVERTER_STATUS.md) – fixture conversion and dedupe
- [SPARKLESS_PARITY_STATUS.md](SPARKLESS_PARITY_STATUS.md) – fixture parity results
- [TEST_CREATION_GUIDE.md](TEST_CREATION_GUIDE.md) – fixture format and PySpark as oracle

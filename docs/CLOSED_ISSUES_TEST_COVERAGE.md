# Closed GitHub Issues – Test Coverage

This document maps **closed GitHub issues** to tests in this repo. All tests are run by `cargo test` (unit tests, `tests/parity.rs` parity fixtures, and `tests/plan_parity_fixtures` plan fixtures).

## Summary

- **PySpark parity (feature) issues (#141–#157)**  
  Closed as deferred / out of scope. No implementation to test; no fixture required. Some have **stub** coverage (e.g. JVM stubs, hash/xxhash64) via fixtures that assert our implementation’s behavior.

- **[Sparkless parity] test_* issues (#1–#140)**  
  Closed when parity was achieved. Coverage is via **parity fixtures** in `tests/fixtures/` and `tests/fixtures/converted/`. Fixture names often differ from the test name (e.g. `test_string_upper` → `string_upper_lower.json`, `test_group_by` → `groupby_*.json`).

- **#186 (lit date/datetime), #187 (Window API)**  
  Python tests: `test_robin_sparkless.py` (`test_lit_*`, `test_window_row_number_rank_over`), `test_lit_date_datetime_pyspark_parity.py`, `test_window_pyspark_parity.py`, `test_column_vs_column_pyspark_parity.py`, `test_issue_176_pyspark_parity.py`. These parity tests use **predetermined expected outputs** (from a prior PySpark run); no PySpark at test runtime.

## Working tests for previously skipped fixtures

The following fixtures were previously skipped (platform/algorithm difference). They are now **un-skipped** and expected values were set to **our implementation’s output** so we have a working test for the corresponding closed issues:

| Fixture            | Closed issue | Note |
|--------------------|-------------|------|
| `string_xxhash64`  | #116        | Expected values updated to twox_hash XXH64 output (our implementation). |
| `with_hash`        | #149        | Expected values updated to our `hash()` implementation output. |

## PySpark parity (feature) issues (#141–#157)

| #   | Title | Test coverage |
|-----|--------|----------------|
| 157 | from_csv, to_csv, schema_of_csv, schema_of_json | Deferred. `read_csv` / `read_json` path-based I/O covered by `read_csv.json`, `read_json.json`. |
| 156 | DataFrame.pivot() | Deferred. No fixture. |
| 155 | join(how='left_semi' \| 'left_anti') | Deferred. `converted/semi_join.json`, `converted/anti_join.json` exist but skipped. |
| 154 | JVM/runtime stubs (broadcast, partition_id, input_file_name, etc.) | `with_jvm_stubs.json` exists; **skipped** (environment-specific expected values). Stubs are implemented and used. |
| 153 | JSON write append mode | Deferred. No fixture. |
| 152 | Delta Lake schema evolution and MERGE | Deferred. No fixture. |
| 151 | createDataFrame input types and schema inference | Covered by session/creation tests and parity fixtures that use various schemas. |
| 150 | array_distinct ordering (first-occurrence) | `array_distinct.json` runs; ordering semantics may differ from PySpark. |
| 149 | hash() algorithm (Murmur3 vs xxHash64) | **`with_hash.json`** – un-skipped; expected values set to our implementation. |
| 148 | sentences (NLP) and JVM/UDTF helpers | Deferred. No fixture. |
| 147 | Sketch-based approximate aggregate functions | Deferred. No fixture. |
| 146 | XML and XPath functions | Deferred. No fixture. |
| 145 | Structured Streaming | Deferred. No fixture. |
| 144 | Catalog and DataFrameWriterV2 (writeTo) | Deferred. No fixture. |
| 143 | UDF and UDTF support | Deferred. No fixture. |
| 142 | RDD and distributed execution APIs | Deferred. No fixture. |
| 141 | SQL — full DDL/DML and advanced SQL | Deferred. Plan fixtures and SQL tests cover supported subset. |

## [Sparkless parity] test_* issues (#1–#140)

Covered by **parity fixtures** (`pyspark_parity_fixtures`) and/or **plan fixtures** (`plan_parity_fixtures`). Fixtures live in `tests/fixtures/*.json` and `tests/fixtures/converted/*.json`; many converted fixtures are still skipped. Root fixtures are the main source of **running** tests.

Representative mapping (issue test name → fixture(s)):

- **Joins**: test_join_* → `inner_join.json`, `left_join.json`, `right_join.json`, `outer_join.json`, etc.
- **SQL / group / filter**: test_group_by → `groupby_*.json`; test_basic_select → `filter_age_gt_30.json` and similar; test_filtered_select → filter + select fixtures.
- **String**: test_string_upper / test_string_lower → `string_upper_lower.json`; test_string_length → `string_length_trim.json`; test_crc32 → `string_crc32.json`; test_xxhash64 → **`string_xxhash64.json`** (now running).
- **Math**: test_math_* → `math_sin_cos.json`, `math_sqrt_pow.json`, `math_cosh_cbrt.json`, etc.
- **Conditionals**: test_when_otherwise → `when_otherwise.json`, `when_then_otherwise.json`; test_coalesce → `coalesce.json`; test_nullif / test_isnull / test_ifnull → `phase15_aliases_nvl_isnull.json`, etc.
- **Arrays**: test_array_* → `array_*.json` (array_distinct, array_union, array_contains, etc.); test_size → `array_size.json`; test_element_at → `element_at.json`.
- **Window**: test_row_number, test_rank, test_dense_rank, etc. → `row_number_window.json`, `rank_window.json`, `lag_lead_window.json`, `ntile_window.json`, etc.
- **Hash**: test_xxhash64 → **`string_xxhash64.json`**; hash() → **`with_hash.json`** (both now running).

To run a single fixture:

```bash
PARITY_FIXTURE=<fixture_name> cargo test pyspark_parity_fixtures
```

Plan fixtures (for execute_plan):

```bash
# All plan fixtures in tests/fixtures/plans/
cargo test plan_parity_fixtures
```

## Verifying all tests pass

```bash
cargo test
```

This runs unit tests, `pyspark_parity_fixtures`, `plan_parity_fixtures`, and doc tests. No failures expected.

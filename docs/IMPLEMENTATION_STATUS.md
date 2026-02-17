# Implementation Status: Polars Migration

## Strategic Direction: Sparkless Backend Replacement

Robin-sparkless is designed to **replace the backend logic** of [Sparkless](https://github.com/eddiethedean/sparkless)—the Python PySpark drop-in replacement. Sparkless would call robin-sparkless via PyO3/FFI for DataFrame execution. See [SPARKLESS_INTEGRATION_ANALYSIS.md](SPARKLESS_INTEGRATION_ANALYSIS.md) for architecture mapping, structural learnings, and test conversion strategy.

## Build & Test Status

- `cargo check` passes for the Rust-only, Polars-backed implementation.
- `cargo build --features pyo3` builds the Python extension (optional). `cargo build --features "pyo3,sql"` and `cargo build --features "pyo3,delta"` add SQL and Delta Lake support.
- There are no outstanding Rust compiler errors.
- `cargo test` passes (unit/integration/doc tests).
- `make test` runs Rust tests plus Python tests (creates `.venv`, `maturin develop --features "pyo3,sql,delta"`, `pytest tests/python/`).
- `make check-full` runs Rust check, then `lint-python` (ruff format, ruff check, mypy), then Python tests.
- `make sparkless-parity` runs parity over hand-written and (if present) converted fixtures; set `SPARKLESS_EXPECTED_OUTPUTS` to convert from Sparkless first.

## ✅ Completed

### 1. Rust Core (default build)
- Default build is pure Rust (no Python). Library exposes a Rust API.
- Optional **PyO3 bridge**: `pyo3` feature adds Python bindings; see [PYTHON_API.md](PYTHON_API.md).

### 2. Polars Integration
- `DataFrame` uses Polars `LazyFrame` internally (#438): transformations extend the lazy plan; only actions (`collect`, `show`, `count`, `write`, etc.) trigger materialization. Data sources (`read_csv`, `read_parquet`, `read_json`) return lazy DataFrames.
- `Column` is a thin wrapper around Polars `Expr`.
- Basic helpers implemented in `functions.rs` for literals and aggregates.

### 3. Session API + IO
- `SparkSession` and `SparkSessionBuilder` are the Rust-facing entry point.
- File readers are implemented via Polars IO:
  - `SparkSession::read_csv`
  - `SparkSession::read_parquet`
  - `SparkSession::read_json`

### 4. PySpark Parity Harness
- `tests/gen_pyspark_cases.py` generates JSON fixtures from PySpark.
- `tests/parity.rs` runs the fixtures through robin-sparkless and asserts parity.
- Parity coverage is tracked in `PARITY_STATUS.md`.

## ⚙️ In Progress / Planned (toward broader PySpark parity)

1. **PySpark-inspired API surface**
   - Clarify which PySpark methods we intend to emulate first.
   - Align naming and signatures (adapted to Rust) for `SparkSession`, `DataFrame`, `Column`.

2. **Behavioral Parity Slice**
   - Continue expanding parity coverage by adding fixtures for new capabilities and edge cases.
   - Current fixture coverage and status lives in `PARITY_STATUS.md`.

3. **Joins** ✅ **COMPLETED**
   - ✅ Implemented common join types (inner, left, right, outer) via `DataFrame::join()`
   - ✅ Parity fixtures for inner, left, right, outer joins

4. **String functions** ✅ **COMPLETED**
   - ✅ `upper()`, `lower()`, `substring()` (1-based), `concat()`, `concat_ws()`
   - ✅ Parity fixtures: `string_upper_lower`, `string_substring`, `string_concat`
   - Expand built-in functions (date/math) with explicit PySpark semantics.
   - Add additional type coercion and null-handling edge cases as fixtures.

5. **Window functions** ✅ **COMPLETED**
   - ✅ `Column::rank()`, `row_number()`, `dense_rank()`, `lag()`, `lead()` with `.over(partition_by)`
   - ✅ Parity fixtures: `row_number_window`, `rank_window`, `lag_lead_window`
   - ✅ `SparkSession::sql()` implemented (optional `sql` feature); temp views and in-memory saved tables (`saveAsTable`, `write_delta_table`); catalog `listTables`, `tableExists`, `dropTempView`, `dropTable`; see [QUICKSTART.md](QUICKSTART.md), [PYTHON_API.md](PYTHON_API.md).

6. **PyO3 Bridge** ✅ **COMPLETED** (Phase 4)
   - Optional `pyo3` feature (PyO3 0.24); `src/python/mod.rs` exposes `robin_sparkless` Python module.
   - SparkSession, DataFrame, Column, GroupedData; create_dataframe, filter, select, join, group_by, collect (list of dicts), read_csv/parquet/json, etc.
   - `maturin develop --features "pyo3,sql,delta"`; Python tests in `tests/python/` (45 tests); `make test` runs Rust + Python tests; `make check-full` adds ruff, mypy, and full CI.
   - API contract: [PYTHON_API.md](PYTHON_API.md).

7. **Phase 5 Test Conversion** ✅ **COMPLETED**
   - Fixture converter maps Sparkless `expected_outputs` to robin-sparkless format (join, window, withColumn, union, distinct, drop, dropna, fillna, limit, withColumnRenamed, etc.).
   - Parity discovers `tests/fixtures/` and `tests/fixtures/converted/`; optional `skip: true` in fixtures.
  - `make sparkless-parity` (set `SPARKLESS_EXPECTED_OUTPUTS` to run converter first); 159 hand-written fixtures passing (array_distinct, with_curdate_now skipped).
  - See [CONVERTER_STATUS.md](CONVERTER_STATUS.md), [SPARKLESS_PARITY_STATUS.md](SPARKLESS_PARITY_STATUS.md).

8. **Phase 6 Broad Function Parity** (partial) ✅
   - Array: `array_size`/`size`, `array_contains`, `element_at`, `explode`, `array_sort`, `array_join`, `array_slice`; **implemented** (via Polars list.eval): `array_position`, `array_remove`, `posexplode`, `array_exists`, `array_forall`, `array_filter`, `array_transform`, `array_sum`, `array_mean`; **Phase 8**: `array_repeat`, `array_flatten` **implemented** via map UDFs. Fixtures: `array_contains`, `element_at`, `array_size`, `array_sum`.
   - Window: `first_value`, `last_value`, `percent_rank`, `cume_dist`, `ntile`, `nth_value` with `.over()`; parity fixtures for all (percent_rank/cume_dist/ntile/nth_value via multi-step workaround).
   - String: `regexp_extract_all`, `regexp_like`; **Phase 10**: `mask`, `translate`, `substring_index`; **Phase 8**: `soundex`, `levenshtein`, `crc32`, `xxhash64` **implemented** via map UDFs.
   - **Phase 10**: JSON `get_json_object`, `from_json`, `to_json`. **Phase 8**: Map `create_map`, `map_keys`, `map_values`, `map_entries`, `map_from_arrays` **implemented**. See [PYSPARK_DIFFERENCES.md](PYSPARK_DIFFERENCES.md).

9. **Phase 7 SQL & Advanced** ✅ **COMPLETED**
   - Optional **SQL** (`sql` feature): `SparkSession::sql(query)`, temp views and in-memory tables (`create_or_replace_temp_view`, `table(name)`, `df.write().saveAsTable(name, mode)`, `write_delta_table(name)`); catalog `listTables`, `tableExists`, `dropTempView`, `dropTable`; `read_delta(name_or_path)` (path → Delta on disk, name → in-memory table); sqlparser → DataFrame ops (SELECT, FROM, WHERE, JOIN, GROUP BY, ORDER BY, LIMIT).
   - Optional **Delta Lake** (`delta` feature): `read_delta`, `read_delta_with_version` (time travel), `write_delta` (overwrite/append) via delta-rs.
   - **Performance**: `cargo bench` (criterion) compares robin-sparkless vs Polars; target within ~2x. Error messages improved; Troubleshooting in [QUICKSTART.md](QUICKSTART.md).

10. **Path to 100% before Sparkless integration** ([ROADMAP.md](ROADMAP.md) Phases 12–21)
   - **Phase 12** ✅ **COMPLETED**: DataFrame methods parity — implemented sample, random_split, first, head, take, tail, is_empty, to_df, stat (cov/corr), summary, to_json, explain, print_schema, checkpoint, local_checkpoint, repartition, coalesce, select_expr, col_regex, with_columns, with_columns_renamed, na (fill/drop), to_pandas, offset, transform, except_all, intersect_all; **freq_items**, **approx_quantile**, **crosstab**, **melt** (full implementations in Rust); **sample_by** (stratified sampling); Spark no-ops (hint, is_local, input_files, same_semantics, semantic_hash, observe, with_watermark). Parity fixtures: first_row, head_n, offset_n. PyO3: all new DataFrame methods exposed including random_split, summary, to_df, select_expr, col_regex, with_columns, with_columns_renamed, stat(), na(), to_pandas; PyDataFrameStat (cov, corr) and PyDataFrameNa (fill, drop). See [PYTHON_API.md](PYTHON_API.md). Methods ~35 → ~55+.
   - **Phase 13** ✅ (completed): Functions batch 1 — string: ascii, format_number, overlay, position, char, chr; base64, unbase64; binary: sha1, sha2, md5 (hex string out); collection: array_compact. Parity: parse_with_column_expr branches and fixtures `string_ascii`, `string_format_number` (82 fixtures total). PyO3: module-level and Column methods (ascii_, format_number, overlay, position, char_, chr_, base64_, unbase64_, sha1_, sha2_, md5_, array_compact). Dependencies: base64, sha1, sha2, md5 crates.
   - **Phase 14** ✅ (completed): Functions batch 2 — math: sin, cos, tan, asin, acos, atan, atan2, degrees, radians, signum (UDFs in udfs.rs); datetime: quarter, weekofyear, dayofweek, dayofyear, add_months, months_between, next_day (Polars dt + chrono UDFs); type/conditional: cast, try_cast (parse_type_name + strict_cast/cast), isnan, greatest, least (UDF apply_greatest2/apply_least2). Parity: parser branches for all; fixtures `math_sin_cos`, `datetime_quarter_week` (84 fixtures). PyO3: module and Column methods for all Phase 14 functions. Docs: PYTHON_API, PARITY_STATUS, IMPLEMENTATION_STATUS, ROADMAP updated.
   - **Phase 15** ✅ **COMPLETED**: Functions batch 3 — Batch 1 (nvl, nvl2, substr, power, ln, ceiling, lcase, ucase, dayofmonth, to_degrees, to_radians, isnull, isnotnull), Batch 2 (left, right, replace, startswith, endswith, contains, like, ilike, rlike), Batch 3 (cosh, sinh, tanh, acosh, asinh, atanh, cbrt, expm1, log1p, log10, log2, rint, hypot), Batch 4 (array_distinct) implemented; parity fixtures 84 → 88. Gap list: [PHASE15_GAP_LIST.md](PHASE15_GAP_LIST.md), [GAP_ANALYSIS_SPARKLESS_3.28.md](GAP_ANALYSIS_SPARKLESS_3.28.md).
   - **Phase 16** ✅ **COMPLETED**: String/regex — regexp_count, regexp_instr, regexp_substr, split_part, find_in_set, format_string, printf. Parity fixtures: regexp_count, regexp_substr, regexp_instr, split_part, find_in_set, format_string (93 fixtures total; array_distinct skipped). PyO3: module and Column methods for all. See [ROADMAP.md](ROADMAP.md).
   - **Phase 17** ✅ **COMPLETED**: Datetime/unix — unix_timestamp, to_unix_timestamp, from_unixtime, make_date, timestamp_seconds, timestamp_millis, timestamp_micros, unix_date, date_from_unix_date; math: pmod, factorial. Parity fixtures: unix_timestamp, from_unixtime, make_date, timestamp_seconds, timestamp_millis, timestamp_micros, unix_date, date_from_unix_date, pmod, factorial (103 fixtures total). PyO3: module and Column methods for all.
   - **Phase 18** ✅ **COMPLETED**: Array/map/struct — array_append, array_prepend, array_insert, array_except/intersect/union, zip_with, map_concat, map_filter, map_zip_with, named_struct (124 fixtures). PyO3: map_filter_value_gt, zip_with_coalesce, map_zip_with_coalesce.
   - **Phase 19** ✅ **COMPLETED**: Aggregates (any_value, bool_and, bool_or, count_if, max_by, min_by, percentile, product, collect_list, collect_set), try_* (try_divide, try_add, try_subtract, try_multiply), misc (width_bucket, elt, bit_length, typeof). Parity fixtures: groupby_any_value, groupby_product, try_divide, width_bucket (128 fixtures). PyO3: GroupedData methods; try_*, width_bucket, elt, bit_length, typeof.
   - **Phase 20** ✅ **COMPLETED**: Ordering (asc, desc, nulls_first/last); aggregates (median, mode, stddev_pop, var_pop, try_sum, try_avg); numeric (bround, negate, positive, cot, csc, sec, e, pi). Parity: groupby_median, with_bround; order_by_exprs. PyO3: PySortOrder, order_by_exprs.
   - **Phase 21** ✅ **COMPLETED**: String (btrim, locate, conv); binary (hex, unhex, bin, getbit); type (to_char, to_varchar, to_number, try_to_number, try_to_timestamp); array (arrays_overlap, arrays_zip, explode_outer, posexplode_outer, array_agg); map (str_to_map); struct (transform_keys, transform_values). Parity: with_btrim, with_hex, with_conv, with_str_to_map, arrays_overlap, arrays_zip (136 fixtures). PyO3: all except transform_keys/transform_values.
   - **Phase 23** ✅ (completed): JSON, URL, misc (isin, url_decode, url_encode, json_array_length, parse_url, hash, shift_left, shift_right, version, equal_null, stack). Parity: with_isin, with_url_decode, with_url_encode, json_array_length_test, with_hash, with_shift_left (159 fixtures). PyO3 bindings for all.
   - **Phase 22** ✅ (completed): Datetime extensions — curdate, now, localtimestamp, date_diff, dateadd, datepart, extract, date_part, unix_micros/millis/seconds, dayname, weekday, make_timestamp, make_timestamp_ntz, make_interval, timestampadd, timestampdiff, days, hours, minutes, months, years, from_utc_timestamp, to_utc_timestamp, convert_timezone, current_timezone, to_timestamp. Parity: fixtures with_dayname, with_weekday, with_extract, with_unix_micros, make_timestamp_test, timestampadd_test, from_utc_timestamp_test. PyO3 bindings for all.
   - **Phase 24** ✅ **COMPLETED**: Bit ops, control (assert_true, raise_error), JVM stubs, rand/randn (real RNG, optional seed; per-row values when used in with_column/with_columns), AES crypto (aes_encrypt, aes_decrypt, try_aes_decrypt; AES-128-GCM).
   - **Phases 23–24**: Full parity (JSON/CSV/URL, bit/control/JVM/random/crypto).
   - **Phase 25** ✅ **COMPLETED**: Readiness for post-refactor merge — plan interpreter (`execute_plan`), expression interpreter (all scalar functions; serialized expr → Expr in `src/plan/expr.rs`), logical plan schema ([LOGICAL_PLAN_FORMAT.md](LOGICAL_PLAN_FORMAT.md)), 3 plan fixtures (`tests/fixtures/plans/`), `create_dataframe_from_rows` (Rust + Python). See [READINESS_FOR_SPARKLESS_PLAN.md](READINESS_FOR_SPARKLESS_PLAN.md).
   - **Phase 26**: Prepare and publish robin-sparkless as a Rust crate (crates.io, API stability, docs, release workflow; optional PyPI wheel).
   - **Phase 27**: Sparkless integration (BackendFactory "robin", 200+ tests passing).

11. **Sparkless integration** (in Sparkless repo, after Phase 17)
   - Fixture converter: Sparkless `expected_outputs/` JSON → robin-sparkless fixtures
   - Structural alignment: service-style modules, trait-based backends, case sensitivity
   - Function parity: use [PYSPARK_FUNCTION_MATRIX](https://github.com/eddiethedean/sparkless/blob/main/PYSPARK_FUNCTION_MATRIX.md) as checklist

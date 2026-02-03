# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Phase 17 – Remaining gaps 2: datetime/unix and math** ✅ **COMPLETED**
  - **Datetime/unix**: `unix_timestamp`, `to_unix_timestamp`, `from_unixtime`, `make_date`, `timestamp_seconds`, `timestamp_millis`, `timestamp_micros`, `unix_date`, `date_from_unix_date` (Rust + PyO3 + parity parser).
  - **Math**: `pmod`, `factorial` (positive modulus; n! for n in 0..=20).
  - **Implementation**: UDFs in udfs.rs for parsing/formatting (chrono), epoch conversion via Polars cast/mul for timestamp_*, date↔days via Polars Date/Int32 cast.
  - **Parity fixtures**: `unix_timestamp`, `from_unixtime`, `make_date`, `timestamp_seconds`, `timestamp_millis`, `timestamp_micros`, `unix_date`, `date_from_unix_date`, `pmod`, `factorial` (93 → 103 fixtures).

- **Phase 16 – Remaining gaps 1: string/regex** ✅ **COMPLETED**
  - **String/regex**: `regexp_count`, `regexp_instr`, `regexp_substr`, `split_part`, `find_in_set`, `format_string`, `printf` (Rust + PyO3 + parity parser).
  - **Implementation**: `regexp_count` via Polars `str().count_matches()`, `regexp_substr` as alias of `regexp_extract(0)`, `regexp_instr` and `find_in_set` via UDFs in udfs.rs, `format_string`/`printf` with printf-style %s/%d/%f/%g parsing, `split_part` via split + list.get.
  - **Parity fixtures**: `regexp_count`, `regexp_substr`, `regexp_instr`, `split_part`, `find_in_set`, `format_string` (88 → 94 fixtures).

- **Phase 15 – Functions batch 3** ✅ **COMPLETED**
  - **Batch 1 (aliases/simple)**: `nvl`, `ifnull`, `nvl2`, `substr`, `power`, `ln`, `ceiling`, `lcase`, `ucase`, `dayofmonth`, `to_degrees`, `to_radians`, `isnull`, `isnotnull` (Rust + PyO3 + parity parser). Fixture `phase15_aliases_nvl_isnull`.
  - **Batch 2 (string)**: `left`, `right`, `replace` (literal), `startswith`, `endswith`, `contains`, `like` (SQL LIKE → regex), `ilike`, `rlike`/`regexp`. Fixture `string_left_right_replace`.
  - **Batch 3 (math)**: `cosh`, `sinh`, `tanh`, `acosh`, `asinh`, `atanh`, `cbrt`, `expm1`, `log1p`, `log10`, `log2`, `rint`, `hypot` (UDFs in udfs.rs + Column/functions + PyO3). Fixture `math_cosh_cbrt`.
  - **Batch 4 (array)**: `array_distinct`. Fixture `array_distinct`.
  - Parity fixtures: 84 → 88. Remaining gaps (string/regex, datetime/unix, array/map/struct, aggregates/try_*) are planned in ROADMAP Phases 16–19; then Phase 20 (publish crate), Phase 21 (Sparkless integration). Gap list: [docs/PHASE15_GAP_LIST.md](docs/PHASE15_GAP_LIST.md), [docs/GAP_ANALYSIS_SPARKLESS_3.28.md](docs/GAP_ANALYSIS_SPARKLESS_3.28.md).

- **Phase 1 – Foundation** ✅
  - Structural alignment: split `dataframe.rs` into `transformations.rs`, `aggregations.rs`, `joins.rs`.
  - Case sensitivity: `spark.sql.caseSensitive` (default false), centralized column resolution for filter, select, withColumn, join; fixture `case_insensitive_columns`.
  - Fixture converter: `tests/convert_sparkless_fixtures.py` maps Sparkless `expected_outputs` → robin-sparkless format; operation mapping for filter, groupby, join, window, withColumn, union, distinct, drop, dropna, fillna, limit, withColumnRenamed.

- **Phase 2 – High-Value Functions** (partial) ✅
  - String: `length`, `trim`, `ltrim`, `rtrim`, `regexp_extract`, `regexp_replace`, `split`, `initcap`; parity fixture `string_length_trim`.
  - Datetime: `to_date()`, `date_format(format)` (chrono strftime), `year`, `month`, `day` in Rust API (no date/datetime fixture yet; harness does not build date columns from JSON).
  - Additional parity: `regexp_like`, `regexp_extract_all` (fixtures + parser support).

- **Phase 3 – DataFrame Methods** ✅
  - `union` / `unionAll`, `unionByName`, `distinct` / `dropDuplicates`, `drop`, `dropna`, `fillna`, `limit`, `withColumnRenamed`.
  - Parity fixtures: `union_all`, `union_by_name`, `distinct`, `drop_columns`, `dropna`, `fillna`, `limit`, `with_column_renamed`.

- **Phase 4 – PyO3 Bridge** ✅
  - Optional Python bindings when built with `--features pyo3`.
  - Python module `robin_sparkless` with PySpark-like API: `SparkSession`, `SparkSessionBuilder`, `DataFrame`, `Column`, `GroupedData`, `WhenBuilder`, `ThenBuilder`.
  - Session: `builder()`, `get_or_create()`, `create_dataframe`, `read_csv`, `read_parquet`, `read_json`, `is_case_sensitive()`.
  - DataFrame: `filter`, `select`, `with_column`, `order_by`, `group_by`, `join`, `union`, `union_by_name`, `distinct`, `drop`, `dropna`, `fillna`, `limit`, `with_column_renamed`, `count`, `show`, `collect` (list of dicts).
  - Column/expressions: `col`, `lit`, `when().then().otherwise()`, `coalesce`, `sum`, `avg`, `min`, `max`, `count`; methods `gt`, `ge`, `lt`, `le`, `eq`, `ne`, `and_`, `or_`, `alias`, `is_null`, `is_not_null`, `upper`, `lower`, `substr`.
  - GroupedData: `count()`, `sum(column)`, `avg(column)`, `min(column)`, `max(column)`, `agg(exprs)`.
  - Build: `maturin develop --features pyo3`; API contract in [docs/PYTHON_API.md](docs/PYTHON_API.md). Python smoke tests in `tests/python/`; `make test` runs Rust + Python tests.

- **Phase 5 – Test Conversion** ✅
  - Parity test discovers `tests/fixtures/*.json` and `tests/fixtures/converted/*.json`; optional `skip: true` / `skip_reason`.
  - `make sparkless-parity`: when `SPARKLESS_EXPECTED_OUTPUTS` is set, runs converter then `cargo test pyspark_parity_fixtures`.
  - 58 hand-written fixtures at Phase 5 completion; target 50+ met. See [CONVERTER_STATUS.md](docs/CONVERTER_STATUS.md), [SPARKLESS_PARITY_STATUS.md](docs/SPARKLESS_PARITY_STATUS.md).

- **Phase 6 – Broad Function Parity** (partial) ✅
  - **Joins**: `DataFrame::join()` with `JoinType` (Inner, Left, Right, Outer); parity fixtures `inner_join`, `left_join`, `right_join`, `outer_join`; `right_input` and `Operation::Join` in harness.
  - **Multi-aggregation**: `GroupedData::agg()` with multiple aggregations; fixture `groupby_multi_agg`.
  - **Window**: `Column::rank()`, `row_number()`, `dense_rank()`, `lag()`, `lead()` with `.over(partition_by)`; `first_value`, `last_value`, `percent_rank`; fixtures `row_number_window`, `rank_window`, `lag_lead_window`, `first_value_window`, `last_value_window`, `percent_rank_window`. API for `cume_dist`, `ntile`, `nth_value` (partition_by).
  - **Array**: `array_size`/`size`, `array_contains`, `element_at`, `explode`, `array_sort`, `array_join`, `array_slice`; **implemented** (Polars list.eval): `array_position`, `array_remove`, `posexplode`; fixtures `array_contains`, `element_at`, `array_size`.
  - **String**: `regexp_extract_all`, `regexp_like`; PyColumn exposure for `size`, `element_at`, `explode`, `first_value`, `last_value`, `percent_rank`, `regexp_like`.
  - **String (basics)**: `upper`, `lower`, `substring` (1-based), `concat`, `concat_ws`; fixtures `string_upper_lower`, `string_substring`, `string_concat`.

- **Phase 7 – SQL & Advanced** ✅
  - **SQL** (optional `sql` feature): `SparkSession::sql(query)` with temp views (`create_or_replace_temp_view`, `table`). Single SELECT, FROM/JOIN, WHERE, GROUP BY, ORDER BY, LIMIT → DataFrame ops. Python: `spark.sql()`, `spark.create_or_replace_temp_view()`, `spark.table()`.
  - **Delta Lake** (optional `delta` feature): `read_delta(path)`, `read_delta_with_version(path, version)` (time travel), `write_delta(path, overwrite)`. Python bindings for read_delta, read_delta_version, write_delta.
  - **Performance**: Criterion benchmarks `cargo bench` (filter/select/groupBy robin vs Polars); target within ~2x.
  - **Robustness**: Clearer error messages (column names, hints); Troubleshooting in [QUICKSTART.md](docs/QUICKSTART.md).

- **Phase 9 – High-Value Functions & DataFrame Methods** ✅
  - Datetime: `current_date`, `current_timestamp`, `date_add`, `date_sub`, `hour`, `minute`, `second`, `datediff`, `last_day`, `trunc`.
  - String: `repeat`, `reverse`, `instr`, `lpad`, `rpad`; fixtures `string_repeat_reverse`, `string_lpad_rpad`.
  - Math: `sqrt`, `pow`, `exp`, `log`; fixture `math_sqrt_pow`.
  - Conditional: `nvl`/`ifnull`, `nullif`, `nanvl`.
  - GroupedData: `first`, `last`, `approx_count_distinct`; fixture `groupby_first_last`.
  - DataFrame: `replace`, `cross_join`, `describe`, `cache`/`persist`/`unpersist`, `subtract`, `intersect`; fixtures `replace`, `cross_join`, `describe`, `subtract`, `intersect`.

- **Phase 10 – Complex types & window parity** (February 2026) ✅
  - **Window parity**: Fixtures `percent_rank_window`, `cume_dist_window`, `ntile_window`, `nth_value_window` now covered (multi-step workaround in harness); no longer skipped.
  - **String 6.4**: `mask`, `translate`, `substring_index` implemented; fixtures `string_mask`, `string_translate`, `string_substring_index`. **Phase 8**: `soundex`, `levenshtein`, `crc32`, `xxhash64` now **implemented** via map UDFs (strsim, crc32fast, twox-hash, soundex crates).
  - **Array extensions**: `array_exists`, `array_forall`, `array_filter`, `array_transform`, `array_sum`, `array_mean` (Polars `list_any_all`, `list_eval`); fixture `array_sum`. **Phase 8**: `array_flatten` and `array_repeat` now **implemented** via map UDFs.
  - **Map functions**: **Phase 8** – `create_map`, `map_keys`, `map_values`, `map_entries`, `map_from_arrays` now **implemented** (Map as `List(Struct{key, value})`; create_map via as_struct/concat_list; map_keys/map_values via list.eval + struct.field; map_from_arrays via UDF).
  - **JSON**: `get_json_object`, `from_json`, `to_json` (Polars `extract_jsonpath`, `dtype-struct`); fixture `json_get_json_object`.
  - **Parity**: 73 fixtures passing (was 68); ~120+ functions; **no remaining Phase 8 stubs** for array_repeat, array_flatten, map, or string 6.4 (soundex/levenshtein/crc32/xxhash64).

- **Phase 8 – Remaining parity completed** (February 2026) ✅
  - **array_repeat**: Implemented via `Expr::map` UDF (list `try_apply_amortized` + extend).
  - **array_flatten**: Implemented via `Expr::map` UDF (list-of-lists flatten per row).
  - **Map**: `create_map` (as_struct + concat_list), `map_keys`/`map_values` (list.eval + struct.field_by_name), `map_entries` (identity), `map_from_arrays` (zip UDF with list builder).
  - **String 6.4**: `soundex` (soundex crate), `levenshtein` (strsim), `crc32` (crc32fast), `xxhash64` (twox-hash) via `Expr::map` / `Expr::map_many` UDFs.
  - New module `src/udfs.rs` for execution-time UDFs used by these expressions.

- **Documentation and roadmap**:
  - [PYSPARK_DIFFERENCES.md](docs/PYSPARK_DIFFERENCES.md): Known divergences (window, SQL, Delta); Phase 8 stubs removed (all implemented). Linked from README and docs index.
  - FULL_BACKEND_ROADMAP Phase 8 marked completed; PARITY_STATUS, ROADMAP, FULL_BACKEND_ROADMAP, IMPLEMENTATION_STATUS, PYSPARK_DIFFERENCES, README, docs/README updated for Phase 8 completion and ~120+ functions.

- **Phase 11 – Parity scale and test conversion** ✅
  - **Parity harness**: Date, timestamp, and boolean column support in fixture input; `dtype_to_string` and `collect_to_simple_format` for Date, Datetime, Int8; `types_compatible` for date/timestamp/Int8.
  - **New fixtures** (73 → 80): `date_add_sub`, `datediff`, `datetime_hour_minute`, `string_soundex`, `string_levenshtein`, `string_crc32`, `string_xxhash64`.
  - **Expression parser**: soundex, levenshtein, crc32, xxhash64 in withColumn expressions.
  - **Converter**: Date/timestamp type mapping in [tests/convert_sparkless_fixtures.py](tests/convert_sparkless_fixtures.py).
  - **CI**: [.github/workflows/ci.yml](.github/workflows/ci.yml) runs format, clippy, audit, deny, and all tests (including `pyspark_parity_fixtures`); separate job for Python (PyO3) tests.
  - **Docs**: [TEST_CREATION_GUIDE.md](docs/TEST_CREATION_GUIDE.md) date/timestamp format; [SPARKLESS_PARITY_STATUS.md](docs/SPARKLESS_PARITY_STATUS.md) CI note; ROADMAP, FULL_BACKEND_ROADMAP, PARITY_STATUS updated.

- **Phase 12 – DataFrame methods parity** ✅
  - **Rust**: Implemented `freq_items`, `approx_quantile`, `crosstab`, `melt` (full implementations); `sample_by` (stratified sampling); Spark no-ops: `hint`, `is_local`, `input_files`, `same_semantics`, `semantic_hash`, `observe`, `with_watermark`. DataFrame methods count ~35 → ~55+.
  - **PyO3**: Exposed `random_split`, `summary`, `to_df`, `select_expr`, `col_regex`, `with_columns`, `with_columns_renamed`, `stat()` (returns `DataFrameStat` with `cov`/`corr`), `na()` (returns `DataFrameNa` with `fill`/`drop`), `to_pandas` (same as collect; for use with `pandas.DataFrame.from_records`). Registered `PyDataFrameStat` and `PyDataFrameNa` in the module.
  - **Parity**: Fixtures `first_row`, `head_n`, `offset_n` for first/head/offset operations.
  - **Docs**: [PYTHON_API.md](docs/PYTHON_API.md), [PARITY_STATUS.md](docs/PARITY_STATUS.md), [IMPLEMENTATION_STATUS.md](docs/IMPLEMENTATION_STATUS.md), [ROADMAP.md](docs/ROADMAP.md), [FULL_BACKEND_ROADMAP.md](docs/FULL_BACKEND_ROADMAP.md), README, and docs index updated for Phase 12.

- **Phase 13 – Functions batch 1 (string, binary, collection)** ✅ (partial)
  - **Rust**: String — `ascii`, `format_number`, `overlay`, `position`, `char`, `chr`; Base64 — `base64`, `unbase64` (base64 crate); Binary — `sha1`, `sha2(bit_length)`, `md5` (sha1, sha2, md5 crates; string in → hex out); Collection — `array_compact`. UDFs in `udfs.rs` for ascii, format_number, char, base64, unbase64, sha1, sha2, md5.
  - **PyO3**: Module-level `ascii`, `format_number`, `overlay`, `position`, `char`, `chr`, `base64`, `unbase64`, `sha1`, `sha2`, `md5`, `array_compact`; Column methods `ascii_`, `format_number`, `overlay`, `char_`, `chr_`, `base64_`, `unbase64_`, `sha1_`, `sha2_`, `md5_`, `array_compact`.
  - **Parity**: `parse_with_column_expr` extended for all new functions; fixtures `string_ascii`, `string_format_number` (82 fixtures total).
  - **Docs**: PARITY_STATUS, IMPLEMENTATION_STATUS, PYTHON_API updated for Phase 13.

- **Phase 14 – Functions batch 2 (math, datetime, type/conditional)** ✅
  - **Math**: `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `atan2(y, x)`, `degrees`, `radians`, `signum` (UDFs in `udfs.rs`; Polars has no trig on `Expr`).
  - **Datetime**: `quarter`, `weekofyear`/`week`, `dayofweek` (Sun=1..Sat=7), `dayofyear`; `add_months`, `months_between`, `next_day(day_of_week)` (chrono UDFs).
  - **Type/conditional**: `cast(column, type_name)` (strict), `try_cast(column, type_name)` (null on failure), `parse_type_name()`; `isnan(column)`; `greatest`/`least` over columns (UDFs for Float64/Int64/String).
  - **Parity**: Parser branches for all Phase 14 functions; fixtures `math_sin_cos`, `datetime_quarter_week` (84 fixtures total).
  - **PyO3**: Module-level and Column methods for sin, cos, tan, asin, acos, atan, atan2, degrees, radians, signum, quarter, weekofyear, dayofweek, dayofyear, add_months, months_between, next_day, cast, try_cast, isnan, greatest, least.
  - **Docs**: README, CHANGELOG, PARITY_STATUS, IMPLEMENTATION_STATUS, ROADMAP, PYTHON_API, FULL_BACKEND_ROADMAP, docs/README, QUICKSTART updated for Phase 14.

### Changed

- **Phase 14**: Math (sin, cos, tan, degrees, radians, signum, etc.), datetime (quarter, weekofyear, add_months, months_between, next_day), type/conditional (cast, try_cast, isnan, greatest, least); 84 parity fixtures; PyO3 bindings; all docs and README updated.
- **Phase 13**: String/binary/collection batch 1 (ascii, format_number, overlay, position, char, chr, base64, unbase64, sha1, sha2, md5, array_compact); 82 parity fixtures; PyO3 bindings; with_columns_renamed type fix in Python.
- **Phase 12**: DataFrame methods ~55+ (freq_items, approx_quantile, crosstab, melt, sample_by, no-ops); PyO3 stat/na/to_pandas, random_split, with_columns, etc.; parity fixtures first_row, head_n, offset_n; all docs and README updated.
- **Phase 11**: Parity fixtures 73 → 80; harness date/datetime/boolean support; CI workflow; converter date/timestamp mapping; docs updated.
- **Documentation**: README, ROADMAP, FULL_BACKEND_ROADMAP, MIGRATION_STATUS, COMPILATION_STATUS updated for Phase 8/10 completion; removed all "stubbed" references for array_repeat, array_flatten, Map, and string 6.4 (soundex, levenshtein, crc32, xxhash64).
- **Phase 8**: All four previously stubbed areas are now implemented: array_repeat, array_flatten, map functions (create_map, map_keys, map_values, map_entries, map_from_arrays), and string 6.4 (soundex, levenshtein, crc32, xxhash64). PYSPARK_DIFFERENCES no longer lists these as stubbed.
- **Phase 10**: Window fixtures (percent_rank, cume_dist, ntile, nth_value) documented as covered in PYSPARK_DIFFERENCES; `substring_index` fixed for negative count (no u32 underflow); `mask` uses `replace_all` for correct regex replacement.
- **PyO3 0.24**: Upgraded optional `pyo3` dependency from 0.22 to 0.24 (addresses RUSTSEC-2025-0020). Python bindings use non-deprecated APIs: `PyList::empty`, `PyDict::new`, `IntoPyObjectExt::into_bound_py_any` for collect.
- Parity harness now accepts optional `right_input` for multi-DataFrame fixtures
- Schema comparison allows Polars `_right` suffix for duplicate join column names
- `GroupedData::agg()` with multiple expressions now reorders columns to match PySpark (grouping cols first)

### Tooling

- Added `deny.toml` for cargo-deny (advisories, bans, sources; licenses need per-crate config)
- Updated Makefile with Rust targets: build, test, check, fmt, clippy, audit, outdated, deny, all

## [0.1.0] - (Initial release)

### Added

- PySpark-like DataFrame API built on Polars
- `SparkSession`, `DataFrame`, `GroupedData`, `Column`
- Operations: filter, select, orderBy, groupBy, withColumn, read_csv, read_parquet, read_json
- Expression functions: col, lit_*, when/then/otherwise, coalesce
- GroupedData aggregates: count, sum, avg, min, max, agg
- Parity test harness with fixture-based PySpark comparison

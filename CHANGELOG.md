# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Documentation and roadmap**:
  - [PYSPARK_DIFFERENCES.md](docs/PYSPARK_DIFFERENCES.md): Known divergences from PySpark (window fixtures skipped, SQL/Delta limits, Phase 8 deferred). Linked from README and docs index.
  - FULL_BACKEND_ROADMAP Phase 1: Marked completed items (dataframe split, case sensitivity, fixture converter); trait/expression doc left as Future.
  - ROADMAP §6: groupBy null/edge cases marked verified via existing fixtures; success metric "Documentation of differences" marked done.
- **Parity fixtures** (58 total, +2):
  - `regexp_like`: `regexp_like(col, pattern)` → boolean; parity parser support for withColumn.
  - `regexp_extract_all`: `regexp_extract_all(col, pattern)` → list of strings; parity parser support.
- **Datetime**: `to_date()` (cast to Date), `date_format(format)` (chrono strftime) in Rust API; no parity fixture yet (test harness does not build date/datetime from JSON).
- **Sparkless parity**: SPARKLESS_PARITY_STATUS "When Sparkless repo is available" steps for running converter and updating pass/fail table.

- **Phase 6 remaining (window + array)**:
  - **Window**: `cume_dist(partition_by, descending)`, `ntile(n, partition_by, descending)`, `nth_value(n, partition_by, descending)` in Rust and Python; parity handler for `first_value`, `last_value`, `percent_rank`, `cume_dist`, `ntile`, `nth_value`; fixtures `first_value_window`, `last_value_window` (passing); `percent_rank_window`, `cume_dist_window`, `ntile_window`, `nth_value_window` (skipped: Polars does not allow combining rank().over() and count().over() in one expr). Fixture simplification planned for Phase 8.
  - **Array**: `array_position`, `array_remove`, `posexplode` **implemented** in Rust and Python (via Polars list.eval with col("") as element; requires polars features list_eval, list_drop_nulls, cum_agg). `array_repeat` not implemented (→ Phase 8). String (6.4) soundex, levenshtein, etc. and Map/JSON documented as Phase 8.

- **Phase 7 SQL & Advanced**:
  - **SQL** (optional `sql` feature): `SparkSession::sql(query)` with temp views (`create_or_replace_temp_view`, `table`). Parses single SELECT with FROM/JOIN, WHERE, GROUP BY, ORDER BY, LIMIT and translates to DataFrame ops. Python: `spark.sql(query)`, `spark.create_or_replace_temp_view(name, df)`, `spark.table(name)`.
  - **Delta Lake** (optional `delta` feature): `read_delta(path)`, `read_delta_with_version(path, version)` (time travel), `write_delta(path, overwrite)` (overwrite/append). Python: `spark.read_delta(path)`, `spark.read_delta_version(path, version)`, `df.write_delta(path, overwrite)`.
  - **Performance**: Criterion benchmarks (`cargo bench`) for filter/select/groupBy (robin vs Polars). Target within ~2x for supported pipelines.
  - **Robustness**: Clearer error messages (column names, hints); Troubleshooting section in [docs/QUICKSTART.md](docs/QUICKSTART.md).

- **Phase 6 Broad Function Parity** (partial):
  - **Array functions**: `array_size`/`size`, `array_contains`, `element_at`, `explode`, `array_sort`, `array_join`, `array_slice`; **implemented** (Polars list.eval): `array_position`, `array_remove`, `posexplode`; parity fixtures: `array_contains`, `element_at`, `array_size`.
  - **Window extensions**: `first_value`, `last_value`, `percent_rank` with `.over(partition_by)`.
  - **String**: `regexp_extract_all`, `regexp_like`.
  - **PyO3**: New functions exposed on `PyColumn` (e.g. `size`, `element_at`, `explode`, `first_value`, `last_value`, `percent_rank`, `regexp_like`).
  - Map and JSON phases deferred (Polars MapType/JSON semantics; documented in [FULL_BACKEND_ROADMAP.md](docs/FULL_BACKEND_ROADMAP.md)).

- **Phase 5 Test Conversion**: Fixture converter and parity over converted fixtures.
  - Converter (`tests/convert_sparkless_fixtures.py`) maps Sparkless `expected_outputs` to robin-sparkless format: join, window, withColumn, union, unionByName, distinct, drop, dropna, fillna, limit, withColumnRenamed (in addition to filter, select, groupBy, orderBy).
  - Parity test discovers `tests/fixtures/*.json` and `tests/fixtures/converted/*.json`; optional `skip: true` / `skip_reason` in fixtures to skip known gaps.
  - `make sparkless-parity`: when `SPARKLESS_EXPECTED_OUTPUTS` is set, runs converter then `cargo test pyspark_parity_fixtures`; see [docs/CONVERTER_STATUS.md](docs/CONVERTER_STATUS.md) and [docs/SPARKLESS_PARITY_STATUS.md](docs/SPARKLESS_PARITY_STATUS.md).
  - 58 hand-written fixtures passing; target 50+ met.
- **Phase 4 PyO3 Bridge**: Optional Python bindings when built with `--features pyo3`.
  - Python module `robin_sparkless` with PySpark-like API: `SparkSession`, `SparkSessionBuilder`, `DataFrame`, `Column`, `GroupedData`, `WhenBuilder`, `ThenBuilder`.
  - Session: `builder()`, `get_or_create()`, `create_dataframe`, `read_csv`, `read_parquet`, `read_json`, `is_case_sensitive()`.
  - DataFrame: `filter`, `select`, `with_column`, `order_by`, `group_by`, `join`, `union`, `union_by_name`, `distinct`, `drop`, `dropna`, `fillna`, `limit`, `with_column_renamed`, `count`, `show`, `collect` (returns list of dicts).
  - Column/expressions: `col`, `lit`, `when().then().otherwise()`, `coalesce`, `sum`, `avg`, `min`, `max`, `count`; column methods `gt`, `ge`, `lt`, `le`, `eq`, `ne`, `and_`, `or_`, `alias`, `is_null`, `is_not_null`, `upper`, `lower`, `substr`.
  - GroupedData: `count()`, `sum(column)`, `avg(column)`, `min(column)`, `max(column)`, `agg(exprs)`.
  - Build/install: `maturin develop --features pyo3` or `maturin build --features pyo3`; `pyproject.toml` for maturin.
  - Python smoke tests in `tests/python/`; `make test` runs Rust + Python tests (creates `.venv`, installs extension, runs pytest).
  - API contract documented in [docs/PYTHON_API.md](docs/PYTHON_API.md).
- `DataFrame::join()` – Join two DataFrames on specified columns
- `JoinType` enum – Inner, Left, Right, Outer (exported from crate root)
- Parity test support for join fixtures via `right_input` and `Operation::Join`
- Four join parity fixtures: `inner_join`, `left_join`, `right_join`, `outer_join`
- **Multi-aggregation**: `GroupedData::agg()` supports multiple aggregations in one call; `groupby_multi_agg` fixture
- **Window functions**: `Column::rank()`, `row_number()`, `dense_rank()`, `lag()`, `lead()` with `.over(partition_by)`
- Parity support for `Operation::Window` with row_number, rank, dense_rank, lag, lead
- Window fixtures: `row_number_window`, `rank_window`, `lag_lead_window`
- **String functions**: `upper()`, `lower()`, `substring()` (1-based), `concat()`, `concat_ws()`
- String fixtures: `string_upper_lower`, `string_substring`, `string_concat`

### Changed

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

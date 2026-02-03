# Implementation Status: Polars Migration

## Strategic Direction: Sparkless Backend Replacement

Robin-sparkless is designed to **replace the backend logic** of [Sparkless](https://github.com/eddiethedean/sparkless)—the Python PySpark drop-in replacement. Sparkless would call robin-sparkless via PyO3/FFI for DataFrame execution. See [SPARKLESS_INTEGRATION_ANALYSIS.md](SPARKLESS_INTEGRATION_ANALYSIS.md) for architecture mapping, structural learnings, and test conversion strategy.

## ✅ Completed

### 1. Rust Core (default build)
- Default build is pure Rust (no Python). Library exposes a Rust API.
- Optional **PyO3 bridge**: `pyo3` feature adds Python bindings; see [PYTHON_API.md](PYTHON_API.md).

### 2. Polars Integration
- `DataFrame` wraps a Polars `DataFrame` internally.
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
   - ✅ `SparkSession::sql()` implemented (optional `sql` feature); temp views; see [QUICKSTART.md](QUICKSTART.md), [PYTHON_API.md](PYTHON_API.md).

6. **PyO3 Bridge** ✅ **COMPLETED** (Phase 4)
   - Optional `pyo3` feature (PyO3 0.24); `src/python/mod.rs` exposes `robin_sparkless` Python module.
   - SparkSession, DataFrame, Column, GroupedData; create_dataframe, filter, select, join, group_by, collect (list of dicts), read_csv/parquet/json, etc.
   - `maturin develop --features pyo3`; Python smoke tests in `tests/python/`; `make test` runs Rust + Python tests.
   - API contract: [PYTHON_API.md](PYTHON_API.md).

7. **Phase 5 Test Conversion** ✅ **COMPLETED**
   - Fixture converter maps Sparkless `expected_outputs` to robin-sparkless format (join, window, withColumn, union, distinct, drop, dropna, fillna, limit, withColumnRenamed, etc.).
   - Parity discovers `tests/fixtures/` and `tests/fixtures/converted/`; optional `skip: true` in fixtures.
  - `make sparkless-parity` (set `SPARKLESS_EXPECTED_OUTPUTS` to run converter first); 73 hand-written fixtures passing.
  - See [CONVERTER_STATUS.md](CONVERTER_STATUS.md), [SPARKLESS_PARITY_STATUS.md](SPARKLESS_PARITY_STATUS.md).

8. **Phase 6 Broad Function Parity** (partial) ✅
   - Array: `array_size`/`size`, `array_contains`, `element_at`, `explode`, `array_sort`, `array_join`, `array_slice`; **implemented** (via Polars list.eval): `array_position`, `array_remove`, `posexplode`, `array_exists`, `array_forall`, `array_filter`, `array_transform`, `array_sum`, `array_mean`; **Phase 8**: `array_repeat`, `array_flatten` **implemented** via map UDFs. Fixtures: `array_contains`, `element_at`, `array_size`, `array_sum`.
   - Window: `first_value`, `last_value`, `percent_rank`, `cume_dist`, `ntile`, `nth_value` with `.over()`; parity fixtures for all (percent_rank/cume_dist/ntile/nth_value via multi-step workaround).
   - String: `regexp_extract_all`, `regexp_like`; **Phase 10**: `mask`, `translate`, `substring_index`; **Phase 8**: `soundex`, `levenshtein`, `crc32`, `xxhash64` **implemented** via map UDFs.
   - **Phase 10**: JSON `get_json_object`, `from_json`, `to_json`. **Phase 8**: Map `create_map`, `map_keys`, `map_values`, `map_entries`, `map_from_arrays` **implemented**. See [PYSPARK_DIFFERENCES.md](PYSPARK_DIFFERENCES.md).

9. **Phase 7 SQL & Advanced** ✅ **COMPLETED**
   - Optional **SQL** (`sql` feature): `SparkSession::sql(query)`, temp views (`create_or_replace_temp_view`, `table`); sqlparser → DataFrame ops (SELECT, FROM, WHERE, JOIN, GROUP BY, ORDER BY, LIMIT).
   - Optional **Delta Lake** (`delta` feature): `read_delta`, `read_delta_with_version` (time travel), `write_delta` (overwrite/append) via delta-rs.
   - **Performance**: `cargo bench` (criterion) compares robin-sparkless vs Polars; target within ~2x. Error messages improved; Troubleshooting in [QUICKSTART.md](QUICKSTART.md).

10. **Sparkless integration** (in Sparkless repo)
   - Fixture converter: Sparkless `expected_outputs/` JSON → robin-sparkless fixtures
   - Structural alignment: service-style modules, trait-based backends, case sensitivity
   - Function parity: use [PYSPARK_FUNCTION_MATRIX](https://github.com/eddiethedean/sparkless/blob/main/PYSPARK_FUNCTION_MATRIX.md) as checklist

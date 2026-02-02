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
   - Implement (or explicitly defer) `SparkSession::sql()` with clear documentation.

6. **PyO3 Bridge** ✅ **COMPLETED** (Phase 4)
   - Optional `pyo3` feature; `src/python/mod.rs` exposes `robin_sparkless` Python module.
   - SparkSession, DataFrame, Column, GroupedData; create_dataframe, filter, select, join, group_by, collect (list of dicts), read_csv/parquet/json, etc.
   - `maturin develop --features pyo3`; Python smoke tests in `tests/python/`; `make test` runs Rust + Python tests.
   - API contract: [PYTHON_API.md](PYTHON_API.md).

7. **Sparkless integration** (in Sparkless repo)
   - Fixture converter: Sparkless `expected_outputs/` JSON → robin-sparkless fixtures
   - Structural alignment: service-style modules, trait-based backends, case sensitivity
   - Function parity: use [PYSPARK_FUNCTION_MATRIX](https://github.com/eddiethedean/sparkless/blob/main/PYSPARK_FUNCTION_MATRIX.md) as checklist

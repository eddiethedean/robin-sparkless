# Implementation Status: Polars Migration

## ✅ Completed

### 1. Rust-only Core
- Removed all PyO3 bindings and Python packaging.
- Removed DataFusion and Arrow-specific glue.
- Library now builds as a pure Rust crate exposing a Rust API.

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

3. **Joins**
   - Implement common join types (inner, left, right, outer) and compare behavior against PySpark.
   - Add parity fixtures for join edge cases (null keys, duplicate keys, column naming).

4. **Broader expression & function coverage**
   - Expand built-in functions (string/date/math) with explicit PySpark semantics.
   - Add additional type coercion and null-handling edge cases as fixtures.

5. **Window functions and SQL**
   - Add window functions parity slice.
   - Implement (or explicitly defer) `SparkSession::sql()` with clear documentation.

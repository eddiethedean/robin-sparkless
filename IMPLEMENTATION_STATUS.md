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

### 3. Session API Skeleton
- `SparkSession` and `SparkSessionBuilder` kept as a Rust-facing entry point.
- `DataFrameReader` still exists but IO helpers are currently stubs.

## ⚙️ In Progress / Planned (toward PySpark parity)

1. **PySpark-inspired API surface**
   - Clarify which PySpark methods we intend to emulate first.
   - Align naming and signatures (adapted to Rust) for `SparkSession`, `DataFrame`, `Column`.

2. **Behavioral Parity Slice**
   - Implement and verify a small but representative set of operations:
     - `createDataFrame` from simple Rust rows.
     - `select`, `filter`, `groupBy(...).count()`.
     - `show`, `collect`, `count`.
   - Compare behavior against PySpark on fixtures (schemas, nulls, types).

3. **IO and Schema**
   - Implement CSV/Parquet/JSON readers through Polars.
   - Aim for PySpark-like schema inference and option handling where feasible.

4. **Testing & Tooling**
   - Rust test suite that encodes PySpark vs Robin Sparkless parity expectations.
   - Scripts or notes for running equivalent PySpark pipelines for comparison.

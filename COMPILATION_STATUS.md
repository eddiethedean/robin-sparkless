# Compilation Status

## Current State

- `cargo check` passes for the Rust-only, Polars-backed implementation.
- There are no outstanding Rust compiler errors.
- `cargo test` passes (unit/integration/doc tests).

## Remaining Work (Non-Compiler)

- Core missing/partial areas are functional gaps rather than compiler issues:
  - Join operations (inner/left/right/outer)
  - Window functions
  - SQL (`SparkSession::sql()`)
  - Broader function coverage (string/date/math) and additional type coercion edge cases
  - Benchmarks/performance envelope work
- Documentation and examples should be kept in sync as the Rust API surface grows (see `ROADMAP.md` and `PARITY_STATUS.md`).

# Compilation Status

## Current State

- `cargo check` passes for the Rust-only, Polars-backed implementation.
- `cargo build --features pyo3` builds the Python extension (optional). `cargo build --features "pyo3,sql"` and `cargo build --features "pyo3,delta"` add SQL and Delta Lake support.
- There are no outstanding Rust compiler errors.
- `cargo test` passes (unit/integration/doc tests). `make test` runs Rust tests plus Python smoke tests (creates `.venv`, installs extension via maturin, runs `pytest tests/python/`). `make sparkless-parity` runs parity over hand-written and (if present) converted fixtures; set `SPARKLESS_EXPECTED_OUTPUTS` to convert from Sparkless first.

## Remaining Work (Non-Compiler)

- **Strategic direction**: Robin-sparkless will replace the backend of [Sparkless](https://github.com/eddiethedean/sparkless). See [SPARKLESS_INTEGRATION_ANALYSIS](SPARKLESS_INTEGRATION_ANALYSIS.md).
- **Optional features (implemented)**: SQL (`SparkSession::sql()`, temp views) and Delta Lake (`read_delta`, `write_delta`, time travel) are available when building with `--features sql` and `--features delta` respectively. Benchmarks: `cargo bench` (robin vs Polars).
- Remaining functional gaps (not compiler issues):
  - Broader function coverage: Phase 6 array_position, array_remove, posexplode **implemented** (Polars list.eval); cume_dist, ntile, nth_value fixtures covered (multi-step workaround). Phase 8 **completed**: array_repeat, array_flatten, Map, String 6.4 (soundex, levenshtein, crc32, xxhash64); JSON (get_json_object, from_json, to_json) implemented. Additional type coercion edge cases.
  - **Path to 100%**: ROADMAP Phases 16–27 — Phases 18–25 completed (~283 functions, 159 fixtures, plan interpreter, expression interpreter for all scalar functions, 3 plan fixtures). Phase 26 (publish Rust crate on crates.io), Phase 27 (Sparkless integration, 200+ tests). See [ROADMAP.md](ROADMAP.md) and [FULL_BACKEND_ROADMAP.md](FULL_BACKEND_ROADMAP.md).
- Documentation and examples should be kept in sync as the Rust API surface grows (see `ROADMAP.md` and `PARITY_STATUS.md`).

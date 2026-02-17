## Rust-first testing and parity

This project is now **Rust-first**. The core behavior of `SparkSession`,
`DataFrame`, `Column`, and related APIs is validated by Rust tests in
`tests/`:

- `dataframe_core.rs` – basic DataFrame creation, `filter`, `select`,
  column–column comparison semantics, etc.
- `groupby_orderby_core.rs` – `group_by` and `order_by` behavior using
  both string and `Column` arguments.
- `sql_core.rs` – `SparkSession::sql`, temp views, and basic DDL
  (e.g. `DROP TABLE` / `DROP VIEW`).
- `delta_core.rs` – `write_delta` / `read_delta_from_path` round‑trip
  tests when the `delta` feature is enabled.
- `lazy_backend.rs`, `error_handling.rs`, `parity.rs`, and other
  existing Rust tests.

### PySpark parity fixtures

PySpark is still used as an **external reference** via JSON fixtures in
`tests/fixtures/`, but it is **only** needed when (re)generating those
fixtures – not for normal Rust test runs:

- `tests/parity.rs` loads fixtures from `tests/fixtures/` (and, if
  present, `tests/fixtures/converted/`) and validates that
  `robin-sparkless` matches the recorded behavior.
- Python helper scripts such as `tests/convert_sparkless_fixtures.py`
  and `tests/regenerate_expected_from_pyspark.py` can be run manually
  – or via `make sparkless-parity` – to convert Sparkless
  `expected_outputs` and refresh fixtures from PySpark when behavior
  changes.

### Python bindings and CI

The previous Python package (PyO3 bindings, `pyproject.toml`, and
Python‑focused CI) has been removed from this repository. Any future
language bindings are expected to live out‑of‑tree and call the Rust
crate via FFI.

The only remaining Python code in this repo is **test tooling**:

- Parity fixture generation and regeneration scripts under `tests/`.

New behavior tests should be written in Rust, ideally:

- As focused unit or integration tests against `SparkSession` /
  `DataFrame` / `Column`, and
- Backed by PySpark‑derived fixtures when subtle semantics need to be
  preserved.

### Running tests

Common commands for day‑to‑day development:

- `make test` – run Rust tests (`cargo test`) including core, parity,
  and plan tests.
- `make check-full` – format check, Clippy, `cargo audit`, `cargo deny`,
  and Rust tests (what CI runs).
- `make test-parity-phase-a` … `make test-parity-phase-g` – run
  parity fixtures for a specific phase (see `docs/PARITY_STATUS.md` and
  `tests/fixtures/phase_manifest.json`).
- `make test-parity-phases` – run all parity phases (A–G).
- `make sparkless-parity` – when `SPARKLESS_EXPECTED_OUTPUTS` is set
  and PySpark/Java are available, convert Sparkless fixtures, regenerate
  expected results from PySpark, and run the Rust parity suite.


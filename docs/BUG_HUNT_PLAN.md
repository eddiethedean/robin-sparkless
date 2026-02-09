# Bug Hunt Plan

A repeatable plan for finding and fixing bugs in robin-sparkless.

## 1. Run the test suite

- **Rust**: `cargo test` — unit tests, integration tests (`error_handling`, `parity`), doc tests.
- **Python**: `make test-python` (or `make test`) — builds extension with `maturin develop --features "pyo3,sql,delta"`, runs `pytest tests/python/`.
- **Parity phases**: `make test-parity-phase-a` … `make test-parity-phase-g` or `make test-parity-phases`.

Fix any failing tests first; they often indicate regressions or environment issues.

## 2. Static analysis

- **Format**: `cargo fmt --check`
- **Clippy**: `cargo clippy -- -D warnings`
- **Audit**: `cargo audit`
- **Deny**: `cargo deny check advisories bans sources`
- **Python**: `make lint-python` (ruff format, ruff check, mypy)

Address all warnings. Treat clippy lints and mypy/ruff issues as potential bugs or tech debt.

## 3. Panic-prone patterns

Search the **library** code (not tests) for:

- **`.unwrap()`** — Can panic on `None`/`Err`. Prefer `?`, `unwrap_or`, or returning `Result`/`Option`.
- **`.expect("...")`** — Same; reserve for “impossible” invariants and document why.
- **`panic!` / `unreachable!()`** — Intentional API contracts (e.g. “at least one column”) are documented in `lib.rs`; ensure callers enforce preconditions.
- **Unchecked indexing** — e.g. `columns[0]`, `args[1]` in UDFs/plan code. Ensure arity/size is validated before use.

**Reviewed (Feb 2026):**

- **`src/sql/parser.rs`**: `stmts.into_iter().next().unwrap()` is safe — guarded by `stmts.len() != 1` return above.
- **`src/session.rs`**: `and_hms_opt(0,0,0).unwrap()` on a valid `NaiveDate` is safe (midnight always valid).
- **`src/schema.rs`**: Panics in `test_*` are test assertions, not production paths.
- **`src/python/dataframe.rs`**: `PyDataFrameWriter::build_writer` previously used `.expect()` on `RwLock` read; if the lock was poisoned (thread panicked while holding write), the Python process could abort. **Fixed**: use `.ok().map(|g| *g).unwrap_or(WriteMode::Overwrite)` (and same for `WriteFormat::Parquet`) so poisoned locks fall back to defaults instead of panicking.

## 4. Error handling at API boundaries

- **Public APIs** that can fail should return `Result<_, PolarsError>` (or appropriate error type) and use `?` internally.
- **Python bindings**: Convert Rust `Result` to `PyResult` and raise clear Python exceptions; avoid panics crossing the FFI boundary.
- Check `create_dataframe`, `read_*`, `filter`, `select`, joins, and SQL for consistent error propagation.

## 5. Python bindings (PyO3)

- **Type/None**: Ensure `Option<T>` and `None` are mapped correctly in both directions; edge cases in `py_any_to_expr`, `collect` output, and `create_dataframe` from Python sequences.
- **Exceptions**: Use `PyResult` and `PyErr`; avoid panics in code called from Python (e.g. writer lock handling — see above).
- Run `make test-python` and any Python-only parity/smoke tests.

## 6. Parity and known gaps

- **Parity status**: `docs/PARITY_STATUS.md` — 212 fixtures passing, 0 skipped (as of Feb 2026).
- **Known differences**: `docs/PYSPARK_DIFFERENCES.md`.
- **Deferred scope**: `docs/DEFERRED_SCOPE.md`.
- **Gap analysis**: `make gap-analysis` / `make gap-analysis-quick` — compare vs PySpark API; prioritize unimplemented or divergent behavior that is in scope and fixable.

Review any **skipped** or **expected_error** parity fixtures; ensure “expected error” is still correct and not a latent bug that should be fixed.

## 7. Fixes and tests

- For each bug: add or adjust a test that would have caught it (unit, integration, or parity fixture).
- Prefer returning `Result` over panicking in library code; keep panics only where documented (e.g. “requires at least one column” in `lib.rs`).
- After changes: run `cargo test`, `make test` (or `make test-python`), and relevant `make test-parity-phase-*`.

---

## Summary of fixes from initial run (Feb 2026)

1. **tests/parity.rs** — Unused variable warnings: `Err(e)` → `Err(_e)` in two `expect_error` branches.
2. **src/python/dataframe.rs** — `PyDataFrameWriter::build_writer`: replaced `.expect(...)` on `mode`/`format` `RwLock` reads with `.ok().map(|g| *g).unwrap_or(WriteMode::Overwrite)` / `WriteFormat::Parquet` so poisoned locks do not abort the process.

No new tests were required for these; existing tests cover behavior.

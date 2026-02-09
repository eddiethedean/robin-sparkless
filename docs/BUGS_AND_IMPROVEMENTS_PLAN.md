# Plan: Bugs and Improvements

A repeatable plan for finding **bugs** and **needed improvements** in robin-sparkless. Use this for periodic audits, before releases, or when onboarding.

**Related docs:** [PARITY_STATUS.md](PARITY_STATUS.md), [PYSPARK_DIFFERENCES.md](PYSPARK_DIFFERENCES.md), [GAP_ANALYSIS_SPARKLESS_3.28.md](GAP_ANALYSIS_SPARKLESS_3.28.md), [DEFERRED_SCOPE.md](DEFERRED_SCOPE.md).

---

## Part A: Finding bugs

### A1. Run the full test suite

| Step | Command | What it catches |
|------|---------|------------------|
| Rust unit + integration | `cargo test` | Regressions, doc tests |
| Parity (all phases) | `make test-parity-phases` or `cargo test pyspark_parity_fixtures` | Behavioral drift vs PySpark fixtures |
| Python | `make test-python` | PyO3 bindings, expression semantics |
| Full CI | `make check-full` | Format, clippy, audit, deny, Rust tests, Python lint, Python tests |

Fix any failing tests first; add a test for every bug you fix.

### A2. Static analysis (zero warnings)

| Tool | Command | Focus |
|------|---------|--------|
| Format | `cargo fmt --check` | Consistency |
| Clippy | `cargo clippy -- -D warnings` | Panics, unnecessary clones, wrong patterns |
| Audit | `cargo audit` | Known vulnerabilities |
| Deny | `cargo deny check advisories bans sources` | Policy (e.g. git sources) |
| Python | `make lint-python` (ruff + mypy) | Unused imports, types, style |

Treat new warnings as potential bugs or tech debt.

### A3. Panic- and crash-prone patterns

Search **library** code (exclude `tests/`, `examples/`, `benches/`):

- **`.unwrap()`** — Prefer `?`, `unwrap_or`, or return `Result`/`Option`.
- **`.expect("...")`** — Only for documented invariants; avoid in PyO3 paths (poisoned lock → abort).
- **`panic!` / `unreachable!()`** — Must match documented API contracts (e.g. in `lib.rs`).
- **Unchecked indexing** — e.g. `row[idx]`, `args[n]` in plan/udf code; validate length/arity first.

Past fixes include `PyDataFrameWriter` lock handling in [src/python/dataframe.rs](src/python/dataframe.rs) (poisoned lock fallback) and type-conversion functions in [src/functions.rs](src/functions.rs) (to_char, to_number, etc.) now returning `Result` instead of panicking.

### A4. Error handling at boundaries

- **Public Rust APIs**: Can fail → return `Result<_, PolarsError>` (or appropriate type); use `?` internally.
- **Python**: Rust `Result` → `PyResult`; raise clear Python exceptions; no panics across FFI.
- **Hot paths**: `create_dataframe`, `create_dataframe_from_rows`, `read_*`, `filter`, `select`, joins, `execute_plan`, SQL.

Check that invalid input (wrong schema, missing columns, bad plan JSON) produces clear errors, not panics.

### A5. Python bindings (PyO3)

- **Types**: `Option<T>`, `None`, and Python `None`/int/float/bool/str round-trip correctly in `collect()`, `create_dataframe`, `py_to_json_value`, plan execution.
- **Extraction order**: As in #182, check that we try the “expression” type before “string” (or other coercible type) where both are valid.
- **Locks**: Any `RwLock`/`Mutex` read in code called from Python should handle poisoned state (e.g. fallback instead of `.expect()`).

### A6. Parity and known gaps

- **Skipped fixtures**: In `tests/fixtures/*.json`, any `"skip": true` — confirm reason still valid and not a latent bug we should fix.
- **Expected-error fixtures**: `"expect_error": true` — ensure the harness still expects failure and the error type/message is still correct.
- **Plan fixtures**: `tests/fixtures/plans/*.json` — run `plan_parity_fixtures`; add tests for new plan ops when you add them.

### A7. Fuzz and edge cases (optional)

- **Empty inputs**: Empty DataFrame, empty schema, zero-row reads, `limit(0)`.
- **Nulls**: Null in partition keys, in join keys, in aggregates, in expressions (e.g. `when(null)`).
- **Large inputs**: Very wide schema, many partition keys, huge row count (if you care about OOM or perf).

---

## Part B: Finding needed improvements

### B1. API and parity gaps (in scope)

| Source | Doc / Command | What to do |
|--------|----------------|------------|
| Gap vs Sparkless 3.28 | [GAP_ANALYSIS_SPARKLESS_3.28.md](GAP_ANALYSIS_SPARKLESS_3.28.md) | List of functions/methods not yet in robin-sparkless; prioritize ones in scope (not in [DEFERRED_SCOPE.md](DEFERRED_SCOPE.md)). |
| Gap vs PySpark repo | `make gap-analysis` / `make gap-analysis-quick` | Produces/updates gap list; implement or document. |
| Missing vs PySpark | [ROBIN_SPARKLESS_MISSING.md](ROBIN_SPARKLESS_MISSING.md) | Canonical “missing vs PySpark” list; track closure. |
| Signature alignment | [SIGNATURE_ALIGNMENT_TASKS.md](SIGNATURE_ALIGNMENT_TASKS.md), [SIGNATURE_GAP_ANALYSIS.md](SIGNATURE_GAP_ANALYSIS.md) | Optional params, overloads, aliases (e.g. two-arg `when`). |

**Improvement types:** Implement missing function, add optional parameter, add alias, add fixture for existing behavior.

### B2. Behavioral differences (document or fix)

- **Doc**: [PYSPARK_DIFFERENCES.md](PYSPARK_DIFFERENCES.md) — every intentional or known divergence (stubs, JVM, crypto, createDataFrame, etc.).
- **Improvements:** Where we can match PySpark without breaking existing users, consider changing behavior and documenting; else keep doc up to date.

### B3. Performance

| Area | How to check | Improvement ideas |
|------|------------------|-------------------|
| Rust vs Polars | `cargo bench` (e.g. filter_select_groupby) | Keep within ~2x of raw Polars; reduce clones, unnecessary collects. |
| Python overhead | PyO3 call overhead, repeated collect/show | Batch operations, avoid round-trips where possible. |
| IO | Large CSV/Parquet/JSON read/write | Reader/writer options, streaming if needed. |

### B4. Code quality and maintainability

- **Duplication**: Same logic in Rust and Python (e.g. option handling); consider helpers or codegen.
- **Complexity**: Long functions in `plan/expr.rs`, `functions.rs`, or Python bindings; split or document.
- **Tests**: New behavior should have a unit, integration, or parity test; document in [TEST_CREATION_GUIDE.md](TEST_CREATION_GUIDE.md).
- **Docs**: Public API docs, `lib.rs` panic/behavior notes, and user-facing docs ([QUICKSTART.md](QUICKSTART.md), [PYTHON_API.md](PYTHON_API.md)) up to date.

### B5. Sparkless integration (Phase 27)

- **Fixture converter**: Sparkless expected_outputs → robin-sparkless fixture format ([SPARKLESS_INTEGRATION_ANALYSIS.md](SPARKLESS_INTEGRATION_ANALYSIS.md) §4).
- **Run Sparkless test suite** with Robin backend: find failing tests; fix bugs or add missing API so more tests pass.
- **Reported upstream**: [SPARKLESS_PARITY_ISSUES_REPORTED.md](SPARKLESS_PARITY_ISSUES_REPORTED.md) — when Sparkless fixes issues, re-run and adjust Robin if needed.

### B6. Roadmap and deferred scope

- **Roadmap**: [ROADMAP.md](ROADMAP.md), [FULL_BACKEND_ROADMAP.md](FULL_BACKEND_ROADMAP.md) — Phase 26 (publish), Phase 27 (Sparkless integration).
- **Deferred**: [DEFERRED_SCOPE.md](DEFERRED_SCOPE.md) — RDD, UDF, streaming, XML, sketch, etc. Improvements here are “out of scope” unless the project decides otherwise; document workarounds.

---

## Part C: Prioritization

1. **Bugs first**: Failing tests, panics in library code, wrong results in parity tests, PyO3 crashes.
2. **Then**: Error handling (clear errors instead of panics), static analysis clean (clippy, mypy, ruff).
3. **Then**: High-value API gaps (in-scope functions used by Sparkless or commonly requested).
4. **Then**: Performance, code quality, docs, and remaining gap closure.

Use **GitHub issues** to track bugs and improvements; tag with `bug`, `improvement`, `parity`, `sparkless`, etc.

---

## Quick checklist (run periodically)

- [ ] `make check-full` passes.
- [ ] No new `.unwrap()` / `.expect()` in library code without a comment or doc.
- [ ] Parity: no new skipped fixtures without a reason; expect_error fixtures still valid.
- [ ] [PYSPARK_DIFFERENCES.md](PYSPARK_DIFFERENCES.md) and [PARITY_STATUS.md](PARITY_STATUS.md) updated if behavior or coverage changed.
- [ ] Open bugs from GitHub triaged or fixed.
- [ ] Gap analysis (quick or full) run if you touched the API surface.

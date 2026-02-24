# AGENTS.md

## Cursor Cloud specific instructions

### Overview

Robin Sparkless is a Rust DataFrame library providing a PySpark-compatible API on top of Polars. It is a Cargo workspace with 4 Rust crates and a Python binding (`sparkless-native`). No external services (databases, Docker, etc.) are required.

### Key commands

All standard dev commands are documented in the `Makefile` and `README.md` Development section. Key references:

- **Build:** `cargo build --workspace --all-features`
- **Test:** `cargo test -p robin-sparkless -p robin-sparkless-core -p robin-sparkless-polars -p spark-sql-parser --all-features`
- **Lint (clippy):** `cargo clippy -p robin-sparkless -p robin-sparkless-core -p robin-sparkless-polars -p spark-sql-parser --all-features --all-targets -- -D warnings`
- **Format:** `cargo fmt` (check: `cargo fmt --check`)
- **Full check:** `make check` or `make check-full`
- **Per-crate check:** `make check-crate CRATE=<name>`

### Non-obvious caveats

1. **Python crate (`sparkless-native`) has pre-existing lint/fmt failures.** The `python/src/lib.rs` file has formatting and clippy issues (pyo3 `useless_conversion` warnings treated as errors). When running workspace-wide clippy or fmt, expect failures from this crate. Use the per-package `-p` flag to lint only the core Rust crates (as shown above).

2. **`cargo audit` reports a known pyo3 advisory (RUSTSEC-2025-0020).** This is tracked and expected; the `.cargo/audit.toml` already ignores some advisories. The pyo3 advisory is pre-existing.

3. **The Makefile overrides `CARGO_HOME` to `$(HOME)/.cargo`.** This avoids sandbox cache corruption. If you see unexpected caching behavior, check the `CARGO_HOME` env var.

4. **No `Cargo.lock` is checked in.** A fresh `cargo build` will resolve and lock dependencies. The `Cargo.toml` has `[patch.crates-io]` entries for `aead`, `aes`, `alloc-no-stdlib`, `alloc-stdlib`, and `anes` (two are vendored in `/workspace/vendor/`).

5. **Long build times.** The first full build takes ~2-3 minutes due to polars and its transitive dependencies. Incremental builds are fast. Use the `timeout: 600000` rule from `.cursor/rules/command-timeout.mdc` for `make check-full` and full test runs.

6. **Examples as quick validation.** Run `cargo run --example demo` for a quick smoke test of the library's core DataFrame functionality.

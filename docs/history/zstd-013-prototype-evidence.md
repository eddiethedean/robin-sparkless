---
search:
  exclude: true
---

> **Archive / historical record:** this page describes a past snapshot or a plan.
> Use the [current support contract](../SUPPORT_MATRIX.md) for release behavior.

This was checked before integration with the newer `codex/release-4.17` implementation. The prototype is superseded by the four direct Polars/Parquet dependency patches; it is not current candidate evidence.

# Historical zstd 0.13 compatibility prototype

Local working-tree evidence on 2026-10-26, macOS arm64 / Rust 1.93.1:

- `Cargo.lock` resolves every zstd 0.13 consumer through the unpublished
  `vendor/zstd` adapter to pinned upstream zstd 0.14.3, zstd-safe 8.2.0 and
  zstd-sys 2.1.2. `cargo tree --locked --offline --workspace --all-features -i
  zstd@0.14.3 --depth 2` confirms both Apache Parquet versions (57/58) and
  Polars Arrow/Parquet use that route. No vulnerable registry zstd 0.13.3 or
  zstd-safe 7.x implementation remains in the locked workspace graph.
- `cargo audit --no-fetch --json` passes against the cached 1,378-advisory
  database with zero non-ignored vulnerabilities and no warnings. The new
  RUSTSEC-2026-0391 exception describes the adapter's package-version false
  positive; no zstd-safe or zstd-sys advisory was added to the ignore list.
- `cargo deny --locked --offline --workspace --all-features check advisories
  bans sources` passes all three checks. `cargo fmt --check` passes.
- `cargo test --release --locked --offline -p robin-sparkless-polars --features
  sql --test zstd_context_regression` passes all three regressions, including the
  real Polars Parquet decompressor after a partial page read.
- `cargo test --release --locked --offline -p robin-sparkless-polars -p
  sparkless-server --features sql,delta --lib --tests` passes the combined backend
  unit/integration suites, the three new regressions, all 27 server unit tests,
  three control-protocol tests and the live Phase 1 HTTP test. These are local
  regression results against the current 4.15.3 workspace metadata, not a passed
  4.17 release gate.
- `cargo clippy --locked --offline --workspace --all-features --all-targets --
  -D warnings` passes, covering the Python extension, native server, optional
  Delta dependencies and both Apache Parquet versions.
- `cd python && ../.venv/bin/maturin sdist --out /private/tmp/sparkless-zstd-sdist`
  builds a source archive containing the adapter manifest, source and README as
  well as the root Cargo patch. An isolated extraction resolves the same fixed
  compression graph with `cargo metadata --offline`. Its first metadata run
  trims only the unused `sparkless-server` and `httpdate` lock entries; a pristine
  source archive therefore needs that lock refresh before using `--locked`.
- After that lock pruning, `cargo +1.93.1 check --locked --offline --manifest-path
  /private/tmp/sparkless-zstd-consumer-1aiiabd9/sparkless-4.15.3/python/Cargo.toml
  --target-dir /Volumes/T7/coding/robin-sparkless/target` passes with default Python
  features (including Delta). All first-party sources and the adapter resolve
  inside the extracted archive; only the build cache is shared with the checkout.
- The five publishable Rust archives pass
  `python3 scripts/verify_rust_package_archives.py`. The release workflow's
  packaging command explicitly excludes the private zstd workspace member.
  Normalized registry manifests still lose the root patch, so passing this
  archive-format check does not close the fixed-dependency distribution blocker.

The isolated pre-integration reproducer fails all three abandoned-encoder,
abandoned-decoder and partial-Polars-Parquet-page regressions with registry zstd
0.13.3, and passes them with the adapter. This establishes a behavioral fix rather
than an unused-API assumption. Full release gates remain open: root Cargo patches
do not propagate into downstream Rust builds, and fresh distributable dependency,
supported-platform, V31 candidate and V32 public-install evidence is still
required. Track closure in the [4.17 blocker table](../plans/sparkless-server/RELEASE_4_17.md).

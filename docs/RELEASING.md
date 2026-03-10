# Releasing robin-sparkless

This document describes how to cut a release and publish the crate to [crates.io](https://crates.io).

The repository is a **Cargo workspace** with members: `robin-sparkless` (root, main facade), `crates/robin-sparkless-core`, `crates/robin-sparkless-polars`, and `crates/spark-sql-parser`. The primary dependency for users is **robin-sparkless**; `robin-sparkless-core`, `robin-sparkless-polars`, and `spark-sql-parser` may be published for advanced or minimal-use cases. `make check` and CI build the whole workspace (`cargo build --workspace --all-features`, `cargo test --workspace --all-features`).

## Version pinning (CI and local)

So that local development and CI use the same toolchains and tools:

- **Rust**: [rust-toolchain.toml](https://github.com/eddiethedean/robin-sparkless/blob/main/rust-toolchain.toml) pins the Rust version (e.g. 1.93.1). CI uses the same `toolchain` value in [.github/workflows/ci.yml](https://github.com/eddiethedean/robin-sparkless/blob/main/.github/workflows/ci.yml) and [.github/workflows/release.yml](https://github.com/eddiethedean/robin-sparkless/blob/main/.github/workflows/release.yml).
- **cargo-nextest**: CI installs a fixed version (e.g. 0.9.92) in the workflow. Use the same version locally if you use nextest.

## Pre-release checklist (e.g. 4.0.0)

- [ ] **Versions** — Root, robin-sparkless-core, and robin-sparkless-polars `Cargo.toml` have the same `version` (e.g. `4.0.0`). Root `Cargo.toml` path deps use matching `version = "4.0"`. Python `python/pyproject.toml` and `python/Cargo.toml` version match if releasing Python (e.g. `4.0.0`).
- [ ] **CHANGELOG** — Add `[X.Y.Z] - YYYY-MM-DD` section with Added/Changed/Fixed; move Unreleased items or leave Unreleased for next.
- [ ] **README** — Rust install examples use the new version (e.g. `robin-sparkless = "4.0.0"`).
- [ ] **CI** — `make check-full` passes (format, clippy, audit, deny, Rust tests, Python lint). Push to a branch and confirm CI green.
- [ ] **Secrets** — GitHub repo has `CARGO_REGISTRY_TOKEN` (crates.io) and `PYPI_API_TOKEN` (PyPI) if publishing Python.
- [ ] **Tag** — After merge to `main`, `git tag vX.Y.Z` and `git push origin vX.Y.Z`; release workflow runs automatically.

## Prerequisites

- The repository must have a GitHub Actions secret named **`CARGO_REGISTRY_TOKEN`** set to a crates.io API token.
- Create a token at [crates.io/settings/tokens](https://crates.io/settings/tokens) (requires a crates.io account). Store it as a repo secret in GitHub under Settings → Secrets and variables → Actions.

## Release steps

1. **Bump the version** in all four `Cargo.toml` files so they stay in sync:
   - [Cargo.toml](https://github.com/eddiethedean/robin-sparkless/blob/main/Cargo.toml) (root, `robin-sparkless`)
   - [crates/robin-sparkless-core/Cargo.toml](https://github.com/eddiethedean/robin-sparkless/blob/main/crates/robin-sparkless-core/Cargo.toml)
   - [crates/robin-sparkless-polars/Cargo.toml](https://github.com/eddiethedean/robin-sparkless/blob/main/crates/robin-sparkless-polars/Cargo.toml)
   - [crates/spark-sql-parser/Cargo.toml](https://github.com/eddiethedean/robin-sparkless/blob/main/crates/spark-sql-parser/Cargo.toml)
   Use the same version for the three robin-sparkless crates (e.g. `4.0.0`). `spark-sql-parser` can use the same version or its own (e.g. `0.3.0`). Update the `version` in root and in the crates; if you publish the subcrates, also update the dependency version in root (e.g. `robin-sparkless-core = { version = "4.0", path = "..." }`, `robin-sparkless-polars = { version = "4.0", path = "..." }`). Commit and push to `main`.

2. **Create and push a tag** matching the version with a `v` prefix:
   ```bash
   git tag v4.0.0
   git push origin v4.0.0
   ```

3. **CI runs automatically** on the tag push (see [.github/workflows/release.yml](https://github.com/eddiethedean/robin-sparkless/blob/main/.github/workflows/release.yml)):
   - Format check, Clippy (workspace), `cargo audit`, `cargo deny`, build and tests (workspace), and `cargo doc --workspace` must pass.
   - Then the workflow publishes to crates.io **in dependency order**:
     1. `spark-sql-parser`
     2. `robin-sparkless-core`
     3. `robin-sparkless-polars`
     4. `robin-sparkless`

4. **Verify**: Check [crates.io/crates/robin-sparkless](https://crates.io/crates/robin-sparkless), [robin-sparkless-core](https://crates.io/crates/robin-sparkless-core), [robin-sparkless-polars](https://crates.io/crates/robin-sparkless-polars), [spark-sql-parser](https://crates.io/crates/spark-sql-parser), and their docs on docs.rs after the workflow completes.

## Version policy

- Tags must match the version in the root `Cargo.toml` (e.g. tag `v4.0.0` only when root and both crates have `version = "4.0.0"`).
- Do not re-tag or overwrite tags; crates.io does not allow republishing the same version.
- The three robin-sparkless crates are published with the same version number so that the main crate can depend on `robin-sparkless-core = "4.0"` and `robin-sparkless-polars = "4.0"` and resolve to the matching release. `spark-sql-parser` may use a separate version (e.g. `0.3.x`).

## Manual publish (optional)

If you need to publish from the repo without using the tag workflow (e.g. a one-off fix for one crate), use the same order so dependencies exist on crates.io:

```bash
cargo publish -p spark-sql-parser --token <CRATES_IO_TOKEN>
cargo publish -p robin-sparkless-core --token <CRATES_IO_TOKEN>
cargo publish -p robin-sparkless-polars --token <CRATES_IO_TOKEN>
cargo publish -p robin-sparkless --token <CRATES_IO_TOKEN>
```

Ensure the version in each crate’s `Cargo.toml` is bumped and that dependency `version` fields in root match the versions you are publishing.

## Optional: Other language bindings

Phase 26 leaves non-Rust bindings as optional. If you decide to add Python or other language bindings in the future, publish them from a separate repository that depends on this crate via FFI, with their own release workflows and package registries.

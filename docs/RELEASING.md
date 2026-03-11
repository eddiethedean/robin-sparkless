# Releasing robin-sparkless

This document describes how to cut a release and publish:

- Rust crates to [crates.io](https://crates.io) and
- The Python package **sparkless** (Sparkless 4.0) to [PyPI](https://pypi.org/project/sparkless/).

The repository is a **Cargo workspace** with members: `robin-sparkless` (root, main facade), `crates/robin-sparkless-core`, `crates/robin-sparkless-polars`, `crates/spark-sql-parser`, and the Python extension crate under `python/`. The primary Rust dependency for users is **robin-sparkless**; the subcrates may be published for advanced or minimal-use cases. `make check` and CI build the whole workspace (`cargo build --workspace --all-features`, `cargo test --workspace --all-features`).

## Version pinning (CI and local)

So that local development and CI use the same toolchains and tools:

- **Rust**: [rust-toolchain.toml](https://github.com/eddiethedean/robin-sparkless/blob/main/rust-toolchain.toml) pins the Rust version (e.g. 1.93.1). CI uses the same `toolchain` value in [.github/workflows/ci.yml](https://github.com/eddiethedean/robin-sparkless/blob/main/.github/workflows/ci.yml) and [.github/workflows/release.yml](https://github.com/eddiethedean/robin-sparkless/blob/main/.github/workflows/release.yml).
- **cargo-nextest**: CI installs a fixed version (e.g. 0.9.92) in the workflow. Use the same version locally if you use nextest.

## Pre-release checklist (e.g. 4.1.0)

- [ ] **Versions** — Root, robin-sparkless-core, and robin-sparkless-polars `Cargo.toml` have the same `version` (e.g. `4.1.0`). Root `Cargo.toml` path deps use matching `version = "4.1"`. Python `python/pyproject.toml` and `python/Cargo.toml` version match if releasing Python (e.g. `4.1.0`).
- [ ] **CHANGELOG** — Add `[X.Y.Z] - YYYY-MM-DD` section with Added/Changed/Fixed; move Unreleased items or leave Unreleased for next.
- [ ] **README** — Rust install examples use the new version (e.g. `robin-sparkless = "4.1.0"`).
- [ ] **CI** — `make check-full` passes (format, clippy, audit, deny, Rust tests, Python lint). Push to a branch and confirm CI green.
- [ ] **Secrets** — GitHub repo has `CARGO_REGISTRY_TOKEN` (crates.io) and `PYPI_API_TOKEN` (PyPI) if publishing Python.
- [ ] **Tag** — After merge to `main`, `git tag vX.Y.Z` and `git push origin vX.Y.Z`; release workflow runs automatically.

## Prerequisites

- The repository must have a GitHub Actions secret named **`CARGO_REGISTRY_TOKEN`** set to a crates.io API token.
- The repository must have a GitHub Actions secret named **`PYPI_API_TOKEN`** set to a PyPI API token for the `sparkless` project.
  - Create a token at [crates.io/settings/tokens](https://crates.io/settings/tokens) (requires a crates.io account). Store it as a repo secret in GitHub under Settings → Secrets and variables → Actions.
  - Create a PyPI token at [pypi.org/manage/account/token](https://pypi.org/manage/account/token/) and store it as `PYPI_API_TOKEN` under the same GitHub settings.

## Release steps

1. **Bump the version** in all Rust and Python manifests so they stay in sync:
   - [Cargo.toml](https://github.com/eddiethedean/robin-sparkless/blob/main/Cargo.toml) (root, `robin-sparkless`)
   - [crates/robin-sparkless-core/Cargo.toml](https://github.com/eddiethedean/robin-sparkless/blob/main/crates/robin-sparkless-core/Cargo.toml)
   - [crates/robin-sparkless-polars/Cargo.toml](https://github.com/eddiethedean/robin-sparkless/blob/main/crates/robin-sparkless-polars/Cargo.toml)
   - [crates/spark-sql-parser/Cargo.toml](https://github.com/eddiethedean/robin-sparkless/blob/main/crates/spark-sql-parser/Cargo.toml)
   - [python/pyproject.toml](https://github.com/eddiethedean/robin-sparkless/blob/main/python/pyproject.toml) (Python package metadata)
   - [python/Cargo.toml](https://github.com/eddiethedean/robin-sparkless/blob/main/python/Cargo.toml) (native extension crate)

   Use the same version for the three robin-sparkless crates and the Python package (e.g. `4.1.0`). `spark-sql-parser` can use the same version or its own (e.g. `0.3.0`). Update the `version` in root and the crates; if you publish the subcrates, also update the dependency version in root (e.g. `robin-sparkless-core = { version = "4.1", path = "..." }`, `robin-sparkless-polars = { version = "4.1", path = "..." }`). Commit and push to `main`.

2. **Create and push a tag** matching the version with a `v` prefix:

   ```bash
   git tag v4.0.0
   git push origin v4.0.0
   ```

3. **Release workflow runs automatically** on the tag push (see [.github/workflows/release.yml](https://github.com/eddiethedean/robin-sparkless/blob/main/.github/workflows/release.yml)):

   - Rust checks:
     - Format check, Clippy (workspace), `cargo audit`, `cargo deny`
     - Build and tests for the whole workspace
     - `cargo doc --workspace`
   - Crates.io publish (Rust) in dependency order:
     1. `spark-sql-parser`
     2. `robin-sparkless-core`
     3. `robin-sparkless-polars`
     4. `robin-sparkless`
   - Python checks:
     - Mypy over the Python sources
     - Build per-OS wheels for the native extension (Ubuntu, macOS, Windows)
     - Python smoke tests + fast pytest subset against the built wheel across a Python version/OS matrix
   - PyPI publish (Python):
     - `sparkless` wheels and sdist are published to PyPI for:
       - manylinux x86_64 + sdist
       - manylinux aarch64
       - musllinux x86_64
       - musllinux aarch64
       - macOS (x86_64-apple-darwin, aarch64-apple-darwin)
       - Windows (x86_64, aarch64)

4. **Verify**:
   - Rust:
     - [crates.io/crates/robin-sparkless](https://crates.io/crates/robin-sparkless)
     - [robin-sparkless-core](https://crates.io/crates/robin-sparkless-core)
     - [robin-sparkless-polars](https://crates.io/crates/robin-sparkless-polars)
     - [spark-sql-parser](https://crates.io/crates/spark-sql-parser)
     - Their docs on docs.rs after the workflow completes.
   - Python:
     - [pypi.org/project/sparkless](https://pypi.org/project/sparkless/)
     - `pip install sparkless==X.Y.Z` in a clean virtualenv and a quick `from sparkless.sql import SparkSession` smoke test.

## Version policy

- Tags must match the version in the root `Cargo.toml` (e.g. tag `v4.1.0` only when root and both crates have `version = "4.1.0"`).
- Do not re-tag or overwrite tags; crates.io does not allow republishing the same version.
- The three robin-sparkless crates are published with the same version number so that the main crate can depend on `robin-sparkless-core = "4.1"` and `robin-sparkless-polars = "4.1"` and resolve to the matching release. `spark-sql-parser` may use a separate version (e.g. `0.3.x`).

## Manual publish (optional)

If you need to publish from the repo without using the tag workflow (e.g. a one-off fix for one crate), use the same order so dependencies exist on crates.io:

```bash
cargo publish -p spark-sql-parser --token <CRATES_IO_TOKEN>
cargo publish -p robin-sparkless-core --token <CRATES_IO_TOKEN>
cargo publish -p robin-sparkless-polars --token <CRATES_IO_TOKEN>
cargo publish -p robin-sparkless --token <CRATES_IO_TOKEN>
```

Ensure the version in each crate’s `Cargo.toml` is bumped and that dependency `version` fields in root match the versions you are publishing.

## Notes on the Python package

- The Python package `sparkless` (4.0+) is published from this repository’s `python/` directory.
- Wheels are built using [maturin](https://github.com/PyO3/maturin) with a `pyo3`-based native extension crate (`sparkless-native`) that links against `robin-sparkless`.
- The package exposes `from sparkless.sql import SparkSession` and uses a private `sparkless._native` module for the Rust bindings. The legacy `sparkless_robin` module name is no longer used.

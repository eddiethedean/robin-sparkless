# Releasing robin-sparkless

This document describes how to cut a release and publish the crate to [crates.io](https://crates.io).

The repository is a **Cargo workspace** with members: `robin-sparkless` (root, main facade), `crates/robin-sparkless-core`, and `crates/robin-sparkless-expr`. The primary dependency for users is **robin-sparkless**; `robin-sparkless-core` and `robin-sparkless-expr` may be published for advanced or minimal-use cases. `make check` and CI build the whole workspace (`cargo build --workspace --all-features`, `cargo test --workspace --all-features`).

## Version pinning (CI and local)

So that local development and CI use the same toolchains and tools:

- **Rust**: [rust-toolchain.toml](../rust-toolchain.toml) pins the Rust version (e.g. 1.89.0). CI uses the same `toolchain` value in `.github/workflows/ci.yml` and `.github/workflows/release.yml`.
- **cargo-nextest**: CI installs a fixed version (e.g. 0.9.92) in the workflow. Use the same version locally if you use nextest.

## Prerequisites

- The repository must have a GitHub Actions secret named **`CARGO_REGISTRY_TOKEN`** set to a crates.io API token.
- Create a token at [crates.io/settings/tokens](https://crates.io/settings/tokens) (requires a crates.io account). Store it as a repo secret in GitHub under Settings → Secrets and variables → Actions.

## Release steps

1. **Bump the version** in [Cargo.toml](../Cargo.toml): set `version` to the new release (e.g. `0.2.0`, `0.3.0`). Commit and push to `main`.

2. **Create and push a tag** matching the version with a `v` prefix:
   ```bash
   git tag v0.3.0
   git push origin v0.3.0
   ```

3. **CI runs automatically** on the tag push (see [.github/workflows/release.yml](../.github/workflows/release.yml)):
   - Format check, Clippy, `cargo audit`, `cargo deny`, tests, and `cargo doc` must pass.
   - Then `cargo publish --token ${{ secrets.CARGO_REGISTRY_TOKEN }}` publishes the crate to crates.io.

4. **Verify**: Check [crates.io/crates/robin-sparkless](https://crates.io/crates/robin-sparkless) and [docs.rs/robin-sparkless](https://docs.rs/robin-sparkless) after the workflow completes.

## Version policy

- Tags must match the version in `Cargo.toml` (e.g. tag `v0.3.0` only when `version = "0.3.0"`).
- Do not re-tag or overwrite tags; crates.io does not allow republishing the same version.

## Optional: Other language bindings

Phase 26 leaves non-Rust bindings as optional. If you decide to add Python or other language bindings in the future, publish them from a separate repository that depends on this crate via FFI, with their own release workflows and package registries.

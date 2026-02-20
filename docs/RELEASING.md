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

1. **Bump the version** in all three `Cargo.toml` files so they stay in sync:
   - [Cargo.toml](../Cargo.toml) (root, `robin-sparkless`)
   - [crates/robin-sparkless-core/Cargo.toml](../crates/robin-sparkless-core/Cargo.toml)
   - [crates/robin-sparkless-expr/Cargo.toml](../crates/robin-sparkless-expr/Cargo.toml)
   Use the same version (e.g. `0.12.3`, `0.13.0`). Update the `version` in root and in both crates; if you publish the subcrates, also update the dependency version in root and in expr (e.g. `robin-sparkless-core = { version = "0.12", path = "..." }` → `"0.12.3"`). Commit and push to `main`.

2. **Create and push a tag** matching the version with a `v` prefix:
   ```bash
   git tag v0.12.3
   git push origin v0.12.3
   ```

3. **CI runs automatically** on the tag push (see [.github/workflows/release.yml](../.github/workflows/release.yml)):
   - Format check, Clippy (workspace), `cargo audit`, `cargo deny`, build and tests (workspace), and `cargo doc --workspace` must pass.
   - Then the workflow publishes to crates.io **in dependency order**:
     1. `robin-sparkless-core`
     2. `robin-sparkless-expr`
     3. `robin-sparkless`

4. **Verify**: Check [crates.io/crates/robin-sparkless](https://crates.io/crates/robin-sparkless), [robin-sparkless-core](https://crates.io/crates/robin-sparkless-core), [robin-sparkless-expr](https://crates.io/crates/robin-sparkless-expr), and their docs on docs.rs after the workflow completes.

## Version policy

- Tags must match the version in the root `Cargo.toml` (e.g. tag `v0.12.3` only when root and both crates have `version = "0.12.3"`).
- Do not re-tag or overwrite tags; crates.io does not allow republishing the same version.
- All three crates are published with the same version number so that the main crate can depend on `robin-sparkless-core = "0.12"` and `robin-sparkless-expr = "0.12"` and resolve to the matching release.

## Manual publish (optional)

If you need to publish from the repo without using the tag workflow (e.g. a one-off fix for one crate), use the same order so dependencies exist on crates.io:

```bash
cargo publish -p robin-sparkless-core --token <CRATES_IO_TOKEN>
cargo publish -p robin-sparkless-expr --token <CRATES_IO_TOKEN>
cargo publish -p robin-sparkless --token <CRATES_IO_TOKEN>
```

Ensure the version in each crate’s `Cargo.toml` is bumped and that dependency `version` fields in root and in `robin-sparkless-expr` match the version you are publishing.

## Optional: Other language bindings

Phase 26 leaves non-Rust bindings as optional. If you decide to add Python or other language bindings in the future, publish them from a separate repository that depends on this crate via FFI, with their own release workflows and package registries.

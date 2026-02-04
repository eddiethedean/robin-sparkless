# Releasing robin-sparkless

This document describes how to cut a release and publish the crate to [crates.io](https://crates.io).

## Prerequisites

- The repository must have a GitHub Actions secret named **`CARGO_REGISTRY_TOKEN`** set to a crates.io API token.
- Create a token at [crates.io/settings/tokens](https://crates.io/settings/tokens) (requires a crates.io account). Store it as a repo secret in GitHub under Settings → Secrets and variables → Actions.

## Release steps

1. **Bump the version** in [Cargo.toml](../Cargo.toml): set `version` to the new release (e.g. `0.1.0`, `0.2.0`). Commit and push to `main`.

2. **Create and push a tag** matching the version with a `v` prefix:
   ```bash
   git tag v0.1.0
   git push origin v0.1.0
   ```

3. **CI runs automatically** on the tag push (see [.github/workflows/release.yml](../.github/workflows/release.yml)):
   - Format check, Clippy, `cargo audit`, `cargo deny`, tests, and `cargo doc` must pass.
   - Then `cargo publish --token ${{ secrets.CARGO_REGISTRY_TOKEN }}` publishes the crate to crates.io.

4. **Verify**: Check [crates.io/crates/robin-sparkless](https://crates.io/crates/robin-sparkless) and [docs.rs/robin-sparkless](https://docs.rs/robin-sparkless) after the workflow completes.

## Version policy

- Tags must match the version in `Cargo.toml` (e.g. tag `v0.1.0` only when `version = "0.1.0"`).
- Do not re-tag or overwrite tags; crates.io does not allow republishing the same version.

## Optional: PyPI wheel

Phase 26 leaves PyPI publication as optional. To publish Python wheels via maturin in the future, add a step (or separate workflow) that runs `maturin publish --features pyo3` using a `PYPI_API_TOKEN` (or similar) secret after the crates.io publish step.

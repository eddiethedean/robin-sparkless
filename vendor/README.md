# Local Polars and Parquet dependency patches

These crates are unmodified upstream release sources except for the optional `zstd`
dependency requirement, updated from `0.13` to `0.14.3`. Polars 0.53 and Arrow
Parquet 57/58 still request zstd 0.13, whose dependency chain includes vulnerable
`zstd-safe` 7.2.4. The patch preserves zstd compression support while allowing Cargo
to resolve the fixed zstd 0.14.3 / zstd-safe 8.2.0 line.

The source and license files in each directory are from the corresponding crates.io
release: `polars-arrow` 0.53.0, `polars-parquet` 0.53.0, `parquet` 57.2.0, and
`parquet` 58.3.0. Keep source changes limited to dependency compatibility and
document the upstream versions when refreshing these copies.

Python source releases must use the PEP 517 backend in `python/_sparkless_build.py`;
it includes these patch sources and prepares the extracted workspace lockfile.
Direct `maturin sdist` omits the patch directories. Release CI checks a fresh
extraction with `scripts/verify_release_source.py`.

These root patches do not propagate to downstream crates.io consumers. Fixed
published dependencies or distributable forks remain a separate Rust release
requirement; see the 4.17 blocker table in the server release plan.

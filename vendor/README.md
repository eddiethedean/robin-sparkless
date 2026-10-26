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

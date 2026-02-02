# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `DataFrame::join()` – Join two DataFrames on specified columns
- `JoinType` enum – Inner, Left, Right, Outer (exported from crate root)
- Parity test support for join fixtures via `right_input` and `Operation::Join`
- Four join parity fixtures: `inner_join`, `left_join`, `right_join`, `outer_join`

### Changed

- Parity harness now accepts optional `right_input` for multi-DataFrame fixtures
- Schema comparison allows Polars `_right` suffix for duplicate join column names

### Tooling

- Added `deny.toml` for cargo-deny (advisories, bans, sources; licenses need per-crate config)
- Updated Makefile with Rust targets: build, test, check, fmt, clippy, audit, outdated, deny, all

## [0.1.0] - (Initial release)

### Added

- PySpark-like DataFrame API built on Polars
- `SparkSession`, `DataFrame`, `GroupedData`, `Column`
- Operations: filter, select, orderBy, groupBy, withColumn, read_csv, read_parquet, read_json
- Expression functions: col, lit_*, when/then/otherwise, coalesce
- GroupedData aggregates: count, sum, avg, min, max, agg
- Parity test harness with fixture-based PySpark comparison

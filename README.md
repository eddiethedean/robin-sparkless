# Robin Sparkless

A Rust DataFrame library that aims to **emulate PySpark’s DataFrame behavior and semantics without requiring the JVM**, using Polars as the execution engine under the hood.

**Long-term goal**: Robin-sparkless is designed to **replace the backend logic** of [Sparkless](https://github.com/eddiethedean/sparkless)—the Python PySpark drop-in replacement—so Sparkless can call robin-sparkless via PyO3/FFI for DataFrame execution. See [docs/SPARKLESS_INTEGRATION_ANALYSIS.md](docs/SPARKLESS_INTEGRATION_ANALYSIS.md) for architecture, structural learnings, and test conversion strategy.

## Features

- **PySpark-inspired API**: `SparkSession`, `DataFrame`, `Column` modeled after PySpark concepts
- **PySpark-like behavior**: Semantics (null handling, groupBy, joins, etc.) should match PySpark as closely as practical
- **Polars-backed**: Uses Polars `DataFrame`/`Expr` internally for performance
- **Pure Rust**: Default build has no Python runtime; optional **PyO3** feature for Python bindings
- **Optional SQL** (`--features sql`): `spark.sql("SELECT ...")` with temp views (`createOrReplaceTempView`, `table`); single SELECT, FROM/JOIN, WHERE, GROUP BY, ORDER BY, LIMIT
- **Optional Delta Lake** (`--features delta`): `read_delta` / `read_delta_with_version` (time travel), `write_delta` (overwrite/append) via delta-rs
- **Benchmarks**: `cargo bench` compares robin-sparkless vs plain Polars; target within ~2x for supported pipelines
- **Sparkless backend target**: Intended to power Sparkless's execution engine; aligns with its 403+ PySpark functions and 270+ test fixtures; **142 parity fixtures** passing (~265+ functions). **Phase 22 completed**: datetime extensions (curdate, now, localtimestamp, date_diff, dateadd, datepart, extract, unix_micros/millis/seconds, dayname, weekday, make_timestamp, timestampadd, timestampdiff, from_utc_timestamp, to_utc_timestamp, convert_timezone, current_timezone, to_timestamp, days, hours, months, years). **Phase 21 completed**: string (btrim, locate, conv), binary (hex, unhex, bin, getbit), type (to_char, to_varchar, to_number, try_to_number, try_to_timestamp), array (arrays_overlap, arrays_zip, explode_outer, posexplode_outer, array_agg), map (str_to_map), struct (transform_keys, transform_values). **Phase 20 completed**: ordering (asc, desc, nulls_first/last), aggregates (median, mode, stddev_pop, var_pop, try_sum, try_avg), numeric (bround, negate, positive, cot, csc, sec, e, pi). Phase 19: aggregates, try_*, misc. Phase 18: array/map/struct. Phase 17: datetime/unix, pmod, factorial. Phase 16: string/regex. Path to 100%: ROADMAP Phases 23–24 (full parity), Phase 25 (publish crate), Phase 26 (Sparkless integration).

## Installation

### Rust

Add to your `Cargo.toml`:

```toml
[dependencies]
robin-sparkless = "0.1.0"
```

### Python (PyO3 bridge, optional)

Build the Python extension with [maturin](https://www.maturin.rs/) (requires Rust and Python 3.8+):

```bash
# From the repo root
pip install maturin
maturin develop --features pyo3   # Editable install
# or build a wheel:
maturin build --features pyo3
pip install target/wheels/robin_sparkless-*.whl
```

Then use the `robin_sparkless` module (see [docs/PYTHON_API.md](docs/PYTHON_API.md) for the full API contract):

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("test").get_or_create()
df = spark.create_dataframe([(1, 25, "Alice"), (2, 30, "Bob")], ["id", "age", "name"])
filtered = df.filter(rs.col("age").gt(rs.lit(26)))
print(filtered.collect())  # [{"id": 2, "age": 30, "name": "Bob"}]
```

## Quick Start

At the moment, `robin-sparkless` exposes a small Rust API and uses Polars as its execution engine. The long-term goal is to make PySpark-style code patterns natural to express in Rust.

```rust
use polars::prelude::*;
use robin_sparkless::{DataFrame, Column, SparkSession};

// In the future, most users should start from a SparkSession, like PySpark:
// let spark = SparkSession::builder().app_name("my_app").get_or_create();
// let df = spark.create_dataframe(data, schema);
//
// For now, we start from a Polars DataFrame:

let polars_df = df!(
    "id" => &[1, 2, 3, 4],
    "age" => &[25, 35, 45, 55],
    "score" => &[50, 60, 110, 70],
)?;

let df = DataFrame::from_polars(polars_df);

// Introspect (PySpark-style: df.schema, df.columns)
let schema = df.schema()?;
let columns = df.columns()?;

// Build expressions using a Column wrapper (similar to PySpark Column)
let age_col: Column = df.column("age")?;
let adults = df.filter(age_col.expr().clone().gt(lit(30)))?;

// More complex boolean logic, similar to PySpark:
// df.filter((col("age") > 30) & ((col("score") < 100) | (col("age") > 50)))
let complex = df.filter(
    (df.column("age")?.expr().clone().gt(lit(30)))
        .and(
            df.column("score")?.expr().clone().lt(lit(100))
                .or(df.column("age")?.expr().clone().gt(lit(50))),
        ),
)?;

// Complex expressions in withColumn (arithmetic and logical):
use robin_sparkless::{col, lit_i64};
let df_with_computed = df.with_column(
    "above_threshold",
    (col("age").into_expr() + col("score").into_expr()).gt(lit(100))
)?;

adults.show(Some(10))?;
complex.show(Some(10))?;
df_with_computed.show(Some(10))?;
```

The library supports IO (CSV/Parquet/JSON via `SparkSession::read_*`), joins (`DataFrame::join` with Inner/Left/Right/Outer), groupBy aggregates (including multi-agg), window functions (`row_number`, `rank`, `dense_rank`, `lag`, `lead`, `first_value`, `last_value`, `percent_rank`, `cume_dist`, `ntile`, `nth_value` with `.over()`), array functions (`array_size`, `array_contains`, `element_at`, `explode`, `explode_outer`, `array_sort`, `array_join`, `array_slice`, `array_position`, `array_remove`, `posexplode`, `posexplode_outer`, `array_exists`, `array_forall`, `array_filter`, `array_transform`, `array_sum`, `array_mean`, `array_repeat`, `array_flatten`, `array_compact`, `arrays_overlap`, `arrays_zip`, `array_agg`), string functions (`upper`, `lower`, `substring`, `concat`, `concat_ws`, `length`, `trim`, `btrim`, `locate`, `conv`, `regexp_extract`, `regexp_replace`, `regexp_extract_all`, `regexp_like`, `split`, `mask`, `translate`, `substring_index`, `soundex`, `levenshtein`, `crc32`, `xxhash64`; Phase 13: `ascii`, `format_number`, `overlay`, `position`, `char`, `chr`, `base64`, `unbase64`, `sha1`, `sha2`, `md5`; Phase 16: `regexp_count`, `regexp_instr`, `regexp_substr`, `split_part`, `find_in_set`, `format_string`, `printf`; Phase 21: `btrim`, `locate`, `conv`), JSON (`get_json_object`, `from_json`, `to_json`), Map (`create_map`, `map_keys`, `map_values`, `map_entries`, `map_from_arrays`, `str_to_map`), **math** (Phase 14: `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `atan2`, `degrees`, `radians`, `signum`; plus `sqrt`, `pow`, `exp`, `log`; Phase 17: `pmod`, `factorial`), **datetime** (`year`, `month`, `day`, `to_date`, `date_format`, `current_date`, `date_add`, `date_sub`, `datediff`, `last_day`, `trunc`, `hour`, `minute`, `second`; Phase 14: `quarter`, `weekofyear`, `dayofweek`, `dayofyear`, `add_months`, `months_between`, `next_day`; Phase 17: `unix_timestamp`, `from_unixtime`, `make_date`, `timestamp_seconds`, `timestamp_millis`, `timestamp_micros`, `unix_date`, `date_from_unix_date`), and **type/conditional** (Phase 14: `cast`, `try_cast`, `isnan`, `greatest`, `least`; plus `when`/`then`/`otherwise`, `coalesce`, `nvl`, `nullif`, `nanvl`; Phase 19: `try_divide`, `try_add`, `try_subtract`, `try_multiply`, `width_bucket`, `elt`, `bit_length`, `typeof`; Phase 21: `to_char`, `to_varchar`, `to_number`, `try_to_number`, `try_to_timestamp`). **Binary** (Phase 21: `hex`, `unhex`, `bin`, `getbit`). See [docs/QUICKSTART.md](docs/QUICKSTART.md) for more examples.

## Development

### Prerequisites

- Rust (latest stable)

### Building

```bash
# Build the project (Rust only)
cargo build

# Build with Python extension (optional)
cargo build --features pyo3

# Optional: SQL support and/or Delta Lake
cargo build --features "pyo3,sql"       # SQL: spark.sql(), temp views
cargo build --features "pyo3,delta"      # Delta: read_delta, write_delta
cargo build --features "pyo3,sql,delta"  # All optional features

# Run tests (Rust only; PyO3 code is behind the feature)
cargo test

# Run all tests (Rust + Python; creates .venv and installs extension for Python tests)
make test

# Run all checks (format, clippy, audit, deny, tests)
make check
# Or: RUSTUP_TOOLCHAIN=stable make check
# CI (GitHub Actions) runs the same checks on push/PR; see .github/workflows/ci.yml.

# Benchmarks (robin-sparkless vs Polars)
cargo bench

# Sparkless parity (optional: convert from Sparkless expected_outputs, then run parity)
# export SPARKLESS_EXPECTED_OUTPUTS=/path/to/sparkless/tests/expected_outputs
# make sparkless-parity

# Build documentation
cargo doc --open
```

### Python tests

```bash
make test        # Runs Rust tests, then Python tests (creates .venv, maturin develop --features pyo3, pytest)
make test-python # Python tests only
```

Or manually (with virtualenv activated): `maturin develop --features pyo3` then `pytest tests/python/`.

## Architecture

Robin Sparkless aims to provide a **PySpark-like API layer** on top of Polars:

- **SparkSession**: Entry point for creating `DataFrame`s and reading data sources (CSV, Parquet, JSON); optional **SQL** (`sql()`, temp views) and **Delta** (`read_delta`, `read_delta_with_version`) when features are enabled.
- **DataFrame**: Main tabular data structure; operations include `filter`, `select`, `order_by`, `group_by`, `with_column`, `join`; optional `write_delta` when the `delta` feature is enabled.
- **Column**: Represents expressions over columns, similar to PySpark’s `Column`; includes window methods (`rank`, `row_number`, `dense_rank`, `lag`, `lead`) with `.over()`.
- **Functions**: Helper functions like `col()`, `lit_*()`, `count()`, `when`, `coalesce`, window functions, etc., modeled after PySpark’s `pyspark.sql.functions`.

Core behavior (null handling, grouping semantics, joins, window functions, array and string functions, JSON, Map, math, datetime, type/conditional, expression behavior) matches PySpark on **142 parity fixtures** (~265+ functions). Phase 22 (completed): datetime extensions (curdate, now, localtimestamp, date_diff, dateadd, datepart, extract, unix_micros/millis/seconds, dayname, weekday, make_timestamp, timestampadd, timestampdiff, from_utc_timestamp, to_utc_timestamp, etc.). Phase 21: string (btrim, locate, conv), binary (hex, unhex, bin, getbit), type (to_char, to_varchar, to_number, try_to_number, try_to_timestamp), array (arrays_overlap, arrays_zip, explode_outer, posexplode_outer, array_agg), map (str_to_map), struct (transform_keys, transform_values). Phase 20: ordering, aggregates (median, mode, stddev_pop, var_pop, try_sum, try_avg), numeric (bround, negate, positive, cot, csc, sec, e, pi). Phase 19: aggregates, try_*, misc. Phase 18: array/map/struct. Phase 17: datetime/unix, pmod, factorial. Phase 16: string/regex. Phase 15: aliases, string, math, array_distinct. Remaining: ROADMAP Phases 23–24 (full parity), Phase 25 (publish crate), Phase 26 (Sparkless integration). Python bindings expose the full set via the `robin_sparkless` module; see [docs/PYTHON_API.md](docs/PYTHON_API.md). Known divergences are documented in [docs/PYSPARK_DIFFERENCES.md](docs/PYSPARK_DIFFERENCES.md).

## Related Documentation

- [docs/](docs/README.md) – Documentation index
- [CHANGELOG.md](CHANGELOG.md) – Version history and release notes
- [docs/SPARKLESS_INTEGRATION_ANALYSIS.md](docs/SPARKLESS_INTEGRATION_ANALYSIS.md) – Sparkless backend replacement strategy, architecture learnings, test conversion
- [docs/ROADMAP.md](docs/ROADMAP.md) – Development roadmap; Phases 12–21 completed; Phases 22–24 (full parity), Phase 25 (crate publish), Phase 26 (Sparkless integration)
- [docs/FULL_BACKEND_ROADMAP.md](docs/FULL_BACKEND_ROADMAP.md) – Phased plan to full Sparkless backend replacement (400+ functions, PyO3 bridge)
- [docs/PYTHON_API.md](docs/PYTHON_API.md) – Python API contract (Phase 4 PyO3 bridge): build, install, method signatures, data transfer
- [docs/PARITY_STATUS.md](docs/PARITY_STATUS.md) – PySpark parity coverage matrix (142 fixtures)
- [docs/PYSPARK_DIFFERENCES.md](docs/PYSPARK_DIFFERENCES.md) – Known divergences from PySpark (window, SQL, Delta; Phase 8 completed)
- [docs/CONVERTER_STATUS.md](docs/CONVERTER_STATUS.md) – Sparkless → robin-sparkless fixture converter and operation mapping
- [docs/SPARKLESS_PARITY_STATUS.md](docs/SPARKLESS_PARITY_STATUS.md) – Phase 5: pass/fail and failure reasons for converted fixtures; `make sparkless-parity`

## License

MIT

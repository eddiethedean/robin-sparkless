# Quick Start Guide

Robin-sparkless is a PySpark-like DataFrame library in Rust, with Polars as the engine. Long-term, it is designed to replace the backend of [Sparkless](https://github.com/eddiethedean/sparkless). See [SPARKLESS_INTEGRATION_ANALYSIS.md](SPARKLESS_INTEGRATION_ANALYSIS.md) for integration details.

## Building the Project

### Prerequisites
- Rust (latest stable)

### Build (Rust only)

```bash
cargo build
```

**Delta Lake** (optional `delta` feature): No extra runtime dependencies. Build with `--features delta` on the Rust crate. Then use `spark.read_delta(path)`, `spark.read_delta_version(path, version)`, and `df.write_delta(path, overwrite)`. With the `sql` feature, `read_delta(name_or_path)` also accepts a **table name**: if the argument is not path-like, it resolves like `table(name)` (temp view first, then saved table), so you can `df.write_delta_table("t")` then `spark.read_delta("t")` without writing to disk.

### Add as a dependency

In your own crate:

```toml
[dependencies]
robin-sparkless = "0.11.6"
```

## Basic Usage

The current API is intentionally small and focused on wrapping Polars types. **DataFrame uses lazy evaluation** (#438): transformations (filter, select, join, etc.) extend the plan; only actions (`collect`, `show`, `count`, `write`) trigger execution. This enables Polars query optimization across the full pipeline.

```rust
use polars::prelude::*;
use robin_sparkless::{col, lit_i64, DataFrame};

fn main() -> polars::prelude::PolarsResult<()> {
    // Build a Polars DataFrame
    let polars_df = df!(
        "id" => &[1, 2, 3],
        "age" => &[25, 30, 35],
        "name" => &["Alice", "Bob", "Charlie"],
    )?;

    // Wrap it (stored as lazy internally; no materialization until an action)
    let df = DataFrame::from_polars(polars_df);

    // Introspect
    println!("{:?}", df.columns()?);

    // Filter using col() and lit_i64 (Expr for filter: use .into_expr() on Column)
    let adults = df.filter(col("age").gt(lit_i64(18).into_expr()).into_expr())?;

    adults.show(Some(10))?;

    Ok(())
}
```

Example output:

```
["id", "age", "name"]
shape: (3, 3)
┌─────┬─────┬─────────┐
│ id  ┆ age ┆ name    │
│ --- ┆ --- ┆ ---     │
│ i32 ┆ i32 ┆ str     │
╞═════╪═════╪═════════╡
│ 1   ┆ 25  ┆ Alice   │
│ 2   ┆ 30  ┆ Bob     │
│ 3   ┆ 35  ┆ Charlie │
└─────┴─────┴─────────┘
```

### Joins

```rust
use robin_sparkless::{DataFrame, JoinType};

let joined = left_df.join(&right_df, vec!["dept_id"], JoinType::Inner)?;
```

Supported join types: `Inner`, `Left`, `Right`, `Outer`.

### Window Functions

```rust
use robin_sparkless::{col, DataFrame};

// with_column(name, &Column) — use Column from col(), window methods, etc.
// row_number, rank, dense_rank over partition
let df_with_rn = df.with_column("rn", &col("salary").row_number(true).over(&["dept"]))?;

// lag and lead
let df_with_lag = df.with_column("prev_salary", &col("salary").lag(1).over(&["dept"]))?;
```

Supported window functions: `row_number()`, `rank()`, `dense_rank()`, `lag(n)`, `lead(n)` — each used with `.over(&["col1", "col2"])` for partitioning. Aggregations (e.g. `sum(col("x")).over(&["dept"])`) are also supported. Use **`with_column(name, &Column)`** for any `Column` (e.g. from `col()`, `rand(42)`); use **`with_column_expr(name, expr)`** when you have a raw Polars `Expr`.

### String Functions

```rust
use robin_sparkless::{col, concat, concat_ws, DataFrame};

// Column methods: use with_column(name, &Column)
let df = df.with_column("name_upper", &col("name").upper())?;
let df = df.with_column("prefix", &col("name").substr(1, Some(3)))?;

// concat/concat_ws return expressions — use with_column_expr when you have an Expr
let df = df.with_column_expr("full", concat(&[&col("first"), &col("last")]).into_expr())?;
let df = df.with_column_expr("joined", concat_ws("-", &[&col("a"), &col("b")]).into_expr())?;
```

You can also combine string/regex functions with **select expressions** using
`DataFrame::select_exprs`:

```rust
use robin_sparkless::{col, regexp_extract_all, DataFrame};

let expr = regexp_extract_all(&col("s"), r"\\d+").alias("m").into_expr();
let df2 = df.select_exprs(vec![expr])?;
```

### Random (rand, randn)

Use **`with_column`** or **`with_columns`** so each row gets a distinct value (PySpark-like). Optional seed gives reproducible results.

```rust
use robin_sparkless::{rand, randn, DataFrame};

let df = df.with_column("r", &rand(Some(42)))?;   // uniform [0, 1), one value per row
let df = df.with_column("z", &randn(Some(42)))?;  // standard normal, one value per row
```

Additional string: `length`, `trim`, `ltrim`, `rtrim`, `btrim`, `locate`, `conv`, `regexp_extract`, `regexp_replace`, `regexp_extract_all`, `regexp_like`, `split`, `initcap`, `mask`, `translate`, `substring_index` (Phase 10); **Phase 16**: `regexp_count`, `regexp_instr`, `regexp_substr`, `split_part`, `find_in_set`, `format_string`, `printf`; **Phase 21**: `btrim`, `locate`, `conv`. DataFrame methods (Phase 12): `sample`, `random_split`, `first`, `head`, `tail`, `take`, `is_empty`, `to_json`, `to_pandas`, `explain`, `print_schema`, `summary`, `to_df`, `select_expr`, `col_regex`, `with_columns`, `with_columns_renamed`, `stat()` (cov/corr), `na()` (fill/drop), `freq_items`, `approx_quantile`, `crosstab`, `melt`, `sample_by`, etc.

### Datetime

For date/datetime columns: `year()`, `month()`, `day()`, `to_date()` (cast to date), `date_format(format)` (chrono strftime, e.g. `"%Y-%m-%d"`), `hour()`, `minute()`, `second()`, `date_add(n)`, `date_sub(n)`, `datediff(end, start)`, `last_day()`, `trunc(format)`. **Phase 14**: `quarter()`, `weekofyear()`/`week()`, `dayofweek()` (1=Sun..7=Sat), `dayofyear()`, `add_months(n)`, `months_between(end, start)`, `next_day(day_of_week)` (e.g. `"Mon"`, `"Tue"`). **Phase 17**: `unix_timestamp()`, `unix_timestamp(col, format)`, `from_unixtime(col, format)`, `make_date(year, month, day)`, `timestamp_seconds(col)`, `timestamp_millis(col)`, `timestamp_micros(col)`, `unix_date(col)`, `date_from_unix_date(col)`. **Phase 22**: `curdate()`, `now()`, `localtimestamp()`, `date_diff(end, start)`, `dateadd(col, n)`, `datepart(col, field)`, `extract(col, field)`, `date_part(col, field)`, `unix_micros(col)`, `unix_millis(col)`, `unix_seconds(col)`, `dayname(col)`, `weekday(col)` (Mon=0..Sun=6), `make_timestamp(y, m, d, h, mi, s)`, `make_interval`, `timestampadd(unit, amount, ts)`, `timestampdiff(unit, start, end)`, `days(n)`, `hours(n)`, `minutes(n)`, `months(n)`, `years(n)`, `from_utc_timestamp(col, tz)`, `to_utc_timestamp(col, tz)`, `convert_timezone`, `current_timezone()`, `to_timestamp(col)`.

### Math and type (Phase 14)

Math (radians): `sin()`, `cos()`, `tan()`, `asin()`, `acos()`, `atan()`, `atan2(y, x)`, `degrees()`, `radians()`, `signum()` (plus existing `sqrt`, `pow`, `exp`, `log`). **Phase 17**: `pmod(dividend, divisor)`, `factorial(n)`. **Phase 20**: `bround(column, scale)`, `negate`, `positive`, `cot`, `csc`, `sec`, `e`, `pi`. Type: `cast(column, type_name)` (strict; use type names like `"int"`, `"long"`, `"double"`, `"string"`, `"date"`, `"timestamp"`), `try_cast(column, type_name)` (null on failure), `isnan(column)`; conditional: `greatest(columns...)`, `least(columns...)`. **Phase 21**: `to_char(column)`, `to_varchar(column)`, `to_number(column)`, `try_to_number(column)`, `try_to_timestamp(column)`. Binary: `hex(column)`, `unhex(column)`, `bin(column)`, `getbit(column, pos)`.

### Parity

Behavior is validated against PySpark on **159 parity fixtures** (~283+ functions); see [PARITY_STATUS.md](PARITY_STATUS.md). Known differences are in [PYSPARK_DIFFERENCES.md](PYSPARK_DIFFERENCES.md). CI (GitHub Actions) runs format, clippy, audit, deny, Rust tests, and parity tests on every push/PR.

For roadmap and Sparkless integration phases (Phases 12–22 completed; Phases 23–24 remaining), see [ROADMAP.md](ROADMAP.md).

## Troubleshooting

### Common errors

- **Column 'X' not found** — The DataFrame has no column with that name (case-sensitive if `spark.sql.caseSensitive` is true). The error message lists available columns; check spelling and case.

- **create_dataframe: expected 3 column names** — Rust `create_dataframe` accepts only `(i64, i64, String)` rows and exactly three column names. In Python, use `spark.createDataFrame(data, schema=None)` for any schema: list of dicts (infer), list of tuples with column names, or schema as list of `(name, dtype_str)`.

- **Type coercion: cannot find common type** — A comparison or arithmetic involved incompatible types (e.g. string vs numeric). Cast one side with `.cast()` or use compatible types.

- **read_csv(path): ...** — The path may not exist, not be readable, or the file may not be valid CSV. Ensure the path is correct and the file has a header row if using default options.

### Optional features (Rust)

- **SQL** (`--features sql`): `SparkSession::sql(query)` with temp views and tables. Use `create_or_replace_temp_view` and `table(name)`; or `df.write().saveAsTable(name, mode="error"|"overwrite"|"append"|"ignore")` for saved tables. Global temp views (`createOrReplaceGlobalTempView`) persist across sessions; disk-backed `saveAsTable` via `spark.sql.warehouse.dir` persists across restarts. See [PERSISTENCE_GUIDE.md](PERSISTENCE_GUIDE.md). Resolution: `table(name)` uses temp view → saved table → warehouse. Catalog: `listTables()`, `tableExists()`, `dropTempView(name)`, `dropTable(name)`. Supports single SELECT, FROM/JOIN, WHERE, GROUP BY, ORDER BY, LIMIT.
- **Delta Lake** (`--features delta`): `read_delta(path)`, `read_delta_with_version(path, version)` (time travel), `write_delta(path, overwrite)` on DataFrame. With `sql`, `read_delta(name_or_path)` accepts a table name (in-memory); `df.write_delta_table(name)` registers a DataFrame for `read_delta(name)`. Requires `deltalake` + tokio for path-based I/O.

### Benchmarks

Run `cargo bench` to compare robin-sparkless vs plain Polars on filter → select → groupBy pipelines. The goal is to stay within about 2x of Polars for supported operations.

### Test commands (Rust-only)

Common commands when working on robin-sparkless:

- `make test` – run the Rust test suite (`cargo test`).
- `make check-full` – full Rust check suite (format check, Clippy, `cargo audit`, `cargo deny`, tests); this is what CI runs.
- `make test-parity-phase-a` … `make test-parity-phase-g` – run PySpark parity fixtures for a specific phase (see `docs/PARITY_STATUS.md`).
- `make test-parity-phases` – run all parity phases (A–G).
- `make sparkless-parity` – when `SPARKLESS_EXPECTED_OUTPUTS` is set and PySpark/Java are available, convert Sparkless fixtures, regenerate expected from PySpark, and run the Rust parity tests.

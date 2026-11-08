# robin-sparkless-polars

Polars-backed implementation of [robin-sparkless](https://github.com/eddiethedean/robin-sparkless): `SparkSession`, `DataFrame`, `DataFrameReader`, `Column`, expression functions, UDFs, and plan execution. Physical execution lives here and in the optional JDBC/Polars adapter; core contracts remain independent of Polars. The root crate is a facade that re-exports the public API.

For current dependency status and consumer guidance, see the versioned
[security guide](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/SECURITY.md).

[![crates.io](https://img.shields.io/crates/v/robin-sparkless-polars.svg)](https://crates.io/crates/robin-sparkless-polars)
[![docs.rs](https://docs.rs/robin-sparkless-polars/badge.svg)](https://docs.rs/robin-sparkless-polars)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://github.com/eddiethedean/robin-sparkless/blob/main/LICENSE)

## Contents

| Area | Description |
|------|-------------|
| **Session** | `SparkSession`, `SparkSessionBuilder`, `DataFrameReader` — read CSV/Parquet/JSON, optional Delta, `create_dataframe_*`, optional SQL, catalog (temp views, tables) |
| **DataFrame** | `DataFrame`, `GroupedData`, `CubeRollupData`, `PivotedGroupedData` — filter, select, join, group_by, order_by, window, collect |
| **Column / Expr** | `Column`, Polars `Expr` — built via `functions::*` (e.g. `col`, `lit_*`, `when`, aggregations, string/datetime/struct/map) |
| **UDFs** | `UdfRegistry`, `RustUdf`, `apply_*` implementations used by expression layer |
| **Plan** | Plan executor for embedded execution (e.g. Sparkless Python → plan JSON → this crate) |
| **Optional SQL** | `sql` feature: `spark.sql("SELECT ...")` using [spark-sql-parser](https://crates.io/crates/spark-sql-parser) |
| **Optional Delta** | `delta` feature: `read_delta`, `write_delta`, Delta table support |

Implements the engine traits from `robin-sparkless-core` and converts `ExprIr` to Polars `Expr` at the boundary.

## Usage

Most users depend on the main crate, which re-exports the API. This repository's
4.20.0 quickstart is tested with Rust 1.93.1 and explicitly enables SQL:

```toml
[dependencies]
robin-sparkless = { version = "=4.20.0", features = ["sql"] }
```

Start with the [runnable Rust quickstart](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/QUICKSTART.md)
and [API reference](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/RUST_API_REFERENCE.md).

If you need to depend on the Polars backend directly (e.g. for a custom build or integration):

```toml
[dependencies]
robin-sparkless-core = "=4.20.0"
robin-sparkless-polars = { version = "=4.20.0", features = ["sql", "delta"] }
```

## License

MIT. See the [repository](https://github.com/eddiethedean/robin-sparkless) for details.

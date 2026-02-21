# robin-sparkless-polars

Polars-backed implementation of [robin-sparkless](https://github.com/eddiethedean/robin-sparkless): `SparkSession`, `DataFrame`, `DataFrameReader`, `Column`, expression functions, UDFs, and plan execution. This is the **only crate in the workspace that depends on Polars**; the root crate is a thin facade that re-exports the public API.

[![crates.io](https://img.shields.io/crates/v/robin-sparkless-polars.svg)](https://crates.io/crates/robin-sparkless-polars)
[![docs.rs](https://docs.rs/robin-sparkless-polars/badge.svg)](https://docs.rs/robin-sparkless-polars)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://github.com/eddiethedean/robin-sparkless/blob/main/LICENSE)

## Contents

| Area | Description |
|------|-------------|
| **Session** | `SparkSession`, `SparkSessionBuilder`, `DataFrameReader` — read CSV/Parquet/JSON/Delta, `create_dataframe_*`, `sql`, catalog (temp views, tables) |
| **DataFrame** | `DataFrame`, `GroupedData`, `CubeRollupData`, `PivotedGroupedData` — filter, select, join, group_by, order_by, window, collect |
| **Column / Expr** | `Column`, Polars `Expr` — built via `functions::*` (e.g. `col`, `lit_*`, `when`, aggregations, string/datetime/struct/map) |
| **UDFs** | `UdfRegistry`, `RustUdf`, `apply_*` implementations used by expression layer |
| **Plan** | Plan executor for embedded execution (e.g. Sparkless Python → plan JSON → this crate) |
| **Optional SQL** | `sql` feature: `spark.sql("SELECT ...")` using [spark-sql-parser](https://crates.io/crates/spark-sql-parser) |
| **Optional Delta** | `delta` feature: `read_delta`, `write_delta`, Delta table support |

Implements the engine traits from `robin-sparkless-core` and converts `ExprIr` to Polars `Expr` at the boundary.

## Usage

Most users depend on the main crate, which pulls in this crate and re-exports the API:

```toml
[dependencies]
robin-sparkless = { version = "0.14", features = ["sql"] }
```

If you need to depend on the Polars backend directly (e.g. for a custom build or integration):

```toml
[dependencies]
robin-sparkless-core = "0.14"
robin-sparkless-polars = { version = "0.14", features = ["sql", "delta"] }
```

## License

MIT. See the [repository](https://github.com/eddiethedean/robin-sparkless) for details.

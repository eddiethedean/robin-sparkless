# Rust API reference — 4.19.0

Build full rustdoc from the published source tag using the pinned toolchain.
This works independently of hosted build or artifact retention.

```bash
git clone --branch v4.19.0 --depth 1 https://github.com/eddiethedean/robin-sparkless.git
cd robin-sparkless
cargo doc --locked -p robin-sparkless --features sql --no-deps --open
```

The entry page is `target/doc/robin_sparkless/index.html`. CI also retains a
`rust-api-docs` artifact for the commit it built. The
[4.19.0 docs.rs API route](https://docs.rs/robin-sparkless/4.19.0/robin_sparkless/)
was unavailable at review. The [source tag](https://github.com/eddiethedean/robin-sparkless/tree/v4.19.0)
and [crate listing](https://crates.io/crates/robin-sparkless/4.19.0) identify the release.

## Choose your API

| Surface | Contract / entry point |
| --- | --- |
| Engine-neutral embedding | `SparkSession`, `DataFrame`, `ExprIr`, `EngineError`; [embedding guide](EMBEDDING.md) |
| Expressions without Polars types | Root `col`, `lit_i64`, `gt`, `when`; `*_expr_ir` / `*_engine` methods |
| Full Column expression API | `robin_sparkless::prelude::*`, `robin_sparkless::functions`; [user guide](USER_GUIDE.md) |
| Lazy actions | `collect_rows`, `count`, `to_json_rows`; return Results and may fail at execution |
| Schema | `StructType`, `StructField`, `schema_from_json`; [types/errors](TYPES_AND_ERRORS.md) |
| Native providers | Core resolver/read/write capability contracts; [external catalogs](EXTERNAL_CATALOGS.md) |
| Execution control | CancellationToken/backend actions; [cancellation](EXECUTION_CANCELLATION.md) |

Start with the complete [quickstart](QUICKSTART.md). Feature flags are part of
the API contract: SQL, Delta, and database drivers are opt-in for Rust consumers.
See the [security guide](SECURITY.md#known-dependency-exception) for the scope of
historical dependency exceptions and the lockfile used by 4.19.0.

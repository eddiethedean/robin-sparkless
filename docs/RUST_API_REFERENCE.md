# Rust API reference fallback — 4.18.0

The published **4.17.0 docs.rs build failed**: Polars 0.53 imports
`core::unicode::Cased` and `Case_Ignorable`, which the docs.rs nightly compiler
no longer exposes in that form. See the [build log](https://docs.rs/crate/robin-sparkless/4.17.0/builds/5050245).
This is a dependency/compiler incompatibility, not evidence that API docs were
successfully published. Previously published crate metadata cannot be changed.

Use this working source reference and generate full rustdoc with the repository's
pinned **Rust 1.93.1**, rather than relying on that failed nightly build:

```bash
git clone https://github.com/eddiethedean/robin-sparkless.git
cd robin-sparkless
# Check out your exact release tag or reviewed candidate commit first.
cargo doc --locked -p robin-sparkless --features sql --no-deps --open
```

The docs CI job runs this command without `--open` and retains a `rust-api-docs`
artifact; extract it and open `robin_sparkless/index.html`. Artifacts are scoped
to the run's commit, and are subject to GitHub retention. For published 4.17 source,
use the [immutable version tree](https://github.com/eddiethedean/robin-sparkless/tree/v4.17.0/src).
For 4.18 candidate source, inspect the commit shown by the downloaded workflow run.
The local cargo doc route is the fallback even after artifacts expire.

## Choose your API

| Surface | Contract / entry point |
| --- | --- |
| Engine-neutral embedding | `SparkSession`, `DataFrame`, `ExprIr`, `EngineError`; [embedding guide](EMBEDDING.md) |
| Expressions without Polars types | Root `col`, `lit_i64`, `gt`, `when`; `*_expr_ir` / `*_engine` methods |
| Full Column expression API | `robin_sparkless::prelude::*`, `robin_sparkless::functions`; [user guide](USER_GUIDE.md) |
| Lazy actions | `collect_rows`, `count`, `to_json_rows`; return Results and may fail at execution |
| Schema | `StructType`, `StructField`, `schema_from_json`; [types/errors](TYPES_AND_ERRORS.md) |
| Native providers | Core ExternalCatalogProvider contract; [external catalogs](EXTERNAL_CATALOGS.md) |
| Execution control | CancellationToken/backend actions; [cancellation](EXECUTION_CANCELLATION.md) |

Start with the complete [quickstart](QUICKSTART.md). Feature flags are part of
the API contract: SQL, Delta, and database drivers are opt-in for Rust consumers.
Read the [consumer zstd notice](security/zstd-4.18-exception.md) before resolving
an unpatched crates.io graph. Documentation build success does not prove the
consumer security graph is patched.

At release review, verify the actual versioned docs.rs API page. If it still
fails, retain this explicit fallback and record the failure in release evidence;
do not mark public docs.rs healthy merely because Cargo publication succeeded.

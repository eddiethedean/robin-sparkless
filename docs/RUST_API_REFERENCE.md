# Rust API reference — 4.20.0

## Browse or download

The [versioned docs.rs route](https://docs.rs/robin-sparkless/4.20.0/robin_sparkless/)
identifies the published crate. If hosted generation is unavailable, download
`rust-api-docs` from the **CI** workflow, or prepare the complete
`browsable-documentation-bundle` with the **Prepare or publish browsable documentation**
workflow. Both build with the pinned Rust 1.93.1 toolchain; they require no Rust
installation on the reader's machine once downloaded.

The complete bundle contains `rust-api/4.20.0/robin_sparkless/index.html` and the
engine-neutral core reference. Open those files locally or serve the directory
with `python -m http.server`. `documentation-build.json` records the source SHA,
version, selected features and whether the source tree was modified. Download
artifacts from the run matching the version/source you are evaluating.

The publication workflow is manual and defaults to **build only**. After setting
up GitHub Pages for Actions in the repository, a maintainer can dispatch it on
`main` with `publish=true`. The Pages environment controls deployment approval.
Use the deployment output URL plus `rust-api/4.20.0/robin_sparkless/` for the
actual hosted route; do not claim it is available until verified. Save each
release bundle before replacing a Pages deployment; an individual deployment
contains the built version, not a permanent archive of every previous release.
See [publication verification](DOCUMENTATION.md#verify-hosted-publication).

## Minimal local build

Build only the facade and core first. SQL is enabled; optional Delta/database
APIs are not all included in this smaller reference.

```bash
git clone --branch v4.20.0 --depth 1 https://github.com/eddiethedean/robin-sparkless.git
cd robin-sparkless
cargo doc --locked -p robin-sparkless -p robin-sparkless-core --features robin-sparkless/sql --no-deps --open
```

Open `target/doc/robin_sparkless/index.html`. For a feature-expanded reference,
run `cargo doc --locked --workspace --all-features --no-deps`; that requires the
native dependencies for every selected driver. First builds can be substantial.
The [source tag](https://github.com/eddiethedean/robin-sparkless/tree/v4.20.0)
and [crate listing](https://crates.io/crates/robin-sparkless/4.20.0) identify the release.

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
historical dependency exceptions; audit your application's own resolved lockfile.

# robin-sparkless-core

Shared types, config, error, and engine-agnostic expression IR for [robin-sparkless](https://github.com/eddiethedean/robin-sparkless). **No Polars dependency** — use this crate when you want schema, config, or expression building without pulling in an execution backend.

[![crates.io](https://img.shields.io/crates/v/robin-sparkless-core.svg)](https://crates.io/crates/robin-sparkless-core)
[![docs.rs](https://docs.rs/robin-sparkless-core/badge.svg)](https://docs.rs/robin-sparkless-core)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://github.com/eddiethedean/robin-sparkless/blob/main/LICENSE)

## Contents

| Area | Description |
|------|-------------|
| **Config** | `SparklessConfig` for session/engine configuration |
| **Schema** | `DataType`, `StructField`, `StructType` (engine-agnostic) |
| **Error** | `EngineError` used across backends |
| **Engine traits** | `SparkSessionBackend`, `DataFrameReaderBackend`, `DataFrameBackend`, `GroupedDataBackend`, `PlanExecutor`, `JoinType`, `CollectedRows` |
| **Expression IR** | `ExprIr`, `LiteralValue`, and builders: `col`, `when`, `lit_i64`, `lit_str`, `lit_bool`, `lit_f64`, `lit_i32`, `lit_null`, `gt`, `lt`, `eq`, `and_`, `or_`, `between`, `is_in`, `call`, etc. |

Backends (e.g. `robin-sparkless-polars`) implement the engine traits and convert `ExprIr` into their native representation. The root crate `robin-sparkless` re-exports the public API and wires in one backend.

## Usage

Most users depend on the main crate only:

```toml
[dependencies]
robin-sparkless = "0.14"
```

If you are building a custom backend or need only types and expression IR:

```toml
[dependencies]
robin-sparkless-core = "0.14"
```

## License

MIT. See the [repository](https://github.com/eddiethedean/robin-sparkless) for details.

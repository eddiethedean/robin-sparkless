# Product History

## Names and packages

| Name | What it is |
|------|------------|
| **sparkless** (PyPI) | Python package — primary user-facing product |
| **robin-sparkless** (crates.io) | Rust engine crate in this repository |
| **robin-sparkless** (GitHub repo) | Monorepo: Python package + Rust workspace |

Documentation for v4 lives on [Read the Docs (robin-sparkless)](https://robin-sparkless.readthedocs.io/). Older Sparkless 3.x docs at [sparkless.readthedocs.io](https://sparkless.readthedocs.io/) describe the previous Polars-Python backend.

## Sparkless 3.x vs 4.x

| | Sparkless 3.x | Sparkless 4.x |
|---|---------------|---------------|
| **Backend** | Polars Python | Rust `robin-sparkless` (native extension) |
| **Install** | `pip install sparkless` (3.x line) | `pip install "sparkless>=4,<5"` |
| **Repo** | Historical [sparkless](https://github.com/eddiethedean/sparkless) repo | This repo ([robin-sparkless](https://github.com/eddiethedean/robin-sparkless)) |
| **Runtime** | Polars + Python | Native extension; no Polars Python dependency |

Sparkless **4.x** is the current line. The 3.x and 4.x packages share the PyPI name **`sparkless`** with different major versions.

## Version alignment

Release **4.x** versions are aligned across:

- PyPI `sparkless`
- crates.io `robin-sparkless`, `robin-sparkless-core`, `robin-sparkless-polars`

See [RELEASING.md](RELEASING.md) and [CHANGELOG.md](https://github.com/eddiethedean/robin-sparkless/blob/main/CHANGELOG.md).

## Migration

- From Sparkless 3.x or PySpark: [python_migration.md](python_migration.md)
- Before production use: [BEFORE_YOU_ADOPT.md](BEFORE_YOU_ADOPT.md)

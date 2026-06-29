# Contributing to robin-sparkless / Sparkless

Thank you for contributing. This repository contains the **Sparkless** Python package (`python/`) and the **robin-sparkless** Rust engine (workspace crates).

## Choose your area

| Area | Start here |
|------|------------|
| **Python package** (`sparkless`) | [python/README.md](python/README.md), [Testing guide](docs/TESTING_GUIDE.md) |
| **Rust engine** | [docs/QUICKSTART.md](docs/QUICKSTART.md), crate READMEs in `crates/` |
| **Documentation** | [docs/](docs/), build with `mkdocs serve` (see [docs/requirements.txt](docs/requirements.txt)) |

## Prerequisites

- **Rust** — pinned in [rust-toolchain.toml](rust-toolchain.toml) (currently 1.93.x)
- **Python 3.8+** — for the `sparkless` package and pytest suite
- **maturin** — for editable Python/native builds (`pip install maturin`)
- **Java** (optional) — only for `SPARKLESS_TEST_MODE=pyspark` parity runs

## Setup

```bash
git clone https://github.com/eddiethedean/robin-sparkless.git
cd robin-sparkless

# Python editable install (rebuilds native extension on changes)
cd python && maturin develop && cd ..

# Or install package from repo root
pip install "./python[dev]"
```

## Checks before opening a PR

Run the full suite locally (allow 10–20 minutes on first build):

```bash
make check-full
```

This runs: `cargo fmt`, clippy, audit, deny, Rust tests (all features), ruff, and mypy.

Faster subsets:

```bash
make check              # Rust only
make lint-python        # ruff + mypy on sparkless package
pytest tests/ -v        # Python tests (sparkless backend)
make check-crate CRATE=robin-sparkless-polars   # single crate
```

## Python tests

```bash
# Default: sparkless backend (no JVM)
pytest tests/ -v

# Parallel
pytest tests/ -n 12 -v

# Against real PySpark (requires Java + pip install "./python[pyspark]")
SPARKLESS_TEST_MODE=pyspark pytest tests/ -v
```

Parity fixture phases: `make test-parity-phases`

## Rust tests

```bash
cargo test --workspace --all-features
```

## Pull request guidelines

1. **One logical change per PR** when possible (feature, fix, or docs — not mixed).
2. **Tests** — add or extend pytest/Rust tests for behavior changes; link the issue (`Fixes #1234`).
3. **Docs** — update user-facing docs when changing public API or install paths.
4. **Changelog** — add entries under `[Unreleased]` in [CHANGELOG.md](CHANGELOG.md) for user-visible changes.
5. **Format** — `cargo fmt` and `ruff format` before pushing.

## Repository layout

```
python/           # sparkless PyPI package (PyO3 → robin-sparkless)
crates/
  robin-sparkless-core/    # types, ExprIr (no Polars)
  robin-sparkless-polars/  # Polars backend
  spark-sql-parser/        # SQL parsing
src/              # robin-sparkless root facade
tests/            # Python pytest suite (parity + issues)
docs/             # MkDocs site (Read the Docs)
```

## Releases

Maintainers: see [docs/RELEASING.md](docs/RELEASING.md). Do not tag until versions and CHANGELOG are aligned across `Cargo.toml`, `python/pyproject.toml`, and `python/sparkless/sparkless/__init__.py`.

## Questions

- [FAQ](docs/FAQ.md)
- [GitHub Issues](https://github.com/eddiethedean/robin-sparkless/issues)

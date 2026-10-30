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
- **Python 3.11+** — supported by the package; the contributor commands below use Python 3.11
- **uv** — for Python environments, dependencies, and commands ([installation](https://docs.astral.sh/uv/getting-started/installation/))
- **maturin** — for Python/native builds; installed with the development dependencies below
- **Java** (optional) — only for `SPARKLESS_TEST_MODE=pyspark` parity runs

## Setup

```bash
git clone https://github.com/eddiethedean/robin-sparkless.git
cd robin-sparkless

# Create the contributor environment at the repository root
uv venv --python 3.11 .venv

# Install build tools and the dev extra without building the package yet
uv pip install --python .venv/bin/python \
  -r python/pyproject.toml --extra dev "maturin>=1.4,<2"

# Build and install the native extension with editable Python sources
(
  cd python
  uv run --no-project --python ../.venv/bin/python \
    python -m maturin develop --uv --locked --release
)
```

Run these Bash examples from the repository root. No environment activation is required.
On Windows, use Git Bash and replace `.venv/bin/python` with `.venv/Scripts/python.exe`
(and adjust the relative path inside `python/` accordingly).

Use `uv pip` to select dependencies and `uv run --no-project` to run commands in this
environment. The package metadata is in `python/pyproject.toml`; the root
`pyproject.toml` contains test and lint configuration. Project-wide `uv sync` / `uv run`
resolution currently fails because the `pyspark` and `pyspark4` extras require incompatible
Spark versions. `--no-project` skips that automatic project resolution and synchronization.

Rerun the Maturin command after changing Rust or PyO3 code, then run the relevant tests.
An editable install reflects Python source changes, but the native extension still needs
an explicit rebuild. To check which package and extension the environment imports:

```bash
uv run --no-project --python .venv/bin/python python -c \
  "import sparkless, sparkless._native; print(sparkless.__file__); print(sparkless._native.__file__)"
```

To build a wheel, replace `maturin develop --uv` in the build command with `maturin build`.
Maturin prints the wheel path under `target/wheels/`. To test that wheel, install that exact
path with `uv pip install --python .venv/bin/python --reinstall PATH_TO_WHEEL`, then use
the test commands below. If uv's cache is on a different filesystem, add `--link-mode=copy`
to `uv pip install` to avoid the hardlink fallback warning.

## Checks before opening a PR

Run Rust checks/tests and Python lint/type checks:

```bash
make check-full
```

This runs: `cargo fmt`, Clippy, audit, deny, Rust tests (all features), Ruff, and mypy.
Run Python tests separately using the commands below. First builds can take
substantial time; the duration depends on the selected features and host.

Faster subsets:

```bash
make check              # Rust only
make lint-python        # ruff + mypy on sparkless package
uv run --no-project --python .venv/bin/python python -m pytest tests/ -v
make check-crate CRATE=robin-sparkless-polars   # single crate
```

## Python tests

```bash
# Default: sparkless backend (no JVM)
uv run --no-project --python .venv/bin/python python -m pytest tests/ -v

# Focused test file
uv run --no-project --python .venv/bin/python \
  python -m pytest tests/unit/test_external_catalog_provider.py -q

# Parallel
uv run --no-project --python .venv/bin/python python -m pytest tests/ -n 12 -v

# Against real PySpark (requires Java; select the PySpark 3 extra only)
uv pip install --python .venv/bin/python -r python/pyproject.toml --extra pyspark
SPARKLESS_TEST_MODE=pyspark uv run --no-project --python .venv/bin/python \
  python -m pytest tests/ -v
```

Use a separate environment for the `pyspark4` extra; do not install it alongside `pyspark`.

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

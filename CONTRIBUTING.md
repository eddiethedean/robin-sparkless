# Contributing to robin-sparkless / Sparkless

Thank you for contributing. This repository contains the **Sparkless** Python package (`python/`) and the **robin-sparkless** Rust engine (workspace crates).

## Choose your area

| Area | Start here |
|------|------------|
| **Python package** (`sparkless`) | [python/README.md](python/README.md), [Testing guide](docs/TESTING_GUIDE.md) |
| **Rust engine** | [docs/QUICKSTART.md](docs/QUICKSTART.md), crate READMEs in `crates/` |
| **Documentation** | [docs/](docs/), build with `mkdocs serve` (see [docs/requirements.txt](docs/requirements.txt)) |

## Documentation only

From the repository root, install the MkDocs dependencies with uv and run the
site checks. This path needs Python 3.11 and uv.

```bash
uv run --no-project --python 3.11 --with-requirements docs/requirements.txt python scripts/check_documentation.py
uv run --no-project --python 3.11 --with-requirements docs/requirements.txt mkdocs build --strict
uv run --no-project --python 3.11 --with-requirements docs/requirements.txt mkdocs serve
```

Open `http://127.0.0.1:8000/` to preview. Follow the
[docs maintenance guide](docs/DOCUMENTATION.md) for version and example updates.

## Rust only

Install Rust through rustup and run from the checkout so it picks up
`rust-toolchain.toml`. Start with formatting and the crate you changed.

```bash
cargo fmt --all -- --check
cargo test --locked -p robin-sparkless-core
```

Replace `robin-sparkless-core` with the relevant workspace crate. Add the
features your change needs. The Python setup below is for Python or PyO3 work.

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

# Install contributor tools before building the native engine
uv pip install --python .venv/bin/python "maturin>=1.4,<2"
uv pip install --python .venv/bin/python pytest pandas pytest-xdist hypothesis pytest-timeout polars pyarrow ruff mypy build wheel setuptools pyyaml

# Build the engine, then install the Python API with editable sources
(
  cd python/native
  uv run --no-project --python ../../.venv/bin/python \
    python -m maturin develop --uv --locked --release
)
uv pip install --python .venv/bin/python --no-deps -e python
uv pip install --python .venv/bin/python --no-deps -e python/testing
uv pip install --python .venv/bin/python --no-deps -e python/connect
```

Run these Bash examples from the repository root. No environment activation is required.
On Windows, use Git Bash and replace `.venv/bin/python` with `.venv/Scripts/python.exe`
(and adjust the relative path inside `python/native/` accordingly).

Use `uv pip` to select dependencies and `uv run --no-project` to run commands in this
environment. API metadata is in `python/pyproject.toml` and native metadata is in
`python/native/pyproject.toml`; the root
`pyproject.toml` contains test and lint configuration. Project-wide `uv sync` / `uv run`
resolution currently fails because the `pyspark` and `pyspark4` extras require incompatible
Spark versions. `--no-project` skips that automatic project resolution and synchronization.

Rerun the Maturin command after changing Rust or PyO3 code, then run the relevant tests.
An editable install reflects Python source changes, but the native extension still needs
an explicit rebuild. To check which package and extension the environment imports:

```bash
uv run --no-project --python .venv/bin/python python -c \
  "import sparkless, sparkless_native._native; print(sparkless.__file__); print(sparkless_native._native.__file__)"
```

To build distribution wheels, use `.venv/bin/python -m maturin build --locked --release --manifest-path python/native/Cargo.toml`
and `.venv/bin/python -m build --wheel python`. Install both wheels
from `target/wheels/` and `python/dist/` in a fresh environment, then use the test
commands below. If uv's cache is on a different filesystem, add `--link-mode=copy`
to `uv pip install` to avoid the hardlink fallback warning.

## Contributor profiles

The setup above installs the local API, testing helpers, shared launcher, build,
lint and metadata tools. Extend that environment only for your area:

| Work | Additional setup | First verification |
| --- | --- | --- |
| Local Python/native | Canonical setup above; rebuild native after Rust changes | Run a focused pytest test and `make lint-python` |
| Docs and API inventory | Install `docs/requirements.txt` into the same `.venv` | Run [documentation checks](docs/DOCUMENTATION.md#checks); API generation imports the shared launcher |
| SQLite examples | Rebuild native with `--features sqlite` in addition to defaults | `.venv/bin/python examples/python/jdbc_sqlite.py` |
| HTTP/Connect launcher | Install editable `python/server` or `python/server-duckdb` with `--no-deps`; build the matching Rust executable | Pass its absolute `executable_path` or `--server-bin` in examples; editable product sources contain no packaged binary |
| Connect client | Install `pyspark-client==4.0.5` in a separate environment from JVM PySpark | Follow `tests/connect/README.md` and run the selected transport smoke |
| Package builds | Canonical setup includes `build` and Maturin | Build API/native wheels, install in a fresh environment, then `python -m pip check` |

Native compilation requires a C/C++ compiler/linker: Xcode Command Line Tools on
macOS, Visual Studio C++ Build Tools on Windows, and compiler/build tools on
Linux. Install CMake if a selected native dependency requests it. Oracle and DB2
features also require their vendor client/ODBC environment. Keep optional driver
work separate from documentation-only work. No global Python activation is assumed.

## Checks before opening a PR

Choose the smallest check for your change. For the complete local Rust and
Python lint pass, install the same Cargo security tools as CI:

```bash
cargo install cargo-audit --locked --version 0.22.1
cargo install cargo-deny --locked --version 0.19.0
make check-full
```

This runs: `cargo fmt`, Clippy, audit, deny, Rust tests (all features), Ruff, and mypy.
Run Python tests separately using the commands below. Use `make clean` when you need
to reclaim Cargo build space. First builds can take
substantial time; the duration depends on the selected features and host.

### CI map

Every pull request and push to `main` runs the required jobs in
`.github/workflows/ci.yml`. Use the matching local command first:

| Change | CI coverage | Local starting point |
| --- | --- | --- |
| Rust crate or engine | Format, Clippy, SQL/Delta/default-core tests, and server product checks | `cargo fmt --all -- --check`; `cargo test --locked -p <crate>` with required features |
| Python API or test helpers | Build the native wheel, run Python tests, lint and type-check | Rebuild with Maturin, then `.venv/bin/python -m pytest tests/ -q` and `make lint-python` |
| User documentation | Link/fence/version checks, API inventory, strict MkDocs build, installed-wheel examples | Follow [documentation checks](docs/DOCUMENTATION.md#checks) |
| Server transport or launcher | Base and DuckDB server tests plus installed-wheel examples | Run the affected Rust crate tests and the matching [HTTP](docs/HTTP_API.md) or [Connect](docs/SPARK_CONNECT.md) example |

The PySpark smoke and full Python suite are manual CI jobs. The separate
**Published documentation examples** workflow installs release wheels from
PyPI and runs the published local, testing, HTTP, Connect, and DuckDB guides;
dispatch it after publication or when checking those install paths.

Faster subsets:

```bash
make check              # Rust only
make lint-python        # ruff + mypy on the local API, testing package, and launcher
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

## Documentation checks

Follow the [documentation maintenance and release checklist](docs/DOCUMENTATION.md).
Keep public examples runnable from an installed wheel, assert actual test backend
identity, and run the strict docs job before declaring a release candidate ready.

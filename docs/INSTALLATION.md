# Installation and versions

Documentation contract: **4.18.1 is published**, using the **4.18.0 native
engine and server**. The API, engine, and optional server binary are separate
PyPI projects. See [release evidence](RELEASE_EVIDENCE.md) for the exact tag,
commit, public inventories, and workflow results.

## Install the published API

```bash
python3.11 -m venv .venv
# POSIX; Windows: .venv\Scripts\activate
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install "sparkless==4.18.1"
python -c "import sparkless; print(sparkless.__version__)"
```

Use the [4.18.1 source documentation](https://github.com/eddiethedean/robin-sparkless/tree/v4.18.1/docs)
for the published behavior. `/en/latest/` follows development and can describe
unreleased features. A matching wheel needs no local Java or Rust toolchain.

## Published package components

| Component | PyPI distribution | Version |
| --- | --- | --- |
| Python API | [`sparkless`](https://pypi.org/project/sparkless/4.18.1/) | 4.18.1 |
| Native engine | [`sparkless-native`](https://pypi.org/project/sparkless-native/4.18.0/) | 4.18.0 |
| Optional HTTP/Connect server | [`sparkless-server-bin`](https://pypi.org/project/sparkless-server-bin/4.18.0/) | 4.18.0 |

Install the optional server with `python -m pip install "sparkless[server]==4.18.1"`.
The exact engine and server pins are declared by the API package. The public
inventories and hashes were checked by the successful [tagged Release run](https://github.com/eddiethedean/robin-sparkless/actions/runs/41170498256).

## Platforms and build features

The API wheel is universal; the separate `sparkless-native` extension wheels use **cp311-abi3**, compatible with supported CPython 3.11–3.14,
not only CPython 3.11. Wheels cover Linux glibc 2.17+ on x86_64, glibc 2.28+ on ARM64,
and musl 1.2+ on both architectures,
macOS Intel/Apple Silicon, and Windows x86_64/ARM64. Use wheel filenames for exact
macOS deployment floors (the ARM64 extension starts at macOS 11; the separate
server starts at macOS 14). Server binaries cover glibc 2.28+ Linux x86_64 and
macOS 14+ ARM64 only. Other targets require a source build.

| Distribution | SQL | Delta | Database drivers |
| --- | --- | --- | --- |
| Official `sparkless` wheel/default source build | Enabled | Enabled | JDBC/SQLite not enabled by default |
| Root Rust crate default features | Disabled | Disabled | Disabled |
| Rust crate with selected Cargo features | `sql` | `delta` | `jdbc` (Postgres), `sqlite`, `jdbc_mysql`, `jdbc_mariadb`, `jdbc_mssql`, `jdbc_oracle`, `jdbc_db2` |
| Packaged server | HTTP allowlist | No public Delta contract | No general database connector contract |

Python extras install Python dependencies, not native feature switches:
`dev` adds test tools; `jdbc` adds integration tools/drivers; `pyspark` adds Spark
3.5/Delta3 (Java11+); `pyspark4` adds Spark4/Delta4 (Java17+); `server` adds the
exact matching binary package. Keep the two PySpark extras in separate environments.

After canonical source setup, a SQLite/Postgres-enabled development extension:

```bash
(cd python/native && ../../.venv/bin/python -m maturin develop --uv --locked --release --features sqlite,jdbc)
```

This retains default Delta and mandatory SQL. A database extra alone cannot
change a downloaded wheel. See [JDBC tests](JDBC_TESTING.md) for external services.
Rust consumers must read the [4.18 security notice](security/zstd-4.18-exception.md):
the unpatched Cargo graph has a time-bounded zstd exception through **2026-11-26**;
apply the pinned consumer patch. Repository/official distributions use the fixed graph.

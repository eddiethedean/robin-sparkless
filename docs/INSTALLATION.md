# Install Sparkless 4.19.0

Create a Python 3.11 virtual environment. Use its Python executable for both
installation and examples.

=== "macOS and Linux"

    ```bash
    python3.11 -m venv .venv
    source .venv/bin/activate
    ```

=== "Windows PowerShell"

    ```powershell
    py -3.11 -m venv .venv
    .\.venv\Scripts\Activate.ps1
    ```

## Choose a package

Copy the install spec for your workflow. Use quotes around specs with extras.

| Workflow | PyPI install spec | Main import | Wheel requirements |
| --- | --- | --- | --- |
| Local DataFrames and SQL | `sparkless==4.19.0` | `sparkless.sql` | Native engine |
| Local pytest fixtures | `sparkless[testing]==4.19.0` | `sparkless_testing.plugin` | Native engine |
| Testing helpers only | `sparkless-testing==4.19.0` | `sparkless_testing` | Pure Python |
| Base HTTP server | `sparkless-server==4.19.0` | `sparkless_server` | Server wheel |
| Own a base server and Connect session | `sparkless-server[connect]==4.19.0` | `sparkless_server.SparklessConnect` | Server wheel |
| Read configured DuckDB sources | `sparkless-server-duckdb[connect]==4.19.0` | `sparkless_server_duckdb.SparklessConnect` | Server wheel |
| Connect to an existing endpoint | `sparkless-connect[client]==4.19.0` | `pyspark.sql` | Client dependencies for your platform |

For the local API:

```bash
python -m pip install --upgrade pip
python -m pip install "sparkless==4.19.0"
python -c "import sparkless; print(sparkless.__version__)"
```

Expect `4.19.0`. The API pins the matching `sparkless-native` distribution.
Both server products depend on the lightweight `sparkless-connect` launcher.
Their Connect extras install `pyspark-client==4.0.5`. The local API and
standalone Connect setup run without Java. Install a compatible JDK for
[local PySpark reference tests](TESTING_GUIDE.md#quick-start).

If you already use the local API, its `server`, `connect`, `server-duckdb`, and
`connect-duckdb` extras add the corresponding server package. The server
products also install independently of the local engine.

Continue with the [Python quickstart](python_getting_started.md),
[pytest quickstart](TESTING_GUIDE.md#quick-start),
[Connect quickstart](SPARK_CONNECT.md), or [HTTP quickstart](HTTP_API.md#start-and-query).
See [release evidence](RELEASE_EVIDENCE.md) for public distribution links and
[the migration guide](python_migration.md#from-sparkless-418-to-419) for removed
4.18 imports. For an application staying on 4.18.1, use the
[4.18.1 installation guide](https://github.com/eddiethedean/robin-sparkless/blob/v4.18.1/docs/INSTALLATION.md).

For offline installs, collect matching product, companion, and third-party
wheels in one directory and use `--no-index --find-links ./wheelhouse`. See
[installation troubleshooting](TROUBLESHOOTING_PYTHON.md) if pip cannot find a
compatible wheel.

## Platforms and build features

The API and test-helper wheels are pure Python. The `sparkless-native` extension
uses the CPython 3.11 stable ABI and publishes wheels for CPython 3.11–3.14.
The server is distributed as a separate platform-specific executable. Use this
table to choose an install path:

| Product | Published wheel targets | Minimum platform details |
| --- | --- | --- |
| `sparkless-native` | Linux x86_64 and ARM64, macOS Intel and Apple Silicon, Windows x86_64 and ARM64 | Linux x86_64 glibc 2.17; Linux ARM64 glibc 2.28; Linux x86_64 and ARM64 musl 1.2; macOS x86_64 10.12 and ARM64 11; Windows x86_64 and ARM64 |
| `sparkless-server` and `sparkless-server-duckdb` | Linux x86_64 and macOS ARM64 | Linux glibc 2.28; macOS 14 |
| `sparkless`, `sparkless-testing`, `sparkless-connect` | Platform independent | Python 3.11 or newer. Published native wheels currently cover CPython through 3.14. |

The extension wheel and server executable have different platform support.
Windows and Linux ARM64 can run the local API from its native wheel but have
no published Sparkless server executable. Confirm the exact release's
[PyPI file list](https://pypi.org/project/sparkless-native/4.19.0/#files)
before pinning a deployment image. Other native targets require a source build
with the repository's pinned Rust toolchain and a platform C/C++ linker.

To check wheel availability without allowing pip to fall back to an sdist:

```bash
python -m pip install --only-binary=:all: "sparkless-native==4.19.0"
```

If pip reports no compatible distribution, record the interpreter and platform
tags before choosing another route:

```bash
python -c "import platform, sys; print(sys.version); print(platform.platform(), platform.machine())"
python -m pip debug --verbose
python -m pip show sparkless sparkless-native
```

An sdist install attempts a local native build. It needs the pinned Rust
toolchain, Maturin, a C/C++ toolchain, and the source-build dependencies in
[contributor setup](https://github.com/eddiethedean/robin-sparkless/blob/main/CONTRIBUTING.md#setup).
On Windows, install Visual Studio C++ Build Tools. On macOS, install Xcode
Command Line Tools. On Linux, install the compiler and linker for the target
architecture. The packaged server wheel has no user-facing source fallback
install path. Build its executable with Cargo as described in the
[server crate guide](https://github.com/eddiethedean/robin-sparkless/blob/v4.19.0/crates/sparkless-server/README.md).

| Distribution | SQL | Delta | Database drivers |
| --- | --- | --- | --- |
| Official `sparkless` wheel/default source build | Enabled | Enabled | JDBC/SQLite not enabled by default |
| Root Rust crate default features | Disabled | Disabled | Disabled |
| Rust crate with selected Cargo features | `sql` | `delta` | `jdbc` (Postgres), `sqlite`, `jdbc_mysql`, `jdbc_mariadb`, `jdbc_mssql`, `jdbc_oracle`, `jdbc_db2` |
| Packaged server | HTTP allowlist | No public Delta contract | No general database connector contract |

Python extras install Python dependencies, not native feature switches:
`dev` adds test tools; `jdbc` adds integration tools/drivers; `pyspark` adds Spark
3.5/Delta3 (Java 11+); `pyspark4` adds Spark4/Delta4 (Java 17+).
Keep the two PySpark extras in separate environments.

After canonical source setup, a SQLite/Postgres-enabled development extension:

```bash
(cd python/native && ../../.venv/bin/python -m maturin develop --uv --locked --release --features sqlite,jdbc)
```

This retains default Delta and mandatory SQL. A database extra alone cannot
change a downloaded wheel. See [JDBC tests](JDBC_TESTING.md) for external services.
For Rust dependencies and historical compression exceptions, see the
[security guide](SECURITY.md#known-dependency-exception).

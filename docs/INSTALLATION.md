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

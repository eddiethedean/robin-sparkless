# Frequently Asked Questions

## General

### What is Sparkless?

**Sparkless** is a Python package (`pip install sparkless`) that provides a PySpark-like `SparkSession` / `DataFrame` API without the JVM. It is powered by the Rust crate **robin-sparkless**, which executes on [Polars](https://www.pola.rs/).

### What is robin-sparkless?

The **Rust engine** in this repository. Most Python users only install `sparkless` from PyPI. Rust developers embed `robin-sparkless` directly via crates.io.

### Is this a drop-in replacement for PySpark?

The local API supports a subset of PySpark. Start with
`from sparkless.sql import SparkSession` and check the behavior your code uses
against your production Spark version. See [Before you adopt](BEFORE_YOU_ADOPT.md).

### Do I need Java?

The local Sparkless API and the standalone PySpark Connect client can run
against Sparkless without Java. Running the testing plugin in
`SPARKLESS_TEST_MODE=pyspark` starts a local Spark JVM and requires Java.

### Can Sparkless read my DuckDB test database?

Sparkless 4.19 can read configured tables through the optional
`sparkless-server-duckdb` product. The server opens the database read-only,
and tests access tables by name using the PySpark Connect client.
See [read DuckDB tables](DUCKDB_SOURCE.md).

### How is Sparkless different from Polars?

Sparkless exposes a **PySpark-compatible API** (`SparkSession`, `groupBy`, `spark.sql`, temp views). Polars has its own Python API. Sparkless is for teams with existing PySpark code or tests; Polars is for new Polars-native pipelines.

## Installation

### How do I install Sparkless?

```bash
pip install "sparkless==4.19.0"
```

See [Python getting started](python_getting_started.md) and [Troubleshooting (Python)](TROUBLESHOOTING_PYTHON.md).

### What Python versions are supported?

The local engine publishes wheels for CPython **3.11–3.14**. The server has a smaller platform set. See the [wheel platform matrix](INSTALLATION.md#platforms-and-build-features).

### Sparkless 3 vs 4?

Sparkless **3.x** used Polars Python as the backend. Sparkless **4.x** uses the Rust `robin-sparkless` engine (native extension, no Polars Python at runtime). See [Product history](PRODUCT_HISTORY.md) and [Migration guide](python_migration.md).

## Usage

### Can I use Python UDFs (`@udf`, pandas UDFs)?

Typed scalar `F.udf` works in the local Python API. Public pandas/vectorized UDFs and remote Python UDF execution are unsupported. See the checked example and context limits in the [UDF guide](UDF_GUIDE.md).

### Does `spark.sql()` work?

Yes, with the SQL feature enabled in the native build. Temp views, global temp views, and many DDL/DML statements are supported. See [PySpark differences](PYSPARK_DIFFERENCES.md) for gaps.

### How do I run the same tests against PySpark and Sparkless?

Use the `sparkless_testing` pytest plugin. See [Testing guide](TESTING_GUIDE.md).

## Parity and bugs

### How complete is PySpark parity?

There is no defensible full-API parity percentage. Use the versioned [support contract](SUPPORT_MATRIX.md) and generated [compatibility evidence](PARITY_STATUS.md), then test your own workload on both engines.

### Where do I report a parity bug?

[GitHub Issues](https://github.com/eddiethedean/robin-sparkless/issues) with a minimal PySpark vs Sparkless repro.

## Contributing

See [CONTRIBUTING.md](https://github.com/eddiethedean/robin-sparkless/blob/main/CONTRIBUTING.md).

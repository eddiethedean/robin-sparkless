# Frequently Asked Questions

## General

### What is Sparkless?

**Sparkless** is a Python package (`pip install sparkless`) that provides a PySpark-like `SparkSession` / `DataFrame` API without the JVM. It is powered by the Rust crate **robin-sparkless**, which executes on [Polars](https://www.pola.rs/).

### What is robin-sparkless?

The **Rust engine** in this repository. Most Python users only install `sparkless` from PyPI. Rust developers embed `robin-sparkless` directly via crates.io.

### Is this a drop-in replacement for PySpark?

For **local tests and development**, often yes — swap the import to `from sparkless.sql import SparkSession`. For **production Spark clusters**, no. See [Before you adopt](BEFORE_YOU_ADOPT.md).

### Do I need Java?

**No** for normal Sparkless usage. Java is only required if you run tests with `SPARKLESS_TEST_MODE=pyspark` to compare against real PySpark.

### How is Sparkless different from Polars?

Sparkless exposes a **PySpark-compatible API** (`SparkSession`, `groupBy`, `spark.sql`, temp views). Polars has its own Python API. Sparkless is for teams with existing PySpark code or tests; Polars is for new Polars-native pipelines.

## Installation

### How do I install Sparkless?

```bash
pip install "sparkless>=4,<5"
```

See [Python getting started](python_getting_started.md) and [Troubleshooting (Python)](TROUBLESHOOTING_PYTHON.md).

### What Python versions are supported?

Python **3.8+**. Prebuilt wheels are published for common Linux, macOS, and Windows platforms.

### Sparkless 3 vs 4?

Sparkless **3.x** used Polars Python as the backend. Sparkless **4.x** uses the Rust `robin-sparkless` engine (native extension, no Polars Python at runtime). See [Product history](PRODUCT_HISTORY.md) and [Migration guide](python_migration.md).

## Usage

### Can I use Python UDFs (`@udf`, pandas UDFs)?

**No** at the Python layer. Use built-in `functions` or engine-level Rust UDFs ([UDF guide](UDF_GUIDE.md)).

### Does `spark.sql()` work?

Yes, with the SQL feature enabled in the native build. Temp views, global temp views, and many DDL/DML statements are supported. See [PySpark differences](PYSPARK_DIFFERENCES.md) for gaps.

### How do I run the same tests against PySpark and Sparkless?

Use the `sparkless.testing` pytest plugin. See [Testing guide](TESTING_GUIDE.md).

## Parity and bugs

### How complete is PySpark parity?

200+ JSON parity fixtures plus a large pytest suite. Status: [Parity status](PARITY_STATUS.md). Known differences: [PySpark differences](PYSPARK_DIFFERENCES.md).

### Where do I report a parity bug?

[GitHub Issues](https://github.com/eddiethedean/robin-sparkless/issues) with a minimal PySpark vs Sparkless repro.

## Contributing

See [CONTRIBUTING.md](https://github.com/eddiethedean/robin-sparkless/blob/main/CONTRIBUTING.md).

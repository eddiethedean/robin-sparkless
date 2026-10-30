# Troubleshooting Python

Start by recording the exact interpreter, distribution, and native module:

```bash
python -m pip show sparkless
python -c "import sys, sparkless; print(sys.executable, sys.version); print(sparkless.__version__, sparkless.__file__)"
python -c "import sparkless._native as native; print(native.__file__)"
```

## Installation or native import fails

Use `python -m pip`, so installation targets the interpreter you actually run.
Upgrade pip, check Python 3.11+, and match your OS, architecture, libc, and wheel
floor against [Installation](INSTALLATION.md). A `cp311-abi3` wheel supports the
stable ABI on supported Python versions 3.11–3.14. Alpine requires musl wheels;
a glibc wheel cannot substitute. Reinstall the **same intended version** with
`python -m pip install --force-reinstall 'sparkless==VERSION'` after replacing VERSION.
Do not downgrade to an unrelated minor release as a generic remedy.

When no wheel matches, follow the complete [contributor setup](https://github.com/eddiethedean/robin-sparkless/blob/main/CONTRIBUTING.md#setup).
It creates a virtual environment, installs dependencies, and builds with the
pinned Rust toolchain. Native compilation also needs a C/C++ linker (Xcode command
line tools on macOS, MSVC build tools on Windows, compiler/build tools on Linux).
Rebuild after Rust changes. `maturin develop` outside an environment is insufficient.

## An API differs from PySpark

Check the [surface contract](SUPPORT_MATRIX.md) and reproduce on real PySpark
using `SPARKLESS_TEST_MODE=pyspark` and the installed testing plugin. Include
nulls, empty inputs, and explicit schemas. Typed scalar UDFs are supported locally;
pandas/vectorized/remote UDFs are not. See [UDF guide](UDF_GUIDE.md).

For SQL failures, reduce the statement to the documented subset. For database
failures, verify compiled features **before** credentials/network reachability;
installing `sparkless[jdbc]` does not add native drivers to an existing wheel.

## Backend, multiprocessing, or CI differs

The selector is `SPARKLESS_TEST_MODE`, not `SPARKLESS_TEST_BACKEND`. Run the
backend-identity example in [Testing](TESTING_GUIDE.md). Spark3.5 needs a compatible
Java runtime; Spark4 needs Java17+. Pin the engine, Python dependencies, and Java
version consistently. Use separate environments for Spark3 and Spark4.

With fork-based workers, configure multiprocessing before Polars use as described
in [configuration](CONFIGURATION.md). Prefer the packaged pytest plugin. Shared
sessions retain state: use unique view/table names, or disable shared sessions.
Check working directory, fixture paths, timezone, and native module provenance
before attributing a CI failure to the engine.

## Resource exhaustion or service failure

`collect`, `toPandas`, `toArrow`, and Python UDFs materialize data. Restrict input,
project early, and use bounded Arrow batches where appropriate. Batch byte limits
are not process-memory isolation. A timeout does not prove a mutation failed:
query its operation identity on the same instance. See [operations](PRODUCTION.md).

Report a minimal reproduction through [support](SUPPORT.md), including versions,
platform, backend identity, expected result from Spark, and the complete error.
Do not include secrets or sensitive input data.

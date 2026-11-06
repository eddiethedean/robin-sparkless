# Sparkless

**PySpark-style DataFrames. Native Rust execution. No JVM.**

> **Release candidate:** 4.20.0 has not been tagged or published yet. Use the
> latest published version until the 4.20.0 packages are available.

[![CI](https://github.com/eddiethedean/robin-sparkless/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/robin-sparkless/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/sparkless.svg)](https://pypi.org/project/sparkless/)
[![Documentation](https://readthedocs.org/projects/robin-sparkless/badge/?version=latest)](https://robin-sparkless.readthedocs.io/en/latest/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Sparkless runs familiar DataFrame and SQL code locally using Rust and Polars.
Use it for unit tests, CI, and local pipelines without installing Java or
starting a Spark cluster.

## Get started

Requires CPython **3.11–3.14** for the published native wheels. Package metadata
declares Python 3.11 or newer, so later Python versions may install from source
when a compatible native wheel is not available. See the
[platform table](docs/INSTALLATION.md#platforms-and-build-features).

```bash
python -m pip install "sparkless==4.20.0"
```

```python
from sparkless.sql import SparkSession, functions as F

spark = SparkSession.builder.app_name("example").get_or_create()
df = spark.createDataFrame([("Alice", 25), ("Bob", 30)], ["name", "age"])
df.filter(F.col("age") >= 30).select("name").show()
spark.stop()
```

Sparkless supports common transformations, joins, aggregations, windows, SQL,
and file I/O. The [testing guide](docs/TESTING_GUIDE.md) shows how to run the
same pytest tests with Sparkless and real PySpark.

It runs on one machine and implements a subset of PySpark behavior. Read
[Before you adopt](docs/BEFORE_YOU_ADOPT.md) for compatibility and limitations.

## Explore

| Guide | What you'll find |
| --- | --- |
| [Python getting started](docs/python_getting_started.md) | Installation, platforms, extras, and examples |
| [Python API](docs/PYTHON_API_REFERENCE.md) | DataFrames, functions, SQL, and Arrow export |
| [Testing](docs/TESTING_GUIDE.md) | One test suite with Sparkless and PySpark |
| [Spark Connect server](docs/SPARK_CONNECT.md) | Run the pinned PySpark Connect client against the supported Sparkless plan subset |
| [DuckDB table sources](docs/DUCKDB_SOURCE.md) | Read a local DuckDB test database through Spark Connect |
| [DuckDB Connect execution](docs/DUCKDB_EXECUTION.md) | Execute the documented Spark Connect subset in DuckDB |
| [4.20 migration](docs/MIGRATION_4_20.md) | Keep the default backend or opt into DuckDB execution |
| [4.19 migration](docs/python_migration.md#from-sparkless-418-to-419) | Choose the split packages and update imports |
| [Rust quickstart](docs/QUICKSTART.md) | Embed the `robin-sparkless` engine |

Full documentation: [Read the Docs](https://robin-sparkless.readthedocs.io/en/latest/).

## Contribute

See [CONTRIBUTING.md](CONTRIBUTING.md) for setup and checks. Report bugs in
[GitHub Issues](https://github.com/eddiethedean/robin-sparkless/issues).
[Changelog](CHANGELOG.md) · [Release guide](docs/RELEASING.md) · [MIT license](LICENSE).

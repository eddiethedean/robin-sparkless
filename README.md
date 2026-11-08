# Sparkless

**PySpark-style DataFrames. Native Rust execution. No JVM.**

[![CI](https://github.com/eddiethedean/robin-sparkless/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/robin-sparkless/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/sparkless.svg)](https://pypi.org/project/sparkless/)
[![Documentation](https://readthedocs.org/projects/robin-sparkless/badge/?version=latest)](https://robin-sparkless.readthedocs.io/en/latest/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Sparkless runs familiar DataFrame and SQL code locally using Rust and Polars.
Use it for unit tests, CI, and local pipelines without installing Java or
starting a Spark cluster.

## Get started

Install Sparkless 4.20.0 in a Python **3.11+** virtual environment. See the
[platform table](docs/INSTALLATION.md#platforms-and-build-features) for native
wheel targets and source build requirements.

```bash
python -m pip install "sparkless==4.20.0"
```

Sparkless 4.20.0 is published. See the [release evidence](docs/RELEASE_EVIDENCE.md)
for the tagged source, CI results, and published Python, Rust, and container artifacts.

```python
from sparkless.sql import SparkSession, functions as F

spark = SparkSession.builder.app_name("example").get_or_create()
df = spark.createDataFrame([("Alice", 25), ("Bob", 30)], ["name", "age"])
df.filter(F.col("age") >= 30).select("name").show()
spark.stop()
```

The example prints Bob. Sparkless runs on one machine and implements a subset
of PySpark. Check [compatibility and limitations](docs/BEFORE_YOU_ADOPT.md)
before relying on it for your workload.

## Choose a workflow

| I want to… | Start here |
| --- | --- |
| Run Python DataFrames and SQL locally | [Python API](docs/PYTHON_API_REFERENCE.md) · [Runnable examples](examples/python/README.md) |
| Run one pytest suite with Sparkless and PySpark | [Testing guide](docs/TESTING_GUIDE.md) |
| Point a PySpark Connect client at a local server | [Spark Connect guide](docs/SPARK_CONNECT.md) |
| Query a server over HTTP | [HTTP quickstart](docs/HTTP_API.md#start-and-query) |
| Embed the engine in Rust | [Rust quickstart](docs/QUICKSTART.md) |

## What’s new in 4.20.0

Sparkless 4.20.0 adds optional DuckDB execution for supported Spark Connect plans
in `sparkless-server-duckdb`. Both server products default to Polars.

[DuckDB execution guide](docs/DUCKDB_EXECUTION.md) ·
[Read-only DuckDB sources](docs/DUCKDB_SOURCE.md) ·
[Migration guide](docs/MIGRATION_4_20.md) ·
[Release evidence](docs/RELEASE_EVIDENCE.md)

## Contribute

See [CONTRIBUTING.md](CONTRIBUTING.md) for setup and checks. Report bugs in
[GitHub Issues](https://github.com/eddiethedean/robin-sparkless/issues).
[Full documentation](https://robin-sparkless.readthedocs.io/en/latest/) ·
[Changelog](CHANGELOG.md) · [Release guide](docs/RELEASING.md) · [MIT license](LICENSE).

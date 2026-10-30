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

Requires **Python 3.11+**:

```bash
python -m pip install "sparkless>=4,<5"
```

```python
from sparkless.sql import SparkSession, functions as F

spark = SparkSession.builder.app_name("example").get_or_create()
df = spark.createDataFrame([("Alice", 25), ("Bob", 30)], ["name", "age"])
df.filter(F.col("age") >= 30).select("name").show()
spark.stop()
```

Sparkless supports common transformations, joins, aggregations, windows, SQL,
and file I/O. The `sparkless.testing` plugin lets you run the same pytest tests
with Sparkless and real PySpark.

It runs on one machine and implements a subset of PySpark behavior. Read
[Before you adopt](docs/BEFORE_YOU_ADOPT.md) for compatibility and limitations.

## Explore

| Guide | What you'll find |
| --- | --- |
| [Python getting started](docs/python_getting_started.md) | Installation, platforms, extras, and examples |
| [Python API](docs/PYTHON_API_REFERENCE.md) | DataFrames, functions, SQL, and Arrow export |
| [Testing](docs/TESTING_GUIDE.md) | One test suite with Sparkless and PySpark |
| [Spark Connect server](docs/SPARK_CONNECT.md) | Native server setup and the 4.18 supported subset |
| [Rust quickstart](docs/QUICKSTART.md) | Embed the `robin-sparkless` engine |

Full documentation: [Read the Docs](https://robin-sparkless.readthedocs.io/).

**Rust 4.18.0 security note:** ordinary crates.io consumers need the
[pinned compression patches](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/security/zstd-4.18-exception.md)
for untrusted compressed input. The temporary advisory exception expires 2026-11-26.

## Contribute

See [CONTRIBUTING.md](CONTRIBUTING.md) for setup and checks. Report bugs in
[GitHub Issues](https://github.com/eddiethedean/robin-sparkless/issues).
[Changelog](CHANGELOG.md) · [Release guide](docs/RELEASING.md) · [MIT license](LICENSE).

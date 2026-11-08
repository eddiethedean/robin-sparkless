# Sparkless

**PySpark-style DataFrames in Python. No JVM required.**

[![PyPI](https://img.shields.io/pypi/v/sparkless.svg)](https://pypi.org/project/sparkless/)
[![Python versions](https://img.shields.io/pypi/pyversions/sparkless.svg)](https://pypi.org/project/sparkless/)
[![Documentation](https://readthedocs.org/projects/robin-sparkless/badge/?version=latest)](https://robin-sparkless.readthedocs.io/en/latest/)

Sparkless brings familiar DataFrame and SQL APIs to local development and
testing. Its Rust/Polars engine runs your transformations without Java or a
Spark cluster.

## Install

Install in a virtual environment with Python **3.11 or newer**. Sparkless
installs the matching `sparkless-native` engine automatically. Check the
[platform table](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/INSTALLATION.md#platforms-and-build-features)
for native wheel targets and source build requirements.

```bash
python -m pip install "sparkless==4.20.0"
```

Version 4.20.0 is the current published release. See the
[release evidence](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/RELEASE_EVIDENCE.md)
for its source tag and package inventories.

## Quick start

```python
from sparkless.sql import SparkSession, functions as F

spark = SparkSession.builder.app_name("example").get_or_create()
df = spark.createDataFrame([("Alice", 25), ("Bob", 30)], ["name", "age"])
df.filter(F.col("age") >= 30).select("name").show()
spark.stop()
```

The example prints Bob. Use the same session for joins, aggregations, windows,
SQL, and file I/O. Sparkless runs on one machine and implements a subset of
PySpark. See [compatibility and limitations](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/BEFORE_YOU_ADOPT.md).

## Related packages

| Workflow | Install |
| --- | --- |
| Run the same pytest suite with Sparkless and local PySpark | `sparkless[testing]==4.20.0` |
| Start a local Polars server and a Spark Connect session | `sparkless-server[connect]==4.20.0` |
| Use the DuckDB server product | `sparkless-server-duckdb[connect]==4.20.0` |
| Connect to a server you already run | `sparkless-connect[client]==4.20.0` |

The server packages have their own wheel targets. Check the
[installation matrix](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/INSTALLATION.md#choose-a-package)
before adding a server dependency.

## Learn more

| Guide | What you'll find |
| --- | --- |
| [Getting started](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/python_getting_started.md) | Installation, extras, and examples |
| [Python API](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/PYTHON_API_REFERENCE.md) | DataFrames, functions, SQL, and Arrow export |
| [Testing](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/TESTING_GUIDE.md) | Run the same tests with Sparkless and PySpark |
| [Spark Connect](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/SPARK_CONNECT.md) | Native server setup and supported plans |

[Full documentation](https://robin-sparkless.readthedocs.io/) ·
[Source and contributing](https://github.com/eddiethedean/robin-sparkless/blob/main/CONTRIBUTING.md) ·
[Issues](https://github.com/eddiethedean/robin-sparkless/issues) ·
[MIT license](https://github.com/eddiethedean/robin-sparkless/blob/main/LICENSE).

# Sparkless

**PySpark-style DataFrames in Python. No JVM required.**

[![PyPI](https://img.shields.io/pypi/v/sparkless.svg)](https://pypi.org/project/sparkless/)
[![Python versions](https://img.shields.io/pypi/pyversions/sparkless.svg)](https://pypi.org/project/sparkless/)
[![Documentation](https://readthedocs.org/projects/robin-sparkless/badge/?version=latest)](https://robin-sparkless.readthedocs.io/en/latest/)

Sparkless brings familiar DataFrame and SQL APIs to local development and
testing. Its Rust/Polars engine runs your transformations without Java or a
Spark cluster.

## Install

Requires **Python 3.11+**:

```bash
python -m pip install "sparkless>=4,<5"
```

## Quick start

```python
from sparkless.sql import SparkSession, functions as F

spark = SparkSession.builder.app_name("example").get_or_create()
df = spark.createDataFrame([("Alice", 25), ("Bob", 30)], ["name", "age"])
df.filter(F.col("age") >= 30).select("name").show()
spark.stop()
```

Use common transformations, joins, aggregations, windows, SQL, and file I/O.
For tests, `sparkless.testing` lets you run one pytest suite with Sparkless or
real PySpark.

Sparkless runs on one machine and implements a subset of PySpark behavior.
See [Before you adopt](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/BEFORE_YOU_ADOPT.md)
for compatibility and limitations.

### Supported platforms

Prebuilt wheels cover Linux, macOS, and Windows. See the
[platform guide](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/python_getting_started.md#supported-platforms)
and [PyPI files](https://pypi.org/project/sparkless/#files) for your release.

## Learn more

| Guide | What you'll find |
| --- | --- |
| [Getting started](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/python_getting_started.md) | Installation, extras, and examples |
| [Python API](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/PYTHON_API_REFERENCE.md) | DataFrames, functions, SQL, and Arrow export |
| [Testing](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/TESTING_GUIDE.md) | Run the same tests with Sparkless and PySpark |
| [Spark Connect](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/SPARK_CONNECT.md) | Native server setup and the 4.18 supported subset |

[Full documentation](https://robin-sparkless.readthedocs.io/) ·
[Source and contributing](https://github.com/eddiethedean/robin-sparkless/blob/main/CONTRIBUTING.md) ·
[Issues](https://github.com/eddiethedean/robin-sparkless/issues) ·
[MIT license](https://github.com/eddiethedean/robin-sparkless/blob/main/LICENSE).

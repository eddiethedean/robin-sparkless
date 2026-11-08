---
hide:
  - navigation
  - toc
---

<div class="sparkless-hero" markdown>

<div class="sparkless-hero__intro" markdown>

<p class="sparkless-eyebrow">Python · Spark Connect · Rust</p>

# Run DataFrame workloads locally **without a JVM**

Sparkless runs PySpark-style DataFrame and SQL code on a single machine using
Rust and Polars. Use its local Python API without Java, keep the PySpark client
for supported remote plans, or embed the engine in a Rust application.

[Install Sparkless](INSTALLATION.md){ .md-button .md-button--primary }
[See what’s supported](SUPPORT_MATRIX.md){ .md-button }

</div>

<div class="sparkless-hero__example" markdown>

<p class="sparkless-eyebrow">A local DataFrame test</p>

```python
from sparkless.sql import (
    SparkSession,
)

builder = SparkSession.builder
spark = builder.get_or_create()
try:
    rows = spark.range(5)
    assert rows.count() == 5
finally:
    spark.stop()
```

</div>

</div>

## Choose how to run your code

<div class="grid cards sparkless-paths" markdown>

-   **[Run DataFrames locally](python_getting_started.md)**

    Use `sparkless.sql` for local Python development, DataFrame tests, and file pipelines.

-   **[Check results against PySpark](TESTING_GUIDE.md)**

    Run the same pytest assertions on local Sparkless and a JVM-backed PySpark reference.

-   **[Connect with the PySpark client](SPARK_CONNECT.md)**

    Keep the `pyspark.sql` client and send supported plans to a native Sparkless server.

-   **[Embed the Rust engine](QUICKSTART.md)**

    Add `robin-sparkless` to a Rust application and select the Cargo features you need.

</div>

## Current release: 4.20.0

Version 4.20.0 is published. See the [release record](RELEASE_EVIDENCE.md) for
the verified source tag and public artifacts.

- **Execute Connect plans in DuckDB.** Opt in on the separate DuckDB server;
  Polars remains the default and unsupported plans fail explicitly.
  [Execution guide](DUCKDB_EXECUTION.md) · [migration notes](MIGRATION_4_20.md)
- **Compare schemas fully.** `sparkless-testing` now checks nullability,
  metadata, decimals, nested types, and declared schemas for empty results.
  [Testing comparison contract](TESTING_PYTHON_API.md#comparison-semantics)
- **Improve bounded server work.** HTTP response encoding and Connect scheduling
  reduce avoidable copies and keep health work progressing during blocked sends.
  [Performance guide](PERFORMANCE.md)

## Package layout introduced in 4.19

`SparklessConnect` starts the native server and creates a fresh PySpark Connect
session in one `with` block. The base server, DuckDB server, shared launcher,
and pytest helpers are separate packages. See [server sessions](SPARK_CONNECT.md#start-a-server-and-session),
[DuckDB sources](DUCKDB_SOURCE.md), and the [package matrix](INSTALLATION.md#choose-a-package).

## What to check before adopting Sparkless

Sparkless implements a subset of PySpark on a single machine. The in-process
API and Spark Connect support different operations. Compare representative
inputs, schemas, and results with your production Spark version when exact
behavior matters. [Before you adopt](BEFORE_YOU_ADOPT.md)

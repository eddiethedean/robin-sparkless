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

## New in 4.19.0

- **Own a server and a session together.** `SparklessConnect` starts the native
  server and creates a fresh PySpark Connect session in one `with` block.
  [Start a session](SPARK_CONNECT.md#start-a-server-and-session)
- **Read DuckDB test data.** The optional DuckDB server exposes configured
  tables as read-only sources and runs supported transformations in Polars.
  [Query a DuckDB file](DUCKDB_SOURCE.md)
- **Install the components you use.** The base server, DuckDB server, shared
  launcher, and pytest helpers are separate packages.
  [Compare the packages](INSTALLATION.md#choose-a-package) ·
  [Upgrade from 4.18](python_migration.md#from-sparkless-418-to-419)

## Unreleased 4.20 source candidate

The repository now contains an optional DuckDB Connect execution path and
performance/schema-comparison fixes planned for 4.20.0. The public packages
remain at 4.19.0: Spark-result conformance, owner/restart behavior, performance
measurements, and release artifact checks are still open. Review the
[candidate execution guide](DUCKDB_EXECUTION.md) and [phase plan](plans/sparkless-server/15-duckdb-connect-backend.md);
do not infer these capabilities from the published install.

## What to check before adopting Sparkless

Sparkless implements a subset of PySpark on a single machine. The in-process
API and Spark Connect support different operations. Compare representative
inputs, schemas, and results with your production Spark version when exact
behavior matters. [Before you adopt](BEFORE_YOU_ADOPT.md)

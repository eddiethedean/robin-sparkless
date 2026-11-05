# Run PySpark tests over Spark Connect

Use the real PySpark Connect client with a Sparkless server running on one
machine. The server executes supported DataFrame plans in Rust and Polars.
This setup needs no Java installation or Spark cluster.

!!! info "Choose the guide for your version"

    The examples below use **4.19.0**. Sparkless shipped the server in 4.18. In that release,
    use `SparklessServer` and create the client session yourself. Follow the
    [published 4.18.1 Connect guide](https://github.com/eddiethedean/robin-sparkless/blob/v4.18.1/docs/SPARK_CONNECT.md)
    for the matching install and imports.

## Start a server and session

Install the base server and its pinned `pyspark-client==4.0.5` dependency from PyPI.
Packaged server wheels target Linux x86_64 with glibc 2.28+ and macOS 14+
on Apple Silicon.

```bash
python -m pip install "sparkless-server[connect]==4.19.0"
```

```python
from sparkless_server import SparklessConnect

with SparklessConnect() as spark:
    assert spark.range(10).filter("id > 5").count() == 4
```

`SparklessConnect` starts a fresh server on loopback, waits for readiness, and
returns an independent PySpark Connect session. On exit it attempts to stop the
session before shutting down the server. Use a new helper in each process.
An active instance cannot be nested, and failed cleanup can retain ownership
for retry.

For read-only DuckDB tables, use the [DuckDB server product](DUCKDB_SOURCE.md)
and import `SparklessConnect` from `sparkless_server_duckdb`.

## Connect to a server you already run

Use this pattern for a separately managed server or when several test workers
share a server. Start the server once outside the workers and give each worker
the endpoint URL. Each worker creates and stops its own session.

```bash
python -m pip install "sparkless-connect[client]==4.19.0"
```

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://127.0.0.1:15002").create()
try:
    assert spark.range(10).filter("id > 5").count() == 4
finally:
    spark.stop()
```

When you own the process but want to manage the session yourself, use
`SparklessServer` from the chosen server package. Its `spark_connect_url` and
`http_url` are available once ready. `AsyncSparklessServer` supports
`async with`. The Python launchers default to ephemeral loopback ports.
Pass `connect_port=None` to run an HTTP-only server.

For a manually managed binary built with the `connect` Cargo feature, set
`--connect-host` and `--connect-port` on the `serve` command. The default Connect
address is `127.0.0.1:15002`. See the [deployment runbook](PRODUCTION.md) for
network boundaries and readiness checks.

To launch the packaged binary yourself, install the base server's Connect extra
and run this in **Terminal 1**:

```bash
python -m pip install "sparkless-server[connect]==4.19.0"
server_bin="$(python -c 'from sparkless_server import executable_path; print(executable_path())')"
"$server_bin" serve --connect-host 127.0.0.1 --connect-port 15002 --disable-http
```

In **Terminal 2**, run the remote-session example above. For a separate client
environment, install `sparkless-connect[client]==4.19.0`. Stop the server with
Ctrl+C. The Connect extra pins `pyspark-client==4.0.5`; Java is not required.

## Share one server across pytest workers

Copy [the example directory](https://github.com/eddiethedean/robin-sparkless/tree/main/examples/connect_xdist)
including `pytest.ini`, `conftest.py`, and `test_orders.py`. Run these commands
from that directory:

```bash
python -m pip install "sparkless-server[connect]==4.19.0" pytest pytest-xdist
python -m pytest -n 2 test_orders.py
```

Expect two passing tests. The pytest controller starts one server and passes its
URL to each worker through `workerinput`. Each worker creates its own Connect
session and stops it at session teardown. Pytest then stops the controller's
server. The same recipe also runs without `-n`.

Worker sessions share server admission and resource budgets. Use distinct
resource names for any shared state. The default active operation limit is two
across the server, even when more workers connect. See the
[launcher API](SERVER_PYTHON_API.md) for lifecycle and error handling.

## Supported request surface

| Area | Supported in 4.19.0 |
| --- | --- |
| Relations | `SparkSession.range`, bounded inline scalar DataFrames with explicit schemas, and configured named DuckDB tables with the source-enabled server |
| Transformations | Filter, projection, alias, limit, and supported inner joins |
| Aggregation | Global row and non-null-column counts, grouped count and sum |
| Analysis | Supported relation schemas, scalar schemas, and bounded DDL analysis |
| Configuration | Spark 4.0.5 session profile. Same-value sets are idempotent and unsupported changes fail. |
| Results | Spark logical descriptors and bounded, independently decodable Arrow IPC batches |
| Lifecycle | Execute, reattach, prefix/all release, session release, interrupt, enriched error lookup, and stale identity rejection |

## Unreleased 4.20 DuckDB execution candidate

The published 4.19.0 products still execute through Polars, including when they
read configured DuckDB source tables. A separate source implementation for
4.20 adds an opt-in DuckDB Connect execution backend, typed table creation, and
`INSERT ... VALUES`. Its supported surface, startup settings, owner modes, and
open Spark/interoperability gates are documented in the
[DuckDB execution candidate guide](DUCKDB_EXECUTION.md). Do not use that page as
the 4.19.0 support contract.

Use public column names or column references bound to their input DataFrames.
Bound references retain their input through aliases and joins. Joins on shared
column names return one column per key, in the requested key order.
Filters support `column > integer` strings and the `>=` column expression.
Equality between column references is supported in inner join conditions.
A missing relation, projection expression, or projection literal type returns
`INVALID_ARGUMENT`. An unsupported variant returns `UNIMPLEMENTED`.

Supported logical values are deliberately limited:

| Spark logical type | Arrow physical type |
| --- | --- |
| BOOLEAN | `bool` |
| SMALLINT | `int16` |
| INT | `int32` |
| BIGINT | `int64` |
| finite DOUBLE | `double` |
| STRING | `string` |
| DATE | `date32[day]` |
| TIMESTAMP under the UTC session profile | `timestamp[us, tz=UTC]` |
| DECIMAL(p, s), where 1 ≤ p ≤ 38 and 0 ≤ s ≤ p | `decimal128(p, s)` |

Field labels, nullability, and supported metadata are preserved through the
checked Arrow path. Empty results carry their schema.

## Limits and unsupported operations

Connect plans share the server's process-wide execution admission with HTTP, plus bounded result
and replay capacity. The selected defaults are Sparkless policies, not Apache Spark defaults or
hard process-memory guarantees:

| Limit | Default |
| --- | ---: |
| Active operations across the process | 2 |
| Pending operations across the process | 16 |
| Decoded inline rows / scalar value | 10,000 / 512 KiB |
| Arrow response rows / serialized bytes | 16,384 / 1 MiB |
| Result rows / serialized bytes per operation | 1,000,000 / 64 MiB |
| Live gRPC response streams across the process | 32 |
| Connect operation-history entries / metadata bytes across the process | 1,024 / 4 MiB |
| Retained replay bytes per operation / across the process | 8 MiB / 32 MiB |
| Detached-operation retention | 60 seconds |

Capacity exhaustion and unsupported plans return errors; the server does not report a successful
truncated result. Native CPU time and intermediate memory remain cooperative. Session catalogs and
operation records are in memory and disappear on restart. Reattachment is bounded by the retention
and output budgets and is not a durable exactly-once guarantee.

Spark Connect SQL commands, writes, artifacts, UDFs, Structured Streaming, filesystem/network
readers, unsupported expressions and logical types, and arbitrary plans are outside this contract.
The service returns an explicit error for unsupported operations. Connect user IDs partition
session state; they do not authenticate clients. The server does not provide Connect
authentication by itself. Keep the listener on loopback unless deployment supplies an
authenticated TLS boundary in front of it.

## Verification

The [4.19 development evidence](plans/sparkless-server/RELEASE_4_19_EVIDENCE.md)
records the split-product wheel, clean-install, source smoke, and Spark
4.0.5 differential checks. For earlier results see the
[release evidence](RELEASE_EVIDENCE.md). The MiniLake proxy route requires
separate validation of authentication, routing, and lifecycle behavior.

# Spark Connect server

The 4.18.0 native server distribution adds an optional Spark Connect endpoint that speaks the
Apache Spark 4.0.5 protocol. It runs Rust/Polars execution without a Python worker or JVM in the
server process. This page describes the 4.18 capability contract; the complete candidate and
public-artifact gates remain the release authority.

## Start an owned server

Install the native server extra and the standalone public client in the client environment:

```sh
python -m pip install "sparkless[server]==4.18.0" "pyspark-client==4.0.5"
```

These commands target the 4.18.0 distribution. Before publication, install the
matching candidate wheels instead of downloading 4.18.0 from PyPI. The packaged
server wheels support Linux x86_64 with glibc 2.28+ and macOS 14+ on Apple Silicon;
this is a smaller platform set than the in-process Python extension.

The launcher owns a native server process, waits for its readiness record, and allocates loopback
ports for HTTP and gRPC:

```python
from pyspark.sql import SparkSession
from sparkless import SparklessServer

with SparklessServer() as server:
    spark = SparkSession.builder.remote(server.spark_connect_url).create()
    try:
        assert spark.range(10).filter("id > 5").count() == 4
    finally:
        spark.stop()
```

`AsyncSparklessServer` supports the same lifecycle with `async with`. Set `connect_port=None` to
disable gRPC; reading `spark_connect_url` then raises `UnsupportedSparkConnectError`. The installed
server binary also supports `--disable-connect` and `--disable-http`. Readiness publishes each
endpoint only after its socket is bound and includes the server instance identity and versioned
capability record. The launcher does not import PySpark, download a binary, or compile a server.

For a manually managed server, compile the `connect` Cargo feature and use `--connect-host` and
`--connect-port`. The CLI defaults to `127.0.0.1:15002`; port `0` asks the OS for an available
port. Keep listeners on loopback unless deployment configuration supplies an explicit network
trust boundary.

## Supported request surface

The server executes the declared DataFrame subset through its native engine:

| Area | Supported in the 4.18 contract |
| --- | --- |
| Relations | `SparkSession.range`, bounded scalar local relations with explicit schemas |
| Transformations | Filter, projection/alias, and limit for supported expressions |
| Aggregation | Global `count(1)` and the range/filter/count witness shown above |
| Analysis | Supported relation schemas, scalar schemas, bounded DDL analysis |
| Configuration | Captured Spark 4.0.5 session profile; same-value sets are idempotent and unsupported changes fail |
| Results | Spark Connect logical descriptors and bounded, independently decodable Arrow IPC batches |
| Lifecycle | Execute, reattach, prefix/all release, session release, interrupt, enriched error lookup, stale identity rejection |

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

Field labels, nullability and supported metadata are preserved through the checked Arrow path.
Empty results carry their schema. The 4.18 feature matrix is narrower than the Spark Connect
protocol: an operation is supported only when it is listed here and covered by the 4.18 tests.

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

The optional MiniLake proxy route is not advertised as a supported 4.18 transport. A locally
available route harness or direct-server success does not certify authentication, cluster routing,
or lifecycle behavior of a third-party proxy.

## Verification

The 4.18 release candidate runs the pinned public client against the Spark 4.0.5 reference for the
value, schema, configuration, retry, lifecycle and error corpus. The candidate also verifies
Connect-enabled native wheels, the owned launcher, and the native container. See the [4.18 release
plan](plans/sparkless-server/RELEASE_4_18.md) and [validation record](plans/sparkless-server/VALIDATION.md)
for candidate evidence and remaining release gates.

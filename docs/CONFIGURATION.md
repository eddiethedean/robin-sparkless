# Configuration reference

Applies to 4.18.0. Set environment hardening variables **before** creating sessions.
A Spark configuration key being retained does not mean Sparkless implements the
Spark subsystem it names. Configuration profiles adjust documented compatibility,
not the actual installed PySpark oracle version.

| Local setting | Default | Effect |
| --- | --- | --- |
| `spark.sql.warehouse.dir` | unset | Disk-backed Parquet saveAsTable warehouse; set on builder |
| `SPARKLESS_PYSPARK_COMPAT` | `3.5` | Compatibility profile; `4.0` opts into documented Spark4 behavior |
| `SPARKLESS_TEST_MODE` | `sparkless` | Test fixture engine; use `pyspark` explicitly for Java-backed comparison |
| `SPARKLESS_SHARED_SESSION` | off | Reuses local test sessions; disabled for xdist/PySpark/Delta |
| `SPARKLESS_HARDENED` | off | Enables hardened defaults |
| `SPARKLESS_JDBC_ALLOW_ARBITRARY_SQL` | true normally | `false` blocks arbitrary JDBC query/init/prepare SQL; use dbtable |
| `SPARKLESS_FILES_BASE` | unset | Confines general file reads/writes under a configured root |
| `SPARKLESS_MAX_RANGE_ROWS` | 10,000,000 | Local range row cap |
| `SPARKLESS_MAX_CROSS_JOIN_ROWS` | 10,000,000 | Non-equality join materialization cap |
| `SPARKLESS_RDD_MAX_ROWS` | 10,000,000 | Local RDD materialization cap; not distributed execution |
| `ROBIN_SPARKLESS_MULTIPROCESSING` | unset | Configure process behavior before Polars use; prefer testing plugin |

See [compatibility profiles](PYSPARK_COMPAT_PROFILES.md) for exact profile
semantics and [testing](TESTING_GUIDE.md) for markers/fixtures. For explicit
multiprocessing setup, use `sparkless._configure_for_multiprocessing()` before
native work; forking after native threads start is unsafe as an application design.

## Native HTTP server defaults

Environment integers must be positive; zero is not an “unlimited” switch. CLI
listener flags override environment settings; CLI port0 requests an ephemeral
port. Query `/capabilities` after launch for the effective limits.

| Environment variable | Default |
| --- | ---: |
| `SPARKLESS_SERVER_HOST` | 127.0.0.1 |
| `SPARKLESS_SERVER_PORT` | 8099 |
| `SPARKLESS_MAX_REQUEST_BYTES` | 1,048,576 |
| `SPARKLESS_MAX_SQL_BYTES` | 262,144 |
| `SPARKLESS_MAX_RESULT_ROWS` | 10,000 |
| `SPARKLESS_MAX_RESULT_BYTES` | 8,388,608 |
| `SPARKLESS_MAX_CELL_BYTES` | 1,048,576 |
| `SPARKLESS_MAX_ACTIVE_OPERATIONS` | 2 |
| `SPARKLESS_MAX_PENDING_OPERATIONS` | 16 |
| `SPARKLESS_MAX_SESSIONS` | 64 |
| `SPARKLESS_SESSION_IDLE_TTL_MS` | 900,000 |
| `SPARKLESS_MAX_MUTATION_RECORDS` | 1,024 |
| `SPARKLESS_MAX_MUTATION_BYTES` | 4,194,304 |
| `SPARKLESS_BODY_READ_TIMEOUT_MS` | 10,000 |
| `SPARKLESS_QUEUE_TIMEOUT_MS` | 5,000 |
| `SPARKLESS_RESPONSE_TIMEOUT_MS` | 30,000 |

Contradictory byte budgets fail startup. The reserved `sql-warehouse` session is
exempt from idle expiry. These bounds do not cap query intermediate memory/CPU;
Connect also has independent replay/live-stream limits listed in its guide.
Source authority: [server configuration](https://github.com/eddiethedean/robin-sparkless/blob/main/crates/sparkless-server/src/config.rs).
Operate within a trusted boundary using [the runbook](PRODUCTION.md).

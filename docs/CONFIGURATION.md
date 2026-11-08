# Configuration reference

Affects the local engine and the native server as shown below. Set hardening
environment variables before creating sessions or launching the server.
A Spark configuration key being retained does not mean Sparkless implements the
Spark subsystem it names. Configuration profiles adjust documented compatibility,
not the actual installed PySpark oracle version.

| Local setting | Default | Effect |
| --- | --- | --- |
| `spark.sql.warehouse.dir` | unset | Disk-backed Parquet saveAsTable warehouse; set on builder |
| `SPARKLESS_PYSPARK_COMPAT` | `3.5` | Compatibility profile; `4.0` opts into documented Spark4 behavior |
| `SPARKLESS_TEST_MODE` | `sparkless` | Test fixture engine; use `pyspark` explicitly for Java-backed comparison |
| `SPARKLESS_SHARED_SESSION` | off | Reuses local test sessions; disabled for xdist/PySpark/Delta |
| `SPARKLESS_HARDENED` | off | Exact value `1` disables arbitrary JDBC SQL by default; does not enable file confinement automatically |
| `SPARKLESS_JDBC_ALLOW_ARBITRARY_SQL` | true normally | `false` blocks arbitrary JDBC query/init/prepare SQL; use dbtable |
| `SPARKLESS_FILES_BASE` | unset | Confines general file reads/writes under a configured root |
| `SPARKLESS_MAX_RANGE_ROWS` | 10,000,000 | Local range row cap |
| `SPARKLESS_MAX_CROSS_JOIN_ROWS` | 10,000,000 | Non-equality join materialization cap |
| `SPARKLESS_RDD_MAX_ROWS` | 10,000,000 | Local RDD materialization cap; not distributed execution |
| `SPARKLESS_SQLITE_BASE` | unset | Resolve SQLite database basenames under this existing root; separate from general file confinement |
| `SPARKLESS_ENABLE_DELTA` | off | Test-fixture Delta setup (`1`/`true`/`yes`); does not compile a native feature |
| `ROBIN_SPARKLESS_MULTIPROCESSING` | unset | Used by the multiprocessing helper; call that helper before native work |

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
| `SPARKLESS_DUCKDB_SOURCES` | Unset. Configure read-only source aliases on the [4.19 DuckDB server](DUCKDB_SOURCE.md). |
| `SPARKLESS_EXECUTION_BACKEND` | `polars`. Set to `duckdb` on `sparkless-server-duckdb` to select the 4.20.0 Connect backend. |
| `SPARKLESS_DUCKDB_EXECUTION_OWNER` | Unset. Required JSON owner config when `SPARKLESS_EXECUTION_BACKEND=duckdb`; see [DuckDB execution](DUCKDB_EXECUTION.md). |
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
Source authority: [shared server configuration](https://github.com/eddiethedean/robin-sparkless/blob/main/crates/sparkless-server-core/src/config.rs).

`SPARKLESS_DUCKDB_EXECUTION_OWNER` accepts a trusted startup JSON object with
`mode: "local"` and `database_path`, or `mode: "quack"` with `quack_uri`,
`token`, and optional TLS/extension fields. The local file must have one
Sparkless writer process. Use Quack for a shared writable owner. These settings
configure the published 4.20.0 execution backend; the base server continues to
use Polars for execution.
Operate within a trusted boundary using [the runbook](PRODUCTION.md).

## Precedence, accepted values and lifetime

| Surface | Precedence / accepted values | When to set |
| --- | --- | --- |
| Server listeners and budgets | Defaults, then environment, then CLI listener flags. Bind hosts are IP addresses, not DNS names; environment ports 1–65535, CLI 0 permits ephemeral ports | Before launch; restart to change |
| Python launcher | Constructor defaults use ephemeral loopback ports and pass explicit listener flags; these override server listener environment settings. `env` merges over the parent environment | Before `start()`; see launcher API |
| Native server integer limits | Positive base-10 integers; malformed, zero, excessive or contradictory budgets fail startup | Before launch |
| Local range/cross-join row caps | Unsigned integer; invalid text falls back to 10,000,000; zero is a zero cap, not unlimited | Keep environment fixed for the process lifetime |
| Local RDD cap | Unsigned integer; invalid text falls back to the documented default | Before materialization |
| Local compatibility profile | `3.5` or `4.0`; explicit `sparkless.pyspark.compat` config takes precedence over the profile environment default | Before session creation; later profile changes affect subsequent work, not completed results |
| Test mode | `pyspark` selects PySpark; unset/default selects Sparkless | Before pytest starts; verify backend identity |
| JDBC hardening | With `SPARKLESS_HARDENED=1`, arbitrary SQL requires allow-variable `1`/`true`; otherwise allow-variable `0`/`false` disables it | Before database work; keep fixed while work is active |
| File roots | Existing local directory; paths containing traversal or escaping the canonical root fail | Before IO; never change beneath active queries |

Connect's fixed Spark 4.0.5 configuration profile rejects unsupported changes;
setting an already-current value is idempotent. Most Connect replay/result limits
are compiled policies listed in [Connect limits](SPARK_CONNECT.md#limits-and-unsupported-operations),
not environment switches. DuckDB read-pool sizes, thread/memory limits and source
budgets are fixed policies. Do not invent `SPARKLESS_*` variables to change them.

## Rust configuration from the environment

`SparklessConfig::from_env()` reads the settings below. Ordinary Python builders
do not implicitly call this Rust configuration constructor.

| Setting | Default / effect |
| --- | --- |
| `ROBIN_SPARKLESS_WAREHOUSE_DIR` | Unset; nonempty path becomes `spark.sql.warehouse.dir` |
| `ROBIN_SPARKLESS_CASE_SENSITIVE` | false; `true` (case insensitive) or `1` enables case-sensitive names |
| `ROBIN_SPARKLESS_TEMP_DIR` | Unset; retained in config, reserved for future use, no execution effect |
| `ROBIN_SPARKLESS_CONFIG_*` | Nonempty suffix/value retained in `extra`; underscores become dots and letter case is preserved |

`to_session_config()` starts with `extra`, then overlays the warehouse and
case-sensitive fields. Use exact Spark key casing; an uppercase suffix stays
uppercase. Build the session with `SparkSession::from_config(&config)` or the
builder's `with_config`. A retained extra key does not create a Spark subsystem.

Writer batch bounds are **writer options**, not environment variables; see
[file IO](FILE_IO.md). Temporary debug/benchmark variables found in source are
not a public configuration contract. Report invalid configuration with the
exact setting name and safe value through [support](SUPPORT.md).

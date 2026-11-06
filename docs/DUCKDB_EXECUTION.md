# DuckDB Connect execution (4.20.0 candidate)

The optional `sparkless-server-duckdb` candidate runs the documented Spark
Connect SQL and DataFrame subset in DuckDB. Polars remains the default backend.
This is a single-node service, not a general Spark replacement. The 4.20.0
release has not been tagged or published. Check the [candidate support
contract](SUPPORT_MATRIX.md) and [release evidence](RELEASE_EVIDENCE.md) before
enabling it.

The adapter targets the unmodified Spark Connect 4.0.5 client and an Apache
Spark 4.0.5 reference. Unsupported operations fail explicitly; they never fall
back to Polars. The release contract lists tested result/schema cases,
unsupported operations, packaged platforms, and measured operating limits.

The optional `sparkless-server-duckdb` product can run supported Spark Connect
plans in DuckDB and store typed tables in one configured DuckDB owner. The
default backend remains Polars. Select the backend when the server starts; a
server process and its sessions use one backend for their lifetime.

## Start with a local database

Install the DuckDB server with its Spark Connect client extra and the
standalone DuckDB client used to inspect the database:

```sh
python -m pip install \
  "sparkless-server-duckdb[connect]==4.20.0" \
  "duckdb==1.5.1"
```

Point the execution owner at a persistent database file:

```sh
export SPARKLESS_EXECUTION_BACKEND=duckdb
export SPARKLESS_DUCKDB_EXECUTION_OWNER='{"mode":"local","database_path":"./sparkless-execution.duckdb"}'
```

The server creates the database file if its parent directory exists. A local
database is owned by one server process. Do not open the same file for writes
from another process; configure a shared Quack owner when independent clients
need concurrent access.

The execution owner is separate from the 4.19 read-only source configuration
`SPARKLESS_DUCKDB_SOURCES`. A server may have source aliases as well as an
execution database, but source aliases are not writable through Connect.

Save this as `run_duckdb.py` and run it in the same shell where the environment
variables are set:

```python
from sparkless_server_duckdb import SparklessConnect

with SparklessConnect() as spark:
    spark.sql("CREATE TABLE events (id BIGINT, label VARCHAR)").collect()
    spark.sql("INSERT INTO events VALUES (1, 'started'), (2, 'finished')").collect()
    rows = sorted((row.id, row.label) for row in spark.table("events").collect())
    assert rows == [(1, "started"), (2, "finished")]
```

After the context exits, inspect the local database with a separate DuckDB
process or client. Do not open a local database for writes while Sparkless owns
it.

## Use a shared Quack owner

Configure the Quack URI, token, and extension path in the service environment:

```json
{
  "mode": "quack",
  "quack_uri": "quack://database-owner.example:443/catalog",
  "token": "injected-secret",
  "extension_path": "/opt/sparkless/quack.duckdb_extension",
  "disable_ssl": false
}
```

Store the JSON value in `SPARKLESS_DUCKDB_EXECUTION_OWNER` using the deployment
platform's secret manager. Never commit a live token. The same Quack owner must
be configured for independent DuckDB clients that need to observe these tables.
Sparkless keeps the catalog name fixed and does not accept paths, credentials,
or extension names from Connect requests.

## Query execution connections

Writes and durable operation receipts use one serialized primary DuckDB
connection. Reads use a bounded pool with up to four concurrent connections
and at most 16 queued reads. A caller waiting for a read connection can be
cancelled; once the queue is full, another read receives an explicit capacity
error.

In local mode, all connections share one DuckDB database handle inside the
server process. A local database still has one writable server process; the
pool does not permit a second process to open that file for writes. In Quack
mode, read connections attach independently to the configured owner. A
successful write invalidates pooled reads so later queries see the committed
catalog and data. Quack connection failures also invalidate pooled reads.

These pool sizes are fixed limits, not user-configurable values. Slow network
sends do not retain the only connection available for unrelated reads. The
accepted 8-client workload envelope and its RSS, queue, health, and latency
ceilings are listed in the [support contract](SUPPORT_MATRIX.md#duckdb-connect-execution-limits).

## Supported request subset

The capability response reports the active backend and effective execution
limits. In DuckDB mode, the Connect adapter currently accepts:

- `range`, bounded scalar `LocalRelation`, and named tables in the configured
  execution database;
- aliases, projection, supported scalar filters, `limit`, inner equality joins
  including `USING`, and grouped `count`/`sum` (including `count(*)` and `sum`
  over a resolvable numeric input column);
- one parsed, read-only `SELECT` relation using literals, identifiers, unary and
  binary operators, `COUNT`, `SUM`, and `ABS`; simple `*` and `alias.*`
  projections; supported `WHERE` expressions including `IS NULL` and
  `IS NOT NULL`; expression `GROUP BY`; inner equality/`USING` joins and
  `CROSS JOIN`; and a nonnegative integer `LIMIT`. SQL `SUM` accepts a supported
  numeric input column. DataFrame joins accept inner equality and `USING` forms;
  DataFrame cross joins are outside the subset;
- `CREATE TABLE` with explicitly typed columns and `INSERT INTO ... VALUES`
  using scalar literals; timestamp string values may use an explicit
  `CAST('...' AS TIMESTAMP)` expression, and decimal string values may use an
  explicit `CAST('...' AS DECIMAL(p, s))` expression for exact precision and
  scale.

With the pinned PySpark 4.0.5 client, `spark.sql("SELECT ...")` stays lazy and
executes when an action such as `.collect()` is called. Supported DDL and INSERT
commands take effect during `spark.sql(...)`; the returned DataFrame represents
an empty command result, so calling `.collect()` on it is safe and does not
repeat the write.

The SQL subset rejects CTEs, `DISTINCT`, ordering, offsets, `FETCH`, `HAVING`,
windows, wildcard modifiers, table functions, table sampling and hints, lateral
or column-renamed derived tables, query settings, scalar `CAST` expressions,
and parameterized SQL. SQL casts are accepted only in the declared INSERT value
forms. `NULL` is accepted in scalar predicates, but a top-level `NULL` result is
not because Spark Connect's initial result type set has no NullType. Unsupported
clauses fail before DuckDB execution instead of being silently dropped during
planning.

The fixed Spark SQL configuration profile exposes 11 values through the public
Connect `RuntimeConfig` operations. `Get`, `GetWithDefault`, `GetAll`,
`IsModifiable`, same-value `Set`, and `Unset` are supported. These values are
read-only; changing one to a different value fails.

Create-table columns are limited to boolean, signed integer, float/double,
string, date, timestamp/timestamp-NTZ, binary, and decimal types with precision
up to 38. A bare SQL `TIMESTAMP` is stored with UTC timezone semantics;
`TIMESTAMP_NTZ` stays timezone-free. Nested, unsigned, time-of-day, UUID, and
other DuckDB types are rejected at DDL validation.

For command writes, operation receipts are inserted in the same DuckDB
transaction as the statement. Repeating the same Connect operation identity and
request returns the stored outcome without executing the write again. Reusing
an identity with different request bytes is rejected. The durable receipt table
is capped at 1,000,000 entries; the server refuses new writes when full.

The initial implementation rejects unsupported expressions and SQL explicitly.
It does not fall back to Polars. DataFrame writers, CTAS, insert-from-select,
parameterized SQL, temporary views, streaming sources, UDFs, nested/unsigned
Connect result types, and general Spark SQL are outside this surface. HTTP SQL
statement execution is disabled in DuckDB mode; health, readiness, and
capability routes remain available when the HTTP listener is enabled.

## Resource and lifecycle limits

The adapter enforces a 1,000,000-row `range` cap, LocalRelation input caps of
10,000 rows, 1 MiB encoded Arrow IPC and 8 MiB decoded data, and output batches
of at most 16,384 rows or 1 MiB. Each DuckDB connection uses one execution
thread and a 256 MiB memory limit. The one write connection, four read
connections, and 16 queued reads are fixed server limits. Existing Connect
admission, response, session, and replay limits also apply.

| Accepted operating limit | Value |
| --- | ---: |
| Workload | 8 Connect clients plus one paced 1,000,000-row reader |
| Active operations / queue timeout | 16 / 5 seconds |
| Minimum correct throughput | 5 operations per second |
| Server peak RSS | 512 MiB |
| Shared Quack owner peak RSS | 256 MiB |
| Short-query p95 / health p95 | 5 seconds / 1 second |
| Rejections and health errors | 0 during the accepted workload |
| Graceful server and owner shutdown | Exit code 0 |

The RSS, throughput, and latency limits are measured acceptance ceilings for
that workload, not promises for arbitrary SQL, data shapes, or Quack deployments.
Local-owner memory is included in server RSS; Quack owner RSS is sampled
separately. A full read pool or compute queue returns a capacity error rather
than growing without bound. Network sends allow up to 30 seconds for HTTP/2
credit to replenish; reattachable response acknowledgements have a separate
5-second deadline. See the [support contract](SUPPORT_MATRIX.md#duckdb-connect-execution-limits)
for the full product boundary.

## Persistence, retries, and shutdown

Tables and operation receipts persist in the configured database. A receipt is
committed in the same transaction as its write and is capped at 1,000,000
entries. Repeating a write with the same user, session, operation identity, and
request replays the stored outcome without reapplying it. Reusing an identity
with changed request content is rejected. When commit outcome is unknown, retain
the original request and identity; do not retry with a new identity until the
outcome is resolved.

Connect sessions, temporary state, and retained Arrow results belong to one
server instance and do not survive its restart. A durable receipt does not
restore the old session. In local mode, one Sparkless process owns the database
for writes; do not open the same file for writes in another process. Use one
shared Quack owner when independent clients need concurrent access. Cancellation
and graceful shutdown keep producer ownership until the query has stopped.

## Security and deployment

Bind listeners to loopback unless an authenticated TLS proxy or equivalent
network boundary protects both HTTP and gRPC. Connect session IDs are not
credentials. The DuckDB server does not accept paths, extension names, or
credentials from a Connect request. Keep Quack tokens in a secret manager and
restrict their owner permissions. See the [production runbook](PRODUCTION.md)
for listener, identity, process, and shutdown guidance.

## Validation evidence

The release candidate is checked from its built Linux x86_64 and macOS arm64
server wheels. Validation includes the pinned Spark 4.0.5 result/schema matrix,
explicit unsupported requests, local and Quack owner lifecycle, transactional
replay, storage-failure classification, HTTP/Connect performance comparisons,
and the operating envelope above. See [release evidence](RELEASE_EVIDENCE.md)
and the [Phase 4.20 plan](plans/sparkless-server/15-duckdb-connect-backend.md)
for the exact revisions, reports, and known limits.

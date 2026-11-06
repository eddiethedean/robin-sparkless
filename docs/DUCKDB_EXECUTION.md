# DuckDB Connect execution (4.20 candidate)

This page documents the source implementation for the unreleased 4.20.0 candidate.
It is not part of the published 4.19.0 support contract. On macOS arm64, the
pinned Spark 4.0.5 differential passes 44 result/schema cases and rejects 20
unsupported forms. Release-profile local-owner and Quack runs complete paced
one-million-row reads at active_operations=16. The original V38 runs exposed a
5-second send-stall timeout and long small-query waits behind a slow result
consumer. The candidate gives network backpressure 30 seconds to replenish
HTTP/2 send credit and uses a bounded pool of independent read connections.
Quack small-count p99 fell from 27.75 seconds to 167 ms in the measured cohort.
V38 capacity budgets, cross-platform and arbitrary partial-I/O recovery, and
exact 4.20.0 artifact checks remain open. These local candidate results are not
production certification.

Release-profile local builds pass startup selection, public DuckDB execution,
write reattachment/replay, Quack owner-loss recovery, and the pinned V37
differential. Both server products were also built into local macOS wheels and
passed isolated install checks. Their embedded executables are local builds;
the wheels still carry the repository's 4.19.0 version metadata, so they are
packaging smoke evidence rather than 4.20.0 artifacts. The [local release
summary](plans/sparkless-server/evidence/phase-420-release-profile-local-summary.json)
records the binary, report, and wheel hashes.

Additional local evidence now covers isolated performance attribution for
420-07–420-11 and graceful shutdown cancellation of an active Quack-backed
Connect query. One p50 pair exceeded its per-pair guardrail for #1739 and for
the #1741 Arrow-sharing workload; neither result repeated within its paired
run set. See the [Phase 4.20 evidence notes](plans/sparkless-server/15-duckdb-connect-backend.md#isolated-performance-attribution)
for the measurements and limits.

The optional `sparkless-server-duckdb` product can run supported Spark Connect
plans in DuckDB and store typed tables in one configured DuckDB owner. The
default backend remains Polars. Select the backend when the server starts; a
server process and its sessions use one backend for their lifetime.

## Start with a local database

Build or install the DuckDB server with Spark Connect enabled. Point the
execution owner at a persistent database file:

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

These pool sizes are fixed candidate limits, not user-configurable values or an
accepted production capacity guarantee. Slow network sends do not retain the
only connection available for unrelated reads, but the server still needs a
measured memory and latency budget for the configured workload.

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

The adapter enforces a 1,000,000-row `range` cap, bounded LocalRelation Arrow
input whose declared schema must match its Arrow schema (rows, encoded bytes,
and decoded bytes are also bounded), and batches of at most 16,384
rows or 1 MiB of Arrow IPC. Existing server admission, response, session, and
replay limits continue to apply. The primary connection and each pooled read
connection use one DuckDB execution thread and a 256 MiB memory limit per
connection; these settings do not bound aggregate or other process-level
allocation.

Tables and operation receipts persist in the configured database. Connect
sessions, temporary state, and retained Arrow results are server-instance
scoped. The receipt table survives a server restart, but that alone does not
restore the prior Connect session or make an old operation reattachable.
Local-owner and file-backed Quack smokes verified that resubmitting the
unchanged write request under the same user, session, and operation identity
after server restart replays its durable receipt. A changed request under that
identity is rejected without effects. These local cases do not establish
behavior across all Quack deployments or release artifacts. If commit
acknowledgement is lost, the live server reports an unknown outcome until the
receipt can be consulted. A local POSIX `RLIMIT_FSIZE` fault makes DuckDB return
`File too large` while writing a 32 MiB transaction WAL at COMMIT; Sparkless
reports the outcome as unknown. The [local fault report](plans/sparkless-server/evidence/phase-420-duckdb-storage-commit-fault-local.json)
records the command and recovery result: reopening found zero rows and zero
receipts, with no partial payload. That result applies to this injected
`RLIMIT_FSIZE` fault only. Cross-platform behavior, other partial-I/O recovery,
and receipt reconciliation remain V38 release checks.

The V38 load matrix used a 5-second queue timeout, one-million-row counts and
slow readers, 100,000-row collects, and 60-second 1/4/8/16-client cohorts.
Reattachable runs covered active limits 2/4/8 against both a local owner and a
separate file-backed Quack owner. Each owner mode recorded 25, 19, and 10
admission rejections at limits 2, 4, and 8, respectively. Rejections begin at
four clients at limits 2/4 and at eight clients at limit 8. Only the reattachable
active-8 cohorts with 1/4 clients had no rejections; that is a one-host candidate
observation, not a release support guarantee. The non-reattachable
mode was measured against both owners at active limits 2/4/8; the original 5-second
send watchdog stalled paced readers in every tested limit. A direct release-profile
repro reached 765,952 rows before the server deadline, while a no-pause control read
all 1,000,000 rows. A gRPC trace showed that the client replenished HTTP/2 send credit
in a delayed batch, after the old watchdog had expired. The local fix separates the
30-second network send-stall allowance from the 5-second reattachable response-ACK
deadline. With the fix, the local and Quack release-profile 8-client/active-16 runs
completed both concurrent and final non-reattachable readers with one million rows,
zero capacity rejections, clean health probes, and graceful shutdown. An active-8
run rejected its ninth concurrent operation at the configured compute queue timeout,
while the final standalone reader passed. See the [post-fix V38 report](plans/sparkless-server/evidence/phase-420-duckdb-v38-backpressure-release-local-summary.json)
and the [pre-fix matrix](plans/sparkless-server/evidence/phase-420-duckdb-v38-load-local-summary.json).
These measurements close the reproduced send-stall defect on this host, not the V38
capacity or supported-platform gates. HTTP SQL probes are
omitted because DuckDB mode intentionally rejects SQL over HTTP.

The bounded read pool was added after an active-16 Quack run showed short queries
waiting up to 27.75 seconds while a non-reattachable reader was blocked sending
its result. With the pool, the final release-profile cohorts recorded 80.3 ops/s
and 153 ms short-job p99 for the local owner, and 79.2 ops/s and 160 ms
short-job p99 for Quack. Quack small-count p99 was 167 ms, 99.4% below the
pre-pool run's 27.75 seconds. Both cohorts completed the concurrent and final
one-million-row readers with no capacity rejections and graceful shutdown. The
Quack server and fixture owner peaked at 116 MB and 66.8 MB RSS. These are local
measurements without an accepted latency or memory budget; raw reports and the
calculation are in the [read-pool summary](plans/sparkless-server/evidence/phase-420-duckdb-v38-read-pool-local-summary.json).

The shared Quack smoke now kills the owner after the first batch of a 1M-row
reattachable result stream. The result completes, HTTP health remains ready, and
an independent client sees the committed data after owner restart. The first
Connect query in the old session after restart reports `Invalid connection id`;
one retry in the same session succeeds. A separate release-profile Quack probe
kept a paced million-row stream active while four concurrent Connect reads were
submitted; all four failed against the lost owner, health stayed ready, and the
reads recovered after owner restart. See the [mixed-load lifecycle report](plans/sparkless-server/evidence/phase-420-quack-execution-final-release-local.json).
This is one-host evidence. Other partial-I/O recovery cases, release/cleanup
acceptance, and cross-platform/package checks remain open.

## Status and release gates

Read the [phase 4 plan](plans/sparkless-server/15-duckdb-connect-backend.md)
and [4.20 roadmap](plans/sparkless-server/ROADMAP.md). The local V37 report
records 44 result/schema comparisons and 20 explicit rejections against Spark
4.0.5. The original shared-owner and local-owner load matrices expose timeouts
at lower active limits; the final active-16 read-pool cohorts pass on this host.
The supported capacity envelope must still be resolved and accepted.
V38 also needs cross-platform storage-failure evidence, recovery checks for other
partial-I/O failures, repeat mixed-load owner-loss acceptance on supported
platforms, complete release/cleanup, and resource-budget acceptance. Exact
release artifacts and supported-platform package checks remain pending. Until the final reports are linked from
[release evidence](RELEASE_EVIDENCE.md), the 4.19.0 install instructions and
support matrix remain authoritative.

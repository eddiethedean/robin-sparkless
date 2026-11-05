# DuckDB Connect execution (4.20 candidate)

This page documents the source implementation for the unreleased 4.20.0 candidate.
It is not part of the published 4.19.0 support contract. The Spark Connect
semantic and interoperability gates V37 and V38 are still open; do not treat
this candidate surface as production-certified until the release evidence says
otherwise.

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

## Supported request subset

The capability response reports the active backend and effective execution
limits. In DuckDB mode, the Connect adapter currently accepts:

- `range`, bounded scalar `LocalRelation`, and named tables in the configured
  execution database;
- aliases, projection, supported scalar filters, `limit`, inner equality joins
  including `USING`, and grouped `count`/`sum`;
- one parsed, read-only `SELECT` relation using literals, identifiers, unary and
  binary operators, `COUNT`, `SUM`, and `ABS`; simple `*` and `alias.*`
  projections; supported `WHERE` expressions including `IS NULL` and
  `IS NOT NULL`; expression `GROUP BY`; inner
  equality/`USING` joins and `CROSS JOIN`; and a nonnegative integer `LIMIT`;
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
or column-renamed derived tables, and query settings. Unsupported clauses fail
before DuckDB execution instead of being silently dropped during planning.

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
replay limits continue to apply. DuckDB is configured with one execution thread
and a 256 MiB memory limit per connection; these settings do not bound every
process-level allocation.

Tables and operation receipts persist in the configured database. Connect
sessions, temporary state, and retained Arrow results are server-instance
scoped. The receipt table survives a server restart, but that alone does not
restore the prior Connect session or make an old operation reattachable. If
commit acknowledgement is lost, the live server reports an unknown outcome;
reconciliation through the same operation identity is subject to the V38
session/restart checks. Do not assume that a client can replay an old operation
after restarting the server.

## Status and release gates

Read the [phase 4 plan](plans/sparkless-server/15-duckdb-connect-backend.md)
and [4.20 roadmap](plans/sparkless-server/ROADMAP.md). V37 must establish Spark
4.0.5 result/schema agreement on the declared request matrix. V38 must prove
transaction outcomes, cancellation, restart behavior, and bidirectional access
through the same local or Quack owner. Until those reports and package checks
are linked from [release evidence](RELEASE_EVIDENCE.md), the 4.19.0 install
instructions and support matrix remain authoritative.

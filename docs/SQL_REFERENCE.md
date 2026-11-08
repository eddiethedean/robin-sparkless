# SQL dialects by execution surface

SQL parsing does not imply execution support. The parser crate recognizes more
syntax than each engine/transport permits. The current
[support matrix](SUPPORT_MATRIX.md) governs 4.20.0; unlisted combinations need
workload validation.

## Choose the surface

| Surface | Queries | Commands and state | Files / Python |
| --- | --- | --- | --- |
| Local Python / Rust with `sql` | SELECT over local views, tables, warehouses and configured providers; filters, projections, joins, aggregates, ordering and windows depend on supported expressions | Bounded database/table DDL and INSERT; provider writes depend on capability | Native feature/context dependent; Python scalar SQL UDF registration is not a public contract |
| HTTP, Polars backend | Advertised bounded SELECT/COUNT over managed service tables; inspect `/capabilities` for the SQL allowlist | CREATE DATABASE/TABLE, INSERT, DROP TABLE/DATABASE, temp-view creation/drop, USE; mutations need instance and operation IDs | No general filesystem, database or Python execution |
| Connect, Polars backend | Supported DataFrame relations, filters, projections, inner joins and count/sum; not general SQL-string execution | No SQL command/write contract | No file readers, DataFrame writers or Python execution |
| Connect, DuckDB backend | One read-only SELECT with the clauses/functions below | Explicitly typed CREATE TABLE and literal INSERT VALUES; durable receipts in configured owner | Read-only configured aliases; no client-selected file access or Python execution |

Local SQL does not provide a complete Spark/Hive dialect, transaction language,
metastore or distributed query planner. Use explicit schemas, aliases and sort
tie-breakers, and compare exact values and schemas with your Spark version.
Unsupported commands can fail when `sql` is called; invalid query expressions
can fail at an action. Do not use a count-only check as proof of correct values.

## DuckDB Connect grammar boundary

| Construct | Accepted subset |
| --- | --- |
| SELECT | Literals, identifiers, simple `*` / `alias.*`, supported unary and binary operators |
| Functions | `COUNT`, `SUM` on a resolvable supported numeric column, `ABS` |
| FROM / JOIN | Configured tables and supported derived relations; inner equality and USING joins, SQL CROSS JOIN |
| WHERE | Supported scalar predicates, including IS NULL / IS NOT NULL |
| GROUP BY | Supported expression keys with count/sum |
| LIMIT | Nonnegative integer |
| CREATE TABLE | Explicit supported scalar column types; no CTAS |
| INSERT INTO ... VALUES | Scalar literal rows; declared timestamp/decimal string casts |

CTEs, DISTINCT, ORDER BY, OFFSET, FETCH, HAVING, windows, table functions,
table sampling/hints, lateral relations, parameterized SQL and general scalar
CAST are rejected. INSERT casts are limited to the documented timestamp and
decimal forms. Nested and unsigned Connect result types are unsupported; a
top-level untyped NULL result is unsupported. Binary/NTZ DDL support does not
imply that every such type can be returned through Connect.

Example query after creating `events` in the
[DuckDB execution tutorial](DUCKDB_EXECUTION.md#start-with-a-local-database):

```sql
SELECT COUNT(*) AS n FROM events WHERE id > 0;
```

The pinned client keeps supported SELECT lazy. Supported commands take effect
during `spark.sql(...)`; their returned DataFrame is an empty command result.
Calling `.collect()` on it does not repeat the command. Calling `spark.sql(...)`
again is a new submission, not a documented durable replay operation.

## Local SQL identifiers and expressions

Use backticks for quoted identifiers. In local SELECT expressions, single and
double quotes delimit string literals; use aliases for stable output field names.
Case sensitivity follows `spark.sql.caseSensitive` (false by default locally).
The local 3.5/4.0 compatibility bundle differs from Connect's fixed UTC Spark
4.0.5 session profile. Timestamp, decimal, invalid-cast and overflow behavior
must be validated at the selected surface; selecting DuckDB does not enable all
DuckDB syntax.

See [functions and contracts](PYTHON_API_REFERENCE.md),
[types and errors](TYPES_AND_ERRORS.md), [HTTP envelopes](HTTP_API.md), and
[write recovery](WRITE_RECOVERY.md). The full remote clause/type exclusions
remain in [DuckDB execution](DUCKDB_EXECUTION.md#supported-request-subset).

# Product architecture

Sparkless provides local PySpark-style Python and Rust APIs plus a bounded
native HTTP/Connect service. These surfaces share engine components but expose
different capabilities; [the support matrix](SUPPORT_MATRIX.md) is the contract.

```text
Python application                       Rust application
  sparkless.sql / testing                  robin-sparkless facade
          | sparkless-native PyO3 extension          |
          +-------------------+----------------------+
                              |
                    core traits / ExprIr / schemas
                              |
                      Polars backend (lazy plans)
                              |
                  local files / enabled database drivers

HTTP SQL clients ---- native server ---- Connect gRPC clients
                         |
             shared admission, sessions and replay buffers
                         |
             +-----------+---------------------+
             |                                 |
     Polars / in-memory catalog      optional DuckDB Connect adapter
                                    configured owner / durable tables
                                    and transactional write receipts
```

Python wrappers implement compatibility behavior around the native extension;
some operations (notably Python scalar UDFs) collect data at the Python boundary.
Ordinary transformations extend lazy plans; actions execute them. The core crate
contains engine-neutral schemas/errors/expressions/provider contracts, the Polars
crate implements them, and the root facade exposes the Rust API. See
[engine layering](ENGINE_ARCHITECTURE.md) for module and dependency details.

## Follow a Connect query

This trace describes the default Polars backend. DuckDB execution uses its
separate typed-plan adapter and configured owner.

1. In `spark.range(10).filter("id > 5").count()`, PySpark serializes the range,
   filter, and count as a Spark Connect plan. The count triggers execution.
2. The Connect transport receives the gRPC request and the server coordinates
   admission, the session, and operation tracking.
3. The Polars adapter validates and translates the supported relation tree. The
   engine executes it and returns a logical schema and Arrow IPC batches.
4. The transport streams those batches to PySpark, which decodes the count.

A new remote expression starts in the Polars Connect adapter's
[`connect/plan.rs`](https://github.com/eddiethedean/robin-sparkless/blob/main/crates/sparkless-server-polars/src/connect/plan.rs).
Define how it binds column references and reports invalid or unsupported input.
Cover schema analysis and execution with the same expression, then compare its
output with Spark 4.0.5 through the public client. See [extension paths](EXTENDING.md)
for local API and catalog changes.

## State and lifetime

Local sessions own temporary views and configuration. Process-global views and
explicit disk warehouses have distinct lifetimes ([persistence](PERSISTENCE_GUIDE.md)).
External catalog providers own their storage and write receipts, with defined
cancellation/commit boundaries ([provider contract](EXTERNAL_CATALOGS.md)).
Polars-mode servers own their managed catalog, sessions, operations, admission and
replay state in memory. The optional DuckDB Connect execution adapter persists tables
and transactional write receipts in a configured local or shared owner. Sessions,
admission and retained query results remain in memory in both modes. HTTP SQL is
disabled in DuckDB execution mode. HTTP and Connect share one instance, with reserved
namespaces separating identities. Neither a server restart nor a client timeout implies durable rollback.
No distributed scheduler, executor fleet, or cross-node recovery subsystem exists.

## Trust boundaries

Local callbacks execute with application privileges. Server transports accept
only their documented operations/types, but caller identities are not authentication.
Deploy network listeners within an authenticated TLS/network boundary and enforce
process limits outside the engine. Output/admission budgets differ from memory
isolation. [Operations](PRODUCTION.md) explains the supported deployment scope.

Design philosophy: reuse familiar PySpark transformations for local feedback,
keep embedding contracts independent of Polars where practical, reject unsupported
remote operations explicitly, and validate semantics against a real Spark oracle.
An importable symbol or historical plan is not a full compatibility guarantee.

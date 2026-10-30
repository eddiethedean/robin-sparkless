# Product architecture

Sparkless provides local PySpark-style Python and Rust APIs plus a bounded
native HTTP/Connect service. These surfaces share engine components but expose
different capabilities; [the support matrix](SUPPORT_MATRIX.md) is the contract.

```text
Python application                       Rust application
  sparkless.sql / testing                  robin-sparkless facade
          | PyO3 native extension                    |
          +-------------------+----------------------+
                              |
                    core traits / ExprIr / schemas
                              |
                      Polars backend (lazy plans)
                              |
                  local files / enabled database drivers

HTTP SQL clients ---- native server ---- Connect gRPC clients
                         |
             shared engine, admission, service catalog,
             sessions, operation identities, replay buffers
```

Python wrappers implement compatibility behavior around the native extension;
some operations (notably Python scalar UDFs) collect data at the Python boundary.
Ordinary transformations extend lazy plans; actions execute them. The core crate
contains engine-neutral schemas/errors/expressions/provider contracts, the Polars
crate implements them, and the root facade exposes the Rust API. See
[engine layering](ENGINE_ARCHITECTURE.md) for module and dependency details.

## State and lifetime

Local sessions own temporary views and configuration. Process-global views and
explicit disk warehouses have distinct lifetimes ([persistence](PERSISTENCE_GUIDE.md)).
External catalog providers own their storage and write receipts, with defined
cancellation/commit boundaries ([provider contract](EXTERNAL_CATALOGS.md)).
The server owns its catalog, sessions, operations, admission and replay state in
memory. HTTP and Connect share one instance, with reserved namespaces separating
identities. Neither a server restart nor a client timeout implies durable rollback.
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

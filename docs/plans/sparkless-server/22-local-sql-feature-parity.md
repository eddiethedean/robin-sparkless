---
search:
  exclude: true
---

# SS-22: Complete existing `sparkless.sql` feature coverage on the server

> **Status: planned; scope requested by the project owner on 2026-11-01.** Cross-cutting work in Phases 5–8; required before Phase 11 certification. This changes planned scope, not released support.
> [Index](README.md) · [Roadmap](ROADMAP.md) · [Decisions](DECISIONS.md) · [Validation](VALIDATION.md)

## Completion target

Make every existing, implemented local `sparkless.sql` feature usable with the server. The baseline is **Sparkless's own API and behavior**, including its aliases, overloads and documented limitations. Universal Apache Spark compatibility is a separate question. Reuse the native engine and shared semantics rather than maintaining a second implementation of each operation in the transport adapters.

The [local API baseline](LOCAL_SQL_FEATURE_BASELINE.md) records the named surface from the checked-in runtime inventory, Python source, native bindings and typing declarations. It includes functions omitted from `functions.__all__`, returned helper objects, Python operators, schema/Row utilities, and the bounded local RDD/SparkContext compatibility surface. SQL syntax and combinations are additionally inventoried from the parser, native translator and existing SQL regression corpus; a Python-symbol list alone cannot establish SQL coverage.

All implemented baseline features are required delivery scope. D21 resolves their implementation and transport mappings; it cannot silently remove a feature from this requirement. A discovered gap must receive a named slice, owner and fixture before its phase closes. Source-only declarations, stubs, no-ops, unsupported argument combinations and known deviations must be classified explicitly against actual local behavior. Preserve truthful compatibility behavior or errors; an importable name is not a requirement to implement functionality absent locally.

Later local additions must update the inventory and receive a server workstream before the local release is finalized. For initial certification, freeze the exact source/wheel baseline and reconcile additions through the certification candidate. A scope change or deferral requires an explicit roadmap revision; it cannot be disguised as an unsupported server feature while claiming complete baseline coverage.

## Transport and compatibility contract

- **Spark Connect:** implement every baseline relational, expression, action, schema and catalog feature that the selected public client can represent, including compositions. Server work includes decoding, analysis, execution, exact Arrow results, errors, session isolation, cancellation and replay. An engine function alone is insufficient.
- **HTTP SQL:** expand the allowlist to every statement, expression, function and result type actually supported by local `SparkSession.sql`, including its query and mutation combinations. Use the shared parser/engine and explicit capability registration. Retain admission, path/credential policy and mutation identity/outcome requirements; expand the protocol where necessary rather than permanently excluding a local SQL feature.
- **Client utilities:** keep `Row`, type construction/serialization, Window builders, aliases, operator overloads, collection/conversion/display helpers and active-session helpers in the client when appropriate. Verify their observable behavior with remote data. HTTP need not invent an SQL spelling for a Python-only utility.
- **Local-only interfaces:** standard Connect does not expose `df.rdd`, Python callback providers or every Sparkless alias/helper. Deliver a separately documented Sparkless client compatibility bridge and, where needed, SS-12 worker RPCs. Retain the unmodified upstream client for ordinary Connect APIs; never claim the extension is a standard Connect feature. D21 must name the concrete bridge API for every such entry and test the complete local operation, not just a loosely related replacement.
- **Optional capabilities:** Python workers, Delta/catalog providers, JDBC/SQLite and other compiled drivers remain opt-in. They are required implementations for completion of their baseline rows, not prerequisites for unrelated native operations. Default Polars must cover the native baseline; optional DuckDB support cannot substitute for it or trigger silent fallback.

## Feature families and delivery ownership

| Existing local family | Required server coverage | Delivery / gates |
| --- | --- | --- |
| Session, builder, config, catalog and lifecycle | Creation/new sessions, active-session helpers, range/table/createDataFrame inputs and options, local/global views, database/table metadata, effective settings, cancellation handles, stop and compatibility properties | Phase 7 / SS-16, SS-07; V39/V40, V56/V57 |
| Column and DataFrame APIs | Every implemented named method, camel/snake alias, property and Python operator; selection and expressions, add/rename/drop, NA drop/fill/replace, all joins, sorting/null ordering, distinct/deduplication, set operations, sampling, repartition/coalesce/hints with their actual local semantics | Phase 7 / SS-16; V39/V40, V56/V57 |
| Functions | All existing public built-ins and overloads: numeric/trigonometric/statistical/approximate, string/regex/hash/encoding, date/time, null/conditional/cast, JSON, arrays/maps/structs, generators/explode, current-session/user and ID helpers | Phase 7 / SS-16; V39/V40, V56/V57 |
| Grouping, statistics and reshaping | Grouped/global aggregates, pivot, cube/rollup/grouping sets where locally implemented, describe/summary, crosstab/frequent items, melt/unpivot, all implemented approximate/statistical functions | Phase 7 / SS-16; V39/V40, V56/V57 |
| Windows | All local ranking/distribution/offset/value/aggregate functions; partition/order builders and every implemented row/range frame, including unbounded endpoints over bounded batch input | Phase 7 / SS-16; V39/V40, V56/V57 |
| Types, Row and results | Every locally implemented scalar/nested/interval/variant/char/varchar representation and conversion; field metadata/nullability, schema parsing/JSON/DDL, empty results, duplicate labels; collect/first/head/take/show, Arrow/batch/Pandas conversion and iteration | Phase 7 / SS-08, SS-16; V18/V39/V40, V56/V57 |
| SQL language | All locally accepted queries, CTEs/subqueries, expressions/functions/generators, joins/sets/windows, CREATE/REPLACE/CTAS, INSERT, UPDATE/DELETE, database/schema/table/view DROP/CREATE, SHOW/DESCRIBE and other accepted statements in their actual supported contexts | Phase 7 / SS-05, SS-07, SS-16; V56/V58 plus V39/V40 for Connect SQL |
| Python execution and bounded local RDD | Existing scalar UDF contexts and return types, DataFrame flatMap, local RDD map/filter/flatMap/reduce/distinct/actions and helpers; metadata-only SparkContext compatibility without distributed scheduling | Phase 5 / SS-12; V27, V56/V57; metadata-only client utilities in Phase 7 |
| Delta and external providers | Local Delta reads/version selection/writes, provider registration/resolution and materialized/batch writes, descriptor/receipt validation, provider errors, warehouse/catalog ownership | Phase 6 / SS-13; V28, V56/V57; Python callbacks use Phase 5 workers, integrated acceptance in Phase 6 |
| Readers, writers and materialization | All locally implemented CSV/JSON/Parquet and JDBC/SQLite/driver APIs, options/schema/inference/write modes/partitioning, table saving and warehouse persistence; cache/persist/storage behavior and checkpoints where implemented | Phase 8 / SS-17; V41–V43, V56/V57; Delta implementation remains Phase 6 |
| Errors and compatibility utilities | All existing exported error aliases, useful error attributes and cancellation behavior; documented limitations, stubs/no-ops and missing argument/type combinations identified individually | In each owning phase; V56/V57 and V58 for SQL errors |

These families are ownership rules for **every** symbol in the baseline, not an illustrative function shortlist. SQL access to a worker/storage/connector feature completes with that feature's Phase 5/6/8 integration. Phase 7 completes the native SQL language on both transports; Phase 8 closes all remaining baseline integration rows. Durable recovery and Structured Streaming remain additional Phases 9/10 work, not prerequisites invented for existing batch semantics.

## Ordered delivery

| Slice | Deliverable | Exit evidence |
| --- | --- | --- |
| LSQL-01 | Reconcile the baseline against the actual source and built wheel; expand to a machine-readable per-symbol/SQL-case capability registry under D21 | Source/wheel hashes, signatures/aliases/overloads, local behavior or explicit stub evidence, owning slice, exact Connect/HTTP/client/worker mapping and fixture for every entry; zero unassigned implemented features |
| LSQL-02 | Phase 5 Python/bridge integration | Existing local UDF and bounded RDD/flatMap pipelines execute remotely with worker ownership, bounded transport, errors and cleanup; no Python inside the native process |
| LSQL-03 | Phase 6 storage/catalog integration | Delta/version/provider read/write and lazy batch callbacks retain schemas, receipts, errors and documented local semantics; no callback invoked on a native runtime thread |
| LSQL-04 | Phase 7 complete native API/types/functions and both SQL adapters | Every native baseline row and SQL regression has an executable remote mapping, including previously excluded pivot/statistics/generators/window frames; client-only utilities tested against remote results |
| LSQL-05 | Phase 8 reader/writer/cache/warehouse integration | All locally implemented connector/options/modes and remaining bridge/materialization rows complete; optional-enabled and disabled deployments tested separately |
| LSQL-06 | Reconcile the candidate local baseline before Phase 11 | V56–V58 and applicable phase gates pass; no implemented baseline feature is omitted, unknown, merely stubbed remotely or deferred without an approved scope revision; earlier releases remain truthful |

Preserve existing release and slice identifiers. Incorporate these obligations into Phases 5–8 and Phase 11 prerequisites rather than adding a post-certification catch-up phase. Targets are scope milestones, not calendar promises or automatic evidence of completion.

## Acceptance and evidence

- **V56 — inventory completeness:** compare source, installed local wheel, SQL parser/translator and regression inventory against the per-feature registry. Check direct imports beyond `__all__`, aliases, properties, operators, returned builders/iterators and optional builds. Every implemented entry has an owning phase and concrete remote mapping; classify declarations/stubs with evidence. Fail on missing entries and stale baseline changes.
- **V57 — local/server feature parity:** run representative existing local regressions and compositions through local Sparkless and the server. Compare values, ordering where promised, schema/metadata/nullability, errors and conversions; use the pinned Spark oracle where compatible and separately assert documented Sparkless differences. Cover standard Connect and any required compatibility bridge, null/empty/boundary inputs, aliases/overloads, optional providers/workers/connectors, concurrent sessions, cancellation/replay and resource limits. No claim of full parity from API counts.
- **V58 — complete HTTP/Connect SQL:** exercise all locally supported SQL cases on both transports, including every statement family and its supported options/types/functions. Verify exact positional HTTP and Arrow results, mutation identity/outcomes and snapshot behavior, policy-configured I/O, negative cases and nested results. If the existing HTTP schema cannot represent a local result, version and test its encoding before enabling that row.

D21's registry records `feature`, `local_source`, `local_fixture`, `local_behavior`, `signature_or_sql_case`, `owner`, `phase`, `slice`, `transport_mapping`, `optional_dependency`, `server_fixture` and separate implementation/validation status. Standard Connect versus Sparkless extension support must be visible. Snapshot generation and diff checks are planned LSQL-01 work; the checked-in baseline is a scope inventory, not an implemented CI gate or a claim of tested local behavior for every declaration.

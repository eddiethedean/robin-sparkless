---
search:
  exclude: true
---

# SS-24 — Optional Apache Hudi table support

**Status: planned.** Phase 15 targets 4.31.0. [Release roadmap](ROADMAP.md#4310-optional-apache-hudi-table-support) · [Decision register](DECISIONS.md#decisions-required-by-milestones) · [Validation matrix](VALIDATION.md#gate-matrix)

## 4.31 scope

Use `hudi-rs` for Hudi timelines, file slices, log merges, and table commits. Sparkless owns DataFrame and SQL execution, catalog access, and Structured Streaming orchestration. Validate interoperability with a pinned Apache Spark and Hudi bundle using fixtures in both directions. Treat the snapshot reader as the first delivery slice. All capabilities and release gates below are part of 4.31. [hudi-rs feature matrix](https://github.com/apache/hudi-rs#features) · [Reader specification](https://github.com/apache/hudi-rs/blob/main/docs/reader-spec.md) · [Writer design](https://github.com/apache/hudi-rs/blob/main/docs/writer-design.md)

| Area | Planned support |
| --- | --- |
| Local Python and embedded Rust | Read and write by table root through the existing DataFrame I/O APIs, starting with `spark.read.format("hudi").load(path)`. Map table and maintenance operations to explicit APIs. |
| Polars server | Expose operator-configured Hudi tables through HTTP SQL and standard Spark Connect. Map the supported read modes, write operations, and streaming APIs to concrete entry points in D23. Require separate operator permissions for reading, writing, and maintenance. |
| Storage | Local filesystem, Amazon S3, Azure Blob Storage, and Google Cloud Storage. Validate each cloud backend and its credential and endpoint policy for reads and writes. |
| Table versions and layouts | Read Parquet-backed Copy-on-Write and Merge-on-Read tables at Hudi table versions 6, 8, and 9, including partitioned and unpartitioned tables. Cover Lance-backed reads for the combinations the selected reader supports. Exercise HFile as metadata table storage. Use Parquet for writes at versions 8 and 9 and cover the restricted append case for version 6 with its own compatibility criteria. |
| Batch reads | Latest snapshot, historical snapshots, incremental ranges, and Merge-on-Read read-optimized mode. Specify visibility of uncompacted updates and deletes by mode. |
| Batch writes | Create, append, upsert, update, delete by key or predicate, full overwrite, and dynamic partition overwrite. Verify record keys, ordering, partitions, save modes, and the committed Hudi instant. |
| Table services | Native Merge-on-Read compaction, failed-write rollback, timeline archival, and metadata table maintenance. Verify Spark-run compaction, clustering, and cleaning on tables Sparkless writes, followed by Sparkless reads and writes on those tables. |
| Structured Streaming | Native single-node Hudi source and sink through the supported `readStream` and `writeStream` surface. Use bounded incremental windows for the source and append or keyed upsert for the sink on certified version 8/9 tables, with checkpointed progress and recoverable commits. [Apache Hudi streaming reads](https://hudi.apache.org/docs/reading_tables_streaming_reads/) |

## D23 contract to freeze

- Select a published `hudi-rs` release and record its exact version, enabled features, and pinned Spark/Hudi fixture toolchain. Prove that the dependency builds on the advertised targets. Sparkless currently declares Rust 1.93 and uses Arrow 57 at its Polars adapter boundary (see `crates/robin-sparkless-polars/Cargo.toml`). Confirm the selected reader's Rust and Arrow requirements, choose a compatible bridge, and measure its conversion cost. Resolve the Merge-on-Read spill backend and its build and runtime dependencies before committing to the support matrix. [hudi-rs releases](https://github.com/apache/hudi-rs/releases)
- Freeze the API and option matrix by table type, version, file format, storage backend, read mode, write operation, and public entry point. Preserve column order, values, nullability, and supported decimal and timestamp semantics through Arrow, and specify the treatment of Hudi metadata columns. Reject unsupported combinations before scanning or mutating data. Resolve gaps in the required capabilities against the pinned release during Phase 15.
- Pin the selected completed Hudi instant or incremental range at local load or server execution, including the upper bound of an open-ended incremental read. Carry it through schema resolution and all scans of that table. Keep instant identity distinct from Delta's numeric version. Map Spark options to the pinned reader's requested-time or completion-time semantics, specify interval boundaries, and report unavailable snapshots or ranges after cleaning. Reserve base-file-only reads for explicit read-optimized requests.
- Reuse Sparkless's reader and catalog snapshot boundaries and keep the Hudi dependency behind an opt-in adapter. Analysis must obtain schema without scanning rows. Start with Sparkless evaluating filters after the Hudi merge. Enable projection or predicate pushdown only for cases with differential evidence that the result is unchanged, including predicates on updated rows and columns needed for merging. [Engine architecture](../../ENGINE_ARCHITECTURE.md) · [External catalog contracts](../../EXTERNAL_CATALOGS.md)
- Confine server access to operator-approved local roots and cloud endpoints, buckets or containers, and prefixes, including paths referenced by table metadata. Bind credentials to the configured owner, enforce provider permission and refresh rules, and redact credentials from errors, logs, and receipts. Reject client-supplied server locations and credentials. Freeze numeric budgets for scan and write batches, queues, parallel I/O, Merge-on-Read memory and spill, and maintenance tasks. Keep work charged until it stops and verify cleanup on success, failure, and cancellation.
- Reuse the catalog batch writer and operation receipt contracts. Bind each mutation and maintenance action to an authorized table owner and operation ID. Start with one coordinated writer or maintenance owner per table and define how it excludes overlapping external writers. Verify visibility of completed instants, rollback of failed writes, and reconciliation after a lost acknowledgement. Report unresolved outcomes and prevent blind retries. [Durable outcome contract](18-durable-connect-recovery.md)
- Use the Phase 10 micro-batch runtime. Bind source offsets to the table identity, timeline layout, and start/end instants. Define initial position, changes emitted for inserts, updates and deletes, and explicit failure on expired ranges or table replacement. Bind sink batch identity to a verifiable Hudi commit so restart can reconcile a completed write before advancing offsets or retrying. Specify trigger and output modes, compatible checkpoints, and delivery guarantees by supported combination. [Structured Streaming plan](19-structured-streaming.md)
- Decide how users opt in on Rust, Python, and server distributions. Check default dependency graphs and artifacts for Hudi dependency isolation. Record the exact supported Connect profiles and register the new local I/O APIs and their server mappings together under [SS-22](22-local-sql-feature-parity.md).

## Delivery slices

| Slice | Deliverable |
| --- | --- |
| 431-01 | Freeze D23 after feasibility checks for the chosen dependency, Arrow bridge, timeline modes, writer receipts, and streaming recovery. Pin reference tooling and generate fixtures. |
| 431-02 | Add the opt-in native adapter and local Python/Rust snapshot paths, then cover configured HTTP SQL and Connect reads with bounded batches and analysis without row scans. |
| 431-03 | Add time travel, incremental reads, and read-optimized mode. Complete the table version and file layout matrix under V60. |
| 431-04 | Add the three cloud backends, credential scopes, and allowed endpoint and prefix configuration. Run V62 for reads and complete its write coverage in 431-05. |
| 431-05 | Add transactional batch writes and table services to the declared local and server surfaces. Pass V63 with Spark-to-Sparkless and Sparkless-to-Spark interoperability. |
| 431-06 | Add the Hudi streaming source and sink using the batch and recovery contracts. Pass V64 for checkpoint, commit, and restart behavior. |
| 431-07 | Run all gates on the candidate, publish the supported matrix and install instructions, and complete the fresh public stage of V61 under the [release procedure](../../RELEASING.md). |

## Release gates

- [ ] **V60 — Batch read correctness.** Compare exact schemas and rows with Spark/Hudi across the declared layouts, versions, read modes, and APIs. Include uncompacted inserts, updates and deletes, out-of-order completion, historical and incremental boundaries, empty results, and concurrent commits. Exercise projection, filtering, aggregation, and an equijoin. Invalid, cleaned, and unsupported inputs produce explicit errors.
- [ ] **V61 — Lifecycle and distribution.** Exercise named-table ownership, unauthorized write rejection, concurrent reads, cancellation, and Connect reattach without reexecution. Measure peak memory, queued bytes, spill, and cleanup against D23 budgets. Verify enabled and disabled builds and exact candidate and fresh public artifacts on advertised platforms. Retain dependency and fixture revisions, artifact identities, and results.
- [ ] **V62 — Cloud storage and credentials.** Verify the declared read and write matrix against S3, Azure Blob Storage, and Google Cloud Storage. Exercise scoped permissions, credential expiry and rotation, endpoint and prefix escapes, and partial or throttled I/O. Verify error redaction, cleanup, and a committed table independently after fault injection.
- [ ] **V63 — Batch writes and table services.** Have Spark read Sparkless commits and Sparkless read and mutate Spark-written tables. Cover all declared operations, save modes, and service modes, including compaction, rollback, archival, metadata updates, and Spark-run cleaning and clustering. Inject failures before and after commit, verify commit receipts after disconnect or restart, and prove retries cannot duplicate an effect. Validate historical and incremental reads across service commits and after retention removes data.
- [ ] **V64 — Structured Streaming.** Compare source offsets, emitted rows, and sink commits with the pinned reference and declared output modes. Crash before and after the Hudi commit and checkpoint update. Verify restart reconciliation, no silent offset skips or duplicate commits, explicit errors on expired history, and bounded backpressure, state, and shutdown. Use self-contained fixtures and retain evidence for each certified source and sink combination.

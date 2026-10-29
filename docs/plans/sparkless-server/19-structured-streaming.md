# SS-19: Optional single-node Structured Streaming through Connect

> **Status: planned; owner unassigned.** Phase: 10; target 4.26.0; optional capability.
> [Index](README.md) · [Roadmap](ROADMAP.md) · [Decisions](DECISIONS.md) · [Validation](VALIDATION.md)

## Outcome and scope

Implement a declared subset of the pinned public `readStream`, `writeStream`, and streaming-query management APIs using a native single-node micro-batch runtime. Begin with rate/file sources, approved file/table sinks, append/update/complete output modes where valid, triggers, query IDs/names, progress/status, await/stop, and checkpoint-based restart. Add event-time watermarks, deduplication, and stateful windowed aggregates under an explicit enabled-operation matrix.

Freeze unsupported source/sink/stateful combinations before advertising the feature. Arrow result streaming and DataFrame batch checkpoints are independent capabilities. Continuous processing, distributed executors, arbitrary connectors, and universal Spark streaming compatibility remain outside the initial scope.

## Dependencies and ordered delivery

Use SS-16 relational/type semantics, SS-17 source/sink policy, and SS-18 durable identity/outcome/recovery. Every enabled streaming sink needs its own replay/commit proof; source offsets, state checkpoints, and sink commits must be coordinated. Python UDFs and optional Delta/JDBC sinks require their own prior enabled capability and streaming-specific tests; they are not mandatory dependencies for the native file/rate subset.

| Slice | Deliverable | Exit evidence |
| --- | --- | --- |
| 426-01 | Freeze D19 streaming API/source/sink/state matrix and budgets | Real public-client request fixtures, reference pins, trigger/offset/checkpoint/commit contracts |
| 426-02 | Implement bounded stateless micro-batches and query management | Start/progress/await/stop, source offsets, output modes, concurrent query isolation |
| 426-03 | Implement stateful event-time processing | Watermarks, late-data policy, window boundaries, deduplication, bounded state and eviction |
| 426-04 | Implement crash-safe progress and sink recovery | Coordinated offsets/state/commit ledger; restart and repeated-batch effects are truthful |
| 426-05 | Complete V46/V47 and optional/default packaged installs | Coordinated 4.26.0 artifacts and measured long-running resource behavior |

## Acceptance

- **V46:** unmodified-client queries match the pinned Spark reference for each enabled source/operator/sink/output/trigger combination. Verify empty batches, schema drift, timestamps, out-of-order/late events, watermark advancement, windows, duplicate input, progress events, and session/query isolation.
- **V47:** crash at every offset/state/sink-commit boundary and recover from checkpoints. Prove or explicitly limit delivery guarantees per sink. Test checkpoint corruption, source loss/replacement, backpressure, state/disk quotas, slow sinks, retry, expiry, client disconnect, cancel/stop, and full shutdown. Long-running runs demonstrate bounded queues/state/checkpoints and health progress under concurrent batch work.
- Define which server/query identity survives a restart and how clients rediscover it; do not inherit a guarantee solely from durable batch replay. Core native streaming requires no Python/JVM unless an explicitly selected optional UDF/connector uses its separate worker process.

RDD/SparkContext and distributed Spark execution remain outside the roadmap; this phase adds single-node DataFrame streaming.

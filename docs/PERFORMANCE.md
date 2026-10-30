# Performance and sizing

Sparkless reduces JVM/cluster startup overhead for local tests. No universal
speedup or dataset size is promised. Runtime depends on input size/types, IO,
query shape, concurrency, Python UDF boundaries, and hardware. Compare against
your actual alternative using the same work and verify output correctness first.

## Reproducible measurements

Record commit/package version, wheel/native features, OS/libc, CPU model/core
quota, memory limit, thread settings, input seed/row counts/schema, query text,
warmup, repetitions and exact command. Separate cold process/startup, warm query,
and end-to-end IO time. Report throughput, p50/p95 latency, peak RSS, failures
and output equality; retain raw samples. Do not compare an M4 laptop baseline
with an Ubuntu runner as a regression budget.

For the native service, use the checked-in
[Connect test/benchmark entry points](https://github.com/eddiethedean/robin-sparkless/blob/main/tests/connect/README.md)
and [V26 budget checker](https://github.com/eddiethedean/robin-sparkless/blob/main/scripts/verify_connect_v26_budget.py).
Read their current `--help`/README and use same-hardware baseline/candidate
reports. [Release evidence](RELEASE_EVIDENCE.md) distinguishes current diagnostics
from publication requirements. Historical PR benchmarks are not general product claims.

## Size a workload

Start at one client and representative data; measure peak RSS through joins,
sorts, groupBy and output conversion, then increase concurrency gradually. A
small output can hide large intermediates. Account for process/runtime overhead,
input buffers, intermediate plans, output copies and Python/Pandas/Arrow copies.
Keep headroom for skew and outliers. Set OS/container memory/CPU limits and
admission below saturation; bounds on output batches do not bound process RSS.

Project/filter early, avoid unnecessary collect/toPandas/toArrow, prefer built-ins
to Python UDFs, and use `toArrowBatches` with explicit limits for incremental
output. Streaming Arrow results is not Structured Streaming. Row-at-a-time Python
UDFs materialize input and can erase performance gains. Test cancellation and
failed queries under load, because cooperative native work can retain capacity.

Choose the maximum dataset and concurrency from measured stable behavior on the
deployment host. If workloads require distributed scheduling, durable execution
recovery, or hard per-query isolation, use infrastructure that supplies those guarantees.

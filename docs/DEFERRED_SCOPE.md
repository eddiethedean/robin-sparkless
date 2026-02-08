# Phase H: Deferred / Optional Scope

This document lists APIs and features that are **explicitly out of scope** for robin-sparkless full parity. They are documented for clarity so users know the boundaries and can choose appropriate workarounds.

See also: [PYSPARK_DIFFERENCES.md](PYSPARK_DIFFERENCES.md), [ROBIN_SPARKLESS_MISSING.md](ROBIN_SPARKLESS_MISSING.md), [FULL_PARITY_ROADMAP.md](FULL_PARITY_ROADMAP.md).

---

## XML / XPath

| APIs | Status | Rationale |
|------|--------|-----------|
| `from_xml`, `to_xml`, `schema_of_xml`, `xpath`, `xpath_boolean`, `xpath_double`, etc. | Not implemented | Would require an XML parser and feature flag. |

**Workaround:** Preprocess XML externally or use JSON instead (`from_json`, `to_json`, `get_json_object` are supported).

**Tracking:** GitHub issue #146

---

## UDF / UDTF

| APIs | Status | Rationale |
|------|--------|-----------|
| `udf`, `pandas_udf`, `udtf`, `call_udf`, `spark.udf.register` | Not implemented | Robin-sparkless has no Python UDF support; execution is pure Rust/Polars. |

**Workaround:** Use built-in expressions and the plan interpreter. For custom logic, preprocess data in Python/Rust before passing to robin-sparkless, or use Polars expressions via the plan format.

**Tracking:** GitHub issue #143

---

## Streaming

| APIs | Status | Rationale |
|------|--------|-----------|
| `withWatermark`, `session_window`, `isStreaming` | No-op / stub | Robin-sparkless uses eager execution only; no streaming execution model. |

**Current behavior:** `isStreaming` always returns `False`; `withWatermark` is a no-op that returns the DataFrame unchanged.

**Workaround:** Use batch processing. For streaming-like workflows, process data in batches and write incrementally.

**Tracking:** GitHub issue #145

---

## Sketch aggregates

| APIs | Status | Rationale |
|------|--------|-----------|
| HyperLogLog (HLL), count-min sketch, `hll_sketch_agg`, `hll_sketch_estimate`, etc. | Not implemented | Optional approximate aggregates; low priority for initial parity. |

**Workaround:** Use exact aggregates (`count`, `count_distinct`) or `approx_count_distinct` where supported.

**Tracking:** GitHub issue #147

---

## RDD / distributed

| APIs | Status | Rationale |
|------|--------|-----------|
| `rdd`, `foreach`, `foreachPartition`, `mapInPandas`, `mapPartitions` | Stub (raise `NotImplementedError`) | Robin-sparkless is eager and single-process; no RDD or distributed execution. |

**Workaround:** Use `collect()`, `toLocalIterator()`, or `to_pandas()` for local access. For row-wise processing, materialize with `collect()` and iterate in Python/Rust.

**Tracking:** GitHub issue #142

---

## Catalog DDL

| APIs | Status | Rationale |
|------|--------|-----------|
| `CREATE TABLE`, `CREATE DATABASE`, `DROP TABLE`, `INSERT INTO`, `writeTo` | Not implemented or stub | No persistent catalog; use file-based storage. |

**Workaround:** Use `df.write().format("parquet"|"csv"|"json").mode("overwrite"|"append").save(path)` to write to paths. Temp views (`createOrReplaceTempView`, `spark.read.table`) are supported for in-session tables.

**Tracking:** GitHub issue #144

---

## Related

- **JVM / runtime stubs:** `broadcast`, `spark_partition_id`, `input_file_name`, `monotonically_increasing_id`, `current_catalog`, `current_user` — implemented as stubs for API compatibility; see [PYSPARK_DIFFERENCES.md](PYSPARK_DIFFERENCES.md#jvm--runtime-stubs).
- **sentences / NLP:** Deferred; could be implemented as string split + list of lists (#148).

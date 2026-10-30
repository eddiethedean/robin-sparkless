# Differences from PySpark — 4.18.0

Sparkless implements a local PySpark-style API on Polars. It is not a full Spark
runtime. This page describes the installed local API; [the support matrix](SUPPORT_MATRIX.md)
separates Rust, HTTP and Connect. Validate your actual workload against your
production Spark version; an import or passing fixture count is not parity proof.

## Compatibility profiles (3.5 vs 4.0)

The default local profile is 3.5; 4.0 is opt-in. Profiles change documented
semantics, not the installed PySpark version. See [profile settings](PYSPARK_COMPAT_PROFILES.md).
Live oracle examples use pinned 3.5.8 and 4.0.5 in docs CI. Historical expected-output
fixtures have mixed provenance, including the loader's 3.2 default.

## Types, inference and ordering

Use explicit StructType schemas for null-only/empty/mixed inputs. Check integer
widths, decimal precision/scale, timestamp/timezone and nested conversions at
storage/Pandas/Arrow boundaries. Tuple schema shortcuts are Sparkless-specific;
use public DataType objects for portable examples. `Row` supports named and
positional access; do not rely on iteration yielding values.

Group/aggregate output and unordered inputs have no stable row order. Sort with
tie-breakers when order matters. Compare row values **and** schema/nullability;
a count-only assertion can miss incorrect values. Random seeds and expression
implementations need not generate the same sequences as Spark.

## Join

Named-key inner/outer/semi/anti joins and supported expression joins execute
locally. Use aliases to disambiguate fields, and validate null keys, duplicate
names and condition semantics against Spark. Non-equality joins can materialize
a cross product and hit `SPARKLESS_MAX_CROSS_JOIN_ROWS`. No shuffle, partition
placement, or distributed join strategy is implied by the API spelling.

## SQL and catalogs

Official Python builds enable SQL; Rust consumers opt into `sql`. Supported
queries and bounded DDL/DML do not imply a Hive metastore, all Spark SQL syntax,
DataFrameWriterV2, or a distributed catalog. Temp views are session scoped;
process-global temp views and configured Parquet warehouses have separate
lifetimes ([persistence](PERSISTENCE_GUIDE.md)). External providers have their own
write/receipt/error contract ([catalog providers](EXTERNAL_CATALOGS.md)).

`spark.catalog` is a property. `listTables` returns names rather than Spark Table
objects. `df.write` is a property; `saveAsTable(name, mode=None)` is the installed
writer signature. Do not copy older binding-only `catalog()` / `write()` forms.
HTTP supports only its allowlist; Connect SQL commands are unsupported.

## Readers and native features

The installed Python DataFrameReader does not expose `schema(...)`; normalize
column types or build from an explicit createDataFrame schema. Rust/internal
reader capabilities do not establish the Python wrapper surface. Options are a
subset of Spark's and can be unsupported or retained without a Spark effect.
Delta is enabled in default Python builds. JDBC/SQLite need selected compiled
features; the Python `jdbc` extra cannot add them to a wheel. See [installation](INSTALLATION.md).

## Python UDFs

Typed scalar `F.udf` executes locally in withColumn/projection and checked scalar
comparison filters. It collects input and runs Python row by row. Use explicit
DataType return objects and handle nulls. Public pandas/vectorized/grouped UDFs,
UDTFs, and remote Python UDF execution are unsupported. SQL UDF registration and
arbitrary expression compositions are not promised by the scalar example.
See [UDF contexts](UDF_GUIDE.md).

## JVM / runtime stubs { #jvm--runtime-stubs }

There are no executors, Spark UI, JVM gateway, distributed scheduler, or Spark
partitioning guarantees. Local `df.rdd` supports a bounded in-process subset,
including count/map/flatMap/collect; this is not distributed RDD execution.
Methods such as `foreach`, `foreachPartition`, `mapInPandas`, `mapPartitions`,
`isStreaming` and `withWatermark` are absent from the checked installed DataFrame
surface. Unsupported APIs can be absent, not merely methods raising NotImplementedError.

Compatibility functions can have limited behavior: `input_file_name()` returns
empty strings; `spark_partition_id()` and `monotonically_increasing_id()` return
constant 0 in the checked example. Do not depend on that ID function for uniqueness.
Current user/catalog helpers are local values, not cluster authorization or tenant
identities. Cache/persist/repartition/broadcast spellings do not establish Spark
storage, shuffle or distributed execution behavior.

## Security hardening (optional)

Set hardening before session creation. `SPARKLESS_HARDENED=1` enables stricter
defaults; `SPARKLESS_FILES_BASE` confines general file IO; setting
`SPARKLESS_JDBC_ALLOW_ARBITRARY_SQL=false` requires dbtable rather than arbitrary
query/init/prepare SQL. These controls do not sandbox native or Python code.
See [all configuration defaults](CONFIGURATION.md), [security](SECURITY.md), and
[the server operations boundary](PRODUCTION.md).

For unsupported/unknown combinations, use [deferred scope](DEFERRED_SCOPE.md)
and add a [minimal oracle regression](TEST_CREATION_GUIDE.md) before adoption.

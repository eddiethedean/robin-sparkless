# Unsupported and unverified scope — 4.18.0

The [support matrix](SUPPORT_MATRIX.md) is the release contract. Historical phase
plans are not capability promises. “Unknown” means no reviewed behavior contract
for that combination; it does not assert that an internal implementation cannot exist.

| Area | Current boundary | Alternative |
| --- | --- | --- |
| Distributed execution | No executor fleet, Spark scheduler, distributed RDD or fault tolerance | Test deployment behavior on real Spark |
| Structured Streaming | No streaming engine; checked installed DataFrame lacks isStreaming/withWatermark | Batch pipeline or a streaming engine |
| Python UDF variants | Local typed scalar UDF only in documented contexts; no public pandas/vectorized/grouped UDF or UDTF | Built-in expressions or real PySpark |
| Remote Python execution | No HTTP/Connect Python worker or UDF contract | Local trusted-process code |
| Catalog federation / WriterV2 | No full Hive/Spark metastore or writeTo contract | Documented local warehouse/external provider |
| Connect general Spark surface | No SQL commands, joins, sorts/windows, general grouped agg, file sources, writes, artifacts or streaming | Published range/local/filter/project/limit/count subset or real Spark |
| HTTP general SQL/IO | Bounded allowlist and in-memory catalog; no arbitrary filesystem/JDBC/Python execution | Local library or appropriate service |
| Durable server recovery | No cross-restart catalog/operation journal or durable exactly-once guarantee | Application reconciliation and durable infrastructure |
| Hard query isolation | No in-process hard CPU/intermediate-memory enforcement | Separate OS-limited processes |
| XML/XPath, advanced sketches/NLP | No reviewed complete public compatibility contract | Preprocess externally or add explicit oracle tests |
| Advanced Spark4 variants/collations/UDTF combinations | Unlisted combinations are unknown; type names alone do not prove semantics | Validate exact API/type/context against your oracle |

Local `df.rdd` does implement a bounded in-process subset (including flatMap),
which must not be confused with distributed execution. SQL builds implement a
bounded CREATE TABLE/INSERT/database-DDL subset. Typed scalar Python UDFs work
locally. Older pages denying all three were inaccurate for this release.

Use [differences](PYSPARK_DIFFERENCES.md) for concrete local deviations,
[UDF contexts](UDF_GUIDE.md), [persistence](PERSISTENCE_GUIDE.md),
[external catalogs](EXTERNAL_CATALOGS.md), and [transport contracts](SPARK_CONNECT.md).
Report unlisted workload requirements through [support](SUPPORT.md) with a minimal
reproduction and exact Spark version; a feature request is not a release commitment.

# Local files and writer options

This reference describes the 4.20 local Python API. HTTP and Connect do not
inherit local file readers or writers. Paths below are local filesystem paths;
there is no general cloud-storage credential or URI contract.

## Reader methods

| Method | Inputs and result | Limits |
| --- | --- | --- |
| `spark.read.csv(path)` | One path string; returns a lazy DataFrame | Python shortcut defaults to string columns unless `inferSchema` is set; header defaults to true |
| `spark.read.json(path)` | Newline-delimited JSON objects; returns a lazy DataFrame | JSON inference options below; not a general multiline JSON-array reader |
| `spark.read.parquet(path)` | Parquet path; returns a lazy DataFrame with stored types | Validate logical type/nullability round trips |
| `spark.read.format(fmt).load(path)` | Format string and required path | Default format is Parquet; CSV through generic load uses native inference defaults |
| `spark.read.format("delta").load(path)` | Delta table directory | Requires native `delta`; use actual Delta logs for provider descriptors |
| `spark.read.table(name)` | Registered/warehouse/external table name | Uses session resolution; see persistence/provider guides |
| `spark.read.jdbc(url, table, properties=None)` | Native database adapter | Compiled driver required; [driver option matrix](JDBC_TESTING.md#per-driver-option-behavior) |

`option(key, value)` accepts string, bool, int or float values; `None` becomes
an empty string. Reader `options(mapping)` accepts a dictionary, not arbitrary
keyword arguments. Repeated native file options resolve to the final value.
There is no public reader `schema(...)`; cast columns after reading or create a
DataFrame from explicitly typed rows. Use exact option spellings below.

## Recognized file options

| Format / direction | Option | Values and behavior |
| --- | --- | --- |
| CSV read/write | `header` | `true`/`1` enables, other values disable; default true |
| CSV read/write | `sep` | First byte of the string is used; use one ASCII separator; default comma |
| CSV read | `inferSchema` | `true`/`1` infers types; false reads strings. Explicitly set it to avoid shortcut/generic-load differences |
| CSV read | `inferSchemaLength` | Integer sample length; native default 100 when inference is enabled |
| CSV read | `nullValue` | One string treated as null across columns |
| JSON read | `inferSchemaLength` | Sample length; 0 selects the underlying full-inference option |
| JSON read | `verifySchema` | `true`/`1` requests full inference; not validation against an explicit reader schema |
| Delta write | `mergeSchema`, `overwriteSchema` | `true`/`1` enables native schema merge/replacement; validate resulting schema |
| Delta write | `replaceWhere` | Rejected rather than converted into a full overwrite |
| Delta write | `partitionOverwriteMode=dynamic` | Rejected on overwrite |
| External table write | `sparkless.writer.maxRowsPerBatch`, `sparkless.writer.maxBytesPerBatch` | Positive output batch bounds; defaults 10,000 rows / 64 MiB; not total query memory limits |

General Spark options such as CSV `quote`, `escape`, `mode`, `multiLine`, and
writer `compression` are not implemented by these option dispatchers. They can
be retained without effect. Parquet uses native reader/writer defaults; setting
a Spark option does not establish that it was applied. Missing/malformed files,
unsupported formats and conversion failures raise errors during planning or an
action. No Spark PERMISSIVE/corrupt-record behavior is promised.

## Write modes

**Set the mode explicitly. The Python writer's initial mode is overwrite.**

| Destination | Mode behavior |
| --- | --- |
| Plain CSV/JSON/Parquet path | `append` is distinct; all other mode strings map to overwrite. `error` and `ignore` do not protect the destination |
| Delta path through `format("delta").save(path)` | `error`/`errorifexists`, `ignore`, `append`, `overwrite`; invalid modes raise ValueError |
| `saveAsTable(name, mode=None)` | Four save modes; explicit method argument overrides chained mode. Initial Python writer mode remains overwrite |
| Database `jdbc(...)` | Explicit mode recommended; driver-specific table/row and transaction behavior |

`save(path)` requires a path and returns `None`. `csv`, `json` and `parquet`
shortcuts select their format; plain path append may materialize and rewrite an
existing file. Appending to an existing directory selects another part filename.
These are not concurrent, atomic append-log guarantees. An unreadable existing
file is not a dependable append target. A new plain path can be a single file;
an existing directory receives part files. Directory overwrite is not a promise
to remove all older parts. Use fresh destinations when replacing a whole dataset.

`partitionBy(*names)` writes Hive-style directories and omits partition columns
from individual part files. Do not assume Spark partition discovery or dynamic
partition overwrite semantics. Validate a read-back before using partitioned
storage in an application. Local warehouse writes use their separate
[storage contract](PERSISTENCE_GUIDE.md#storage-and-concurrency-contract).

## Small round trip

The [file pipeline tutorial](PIPELINE_TUTORIAL.md) creates its own CSV inputs,
normalizes types, joins, aggregates and verifies a Parquet round trip. For a
smaller standalone example, use the first file example in the
[Python quickstart](python_getting_started.md#read-and-write-files).

Project and filter before collecting. Neither a small result nor bounded Arrow
batches guarantees bounded native intermediate memory. Set
`SPARKLESS_FILES_BASE` before work when file confinement is needed; SQLite has
its own `SPARKLESS_SQLITE_BASE` control. See [configuration](CONFIGURATION.md).

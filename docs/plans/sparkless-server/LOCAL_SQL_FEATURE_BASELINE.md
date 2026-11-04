---
search:
  exclude: true
---

# Existing local SQL API baseline — 2026-11-01

> **Planned scope inventory, not implementation or conformance evidence.** Governed by [SS-22](22-local-sql-feature-parity.md). Every implemented entry is required server scope; declarations, helpers and stubs need explicit behavior classification in LSQL-01.

## Provenance and refresh

Snapshot of the current working tree on 2026-11-01. This conservative union uses the checked-in runtime inventory, public Python definitions and exports (including direct imports outside `__all__`), native `#[pymethods]` names/aliases/operators, and typing declarations. Some declarations can be stale; reconcile them against the built wheel rather than treating every name as implemented. Inherited Python behavior and supported overloads/types/options also belong to their owning entry. Server launcher APIs are outside this SQL baseline.

To refresh, regenerate the runtime inventory from the candidate wheel with `python scripts/generate_documentation.py --api`, reconcile the source/export/native-binding supplements below, and review the SQL parser/translator and regression corpus. LSQL-01 must automate this reconciliation and preserve per-entry provenance and fixtures in D21's capability registry. This snapshot does not implement that checker.

| Input | SHA-256 at snapshot |
| --- | --- |
| `docs/reference/python-api-inventory.json` | `03805a9d7dff18b88faae3335dcf3d34bc9bc00ce6e49f2035947d1885391bd4` |
| `python/native/src/lib.rs` | `f1824b984603e629b3980aa73828aff7fe7b90fc8c74862873488175be8795b0` |
| `python/native/sparkless_native/_native.pyi` | `17f39f02fde45c3971b4fd4fc36a28dd219e5bb04111e04169228e7612cf9d5d` |
| `python/sparkless/sparkless/__init__.py` | `26d49aaf1c42801645ca0bca0ce334c093435f057263273051d724d188223479` |
| `python/sparkless/sparkless/sql/__init__.py` | `ce84869c7aaf9ae8983a5696af1eb17efb4c256526e314167ea99df2e7d24634` |
| `python/sparkless/sparkless/sql/functions.py` | `7c997441b4ed405f2541603aa9d75476d3407cff9d90687cd8db2fe421a9304e` |
| `python/sparkless/sparkless/sql/types.py` | `f4cf2415e6e14bd5bfd715b68b0aff203426f7318cc24279e5a18b9d45c73a2f` |
| `python/sparkless/sparkless/sql/window.py` | `8f6adf7868cd5dcab608296e34aa61c79262c4c896b0a9c6da9a6b45bf6e16ee` |
| `python/sparkless/sparkless/sql/utils.py` | `9cf0c9c783861f703610bb7ffa7f1c142645fba0d72f6b1518e2c71625d82f6b` |
| `python/sparkless/sparkless/external_catalog.py` | `a0c0d0e58e32c38732682a7b9e28ed8932f45816841b37e4de5d661756da27dc` |
| `python/sparkless/sparkless/dataframe/na.py` | `bfbc6e29d00240350d27c9422f76fa91c042f6f24edcce56db7219d8036d7128` |

## Assignment rules

All entries default to **Phase 7 / SS-16**, with these mandatory overrides from [SS-22's ownership table](22-local-sql-feature-parity.md#feature-families-and-delivery-ownership):

- **Phase 5 / SS-12:** `udf`, UDF builders/filters/introspection and bounded local RDD/DataFrame `flatMap`/`flat_map`; Python callback execution through the compatibility bridge and isolated workers.
- **Phase 6 / SS-13:** `external_catalog` and its returned objects/protocols, provider registration, Delta reads/version selection/writes. Python callback hosting uses Phase 5 workers; integrated provider behavior closes in Phase 6.
- **Phase 8 / SS-17:** non-Delta reader/writer APIs and session read conveniences, file/JDBC/SQLite/compiled-driver options and modes, saved-table/warehouse persistence, cache/persist/storage/materialization operations. A locally unsupported `writeTo` or other stub is classified as such; it is not promoted into a new local feature.
- **Client compatibility:** types/Row/Window construction, aliases, operators, formatting/conversion/error helpers, local active-session and metadata-only SparkContext interfaces use the client/bridge where standard Connect has no spelling. Their remote-data behavior closes in the owning phase; this is not an exemption from coverage.

## Named entries

Aliases are listed separately so spelling compatibility cannot disappear during translation. Members inherited from a listed base type are included in that type's behavior requirement. Every group is governed by the assignment rules above.

### ArrowBatchIterator (5)

`__enter__`, `__exit__`, `__iter__`, `__next__`, `close`

### CancellationHandle (2)

`cancelAllActiveJobs`, `cancel_all_active_actions`

### Catalog (12)

`cacheTable`, `currentDatabase`, `dropGlobalTempView`, `dropTable`, `dropTempView`, `getTable`, `isCached`, `listDatabases`, `listTables`, `setCurrentDatabase`, `tableExists`, `uncacheTable`

### ChainedWhenBuilder (1)

`then`

### Column (104)

`__add__`, `__and__`, `__getitem__`, `__invert__`, `__mod__`, `__mul__`, `__neg__`, `__or__`, `__pow__`, `__radd__`, `__rand__`, `__richcmp__`, `__rmod__`, `__rmul__`, `__ror__`, `__rpow__`, `__rsub__`, `__rtruediv__`, `__sub__`, `__truediv__`, `alias`, `and_`, `array_compact`, `array_distinct`, `asc`, `asc_nulls_first`, `asc_nulls_last`, `asinh`, `astype`, `atanh`, `between`, `cast`, `contains`, `cosh`, `crc32`, `dense_rank`, `desc`, `descNullsLast`, `desc_nulls_first`, `desc_nulls_last`, `endswith`, `eq`, `eqNullSafe`, `eq_null_safe`, `exp`, `first_value`, `floor`, `ge`, `getField`, `getItem`, `get_field`, `get_udf_call_info`, `gt`, `hour`, `initcap`, `isNotNull`, `isNull`, `is_not_null`, `is_null`, `isin`, `isnotnull`, `isnull`, `lag`, `last_day`, `last_value`, `le`, `lead`, `length`, `levenshtein`, `like`, `lower`, `lt`, `ltrim`, `minute`, `months_between`, `multiply`, `name`, `or_`, `over`, `posexplode`, `rank`, `regexp_extract`, `regexp_replace`, `repeat`, `replace`, `reverse`, `rlike`, `round`, `row_number`, `rtrim`, `second`, `sinh`, `soundex`, `split`, `startswith`, `substr`, `timestamp_seconds`, `to_utc_timestamp`, `trim`, `try_cast`, `upper`, `withField`, `with_field`, `xxhash64`

### CubeRollupData (7)

`agg`, `avg`, `count`, `max`, `mean`, `min`, `sum`

### DataFrame (91)

`__getattr__`, `__getitem__`, `__len__`, `__repr__`, `agg`, `alias`, `cache`, `cancel`, `coalesce`, `collect`, `columns`, `count`, `createOrReplaceGlobalTempView`, `createOrReplaceTempView`, `create_or_replace_temp_view`, `crossJoin`, `crosstab`, `cube`, `describe`, `distinct`, `do_order_by`, `do_order_by_impl`, `drop`, `dropDuplicates`, `drop_duplicates`, `dropna`, `dtypes`, `exceptAll`, `explain`, `fillna`, `filter`, `first`, `flatMap`, `flat_map`, `freqItems`, `groupBy`, `group_by`, `groupby`, `head`, `intersect`, `intersectAll`, `isEmpty`, `join`, `limit`, `melt`, `na`, `orderBy`, `order_by`, `order_by_exprs`, `order_by_names`, `persist`, `pivot`, `printSchema`, `print_schema`, `rdd`, `repartition`, `replace`, `rollup`, `sampleBy`, `schema`, `select`, `selectExpr`, `select_expr`, `show`, `sort`, `storage`, `subtract`, `summary`, `take`, `toArrow`, `toArrowBatches`, `toDF`, `toPandas`, `to_arrow_batches`, `union`, `unionAll`, `unionByName`, `union_all`, `union_by_name`, `unpivot`, `where`, `withColumn`, `withColumnRenamed`, `withColumns`, `withColumnsRenamed`, `with_column`, `with_column_renamed`, `with_columns`, `with_columns_renamed`, `write`, `writeTo`

### DataFrameNaFunctions (4)

`__call__`, `drop`, `fill`, `replace`

### DataFrameReader (11)

`csv`, `delta`, `format`, `jdbc`, `json`, `load`, `option`, `options`, `parquet`, `schema`, `table`

### DataFrameWriter (13)

`csv`, `format`, `insertInto`, `jdbc`, `json`, `mode`, `option`, `options`, `parquet`, `partitionBy`, `save`, `saveAsTable`, `save_as_table`

### Database (1)

`name`

### ExprStr (2)

`alias`, `sql`

### GroupedData (11)

`agg`, `avg`, `collect_list`, `collect_set`, `count`, `count_distinct`, `max`, `mean`, `min`, `pivot`, `sum`

### LocalRDD (12)

`collect`, `collect_elements`, `collect_elements_limited`, `count`, `distinct`, `filter`, `first`, `flatMap`, `isEmpty`, `map`, `reduce`, `take`

### PivotedGroupedData (7)

`agg`, `avg`, `count`, `max`, `mean`, `min`, `sum`

### RuntimeConfig (4)

`__call__`, `get`, `is_case_sensitive`, `set`

### SessionBuilder (8)

`__init__`, `appName`, `app_name`, `config`, `getOrCreate`, `get_or_create`, `master`, `option`

### SortOrder (2)

`column_name`, `descending`

### SparkContext (4)

`appName`, `app_name`, `setLogLevel`, `version`

### SparkSession (35)

`__enter__`, `__exit__`, `__init__`, `app_name`, `backend_type`, `builder`, `cancelAllActiveJobs`, `cancel_all_active_actions`, `cancellationHandle`, `cancellation_handle`, `catalog`, `conf`, `createDataFrame`, `create_dataframe`, `create_dataframe_from_rows`, `create_or_replace_global_temp_view`, `create_or_replace_temp_view`, `drop_global_temp_view`, `drop_temp_view`, `getActiveSession`, `newSession`, `range`, `read`, `read_csv`, `read_delta`, `read_delta_with_version`, `read_json`, `read_parquet`, `setExternalCatalogProvider`, `set_backend_type`, `set_external_catalog_provider`, `sparkContext`, `sql`, `stop`, `table`

### SparklessExternalCatalogError (9)

`actual_version`, `category`, `code`, `expected_version`, `identifier`, `operation`, `provider_error_type`, `retryability`, `version`

### Storage (2)

`create_table`, `schema_exists`

### Table (2)

`database`, `name`

### ThenBuilder (3)

`alias`, `otherwise`, `when`

### UdfFilterColumn (3)

`op`, `other_json`, `udf_column`

### WhenBuilder (1)

`then`

### Window (7)

`currentRow`, `orderBy`, `partitionBy`, `rangeBetween`, `rowsBetween`, `unboundedFollowing`, `unboundedPreceding`

### external_catalog (10)

`ArrowIpcStream`, `BatchWritableExternalCatalogProvider`, `ExternalCatalogProvider`, `ExternalTableDescriptor`, `ExternalTableFormat`, `ExternalTableSaveMode`, `ExternalTableWriteRequest`, `ExternalTableWriteResult`, `SparklessExternalCatalogError`, `WritableExternalCatalogProvider`

### external_catalog.ArrowIpcStream (1)

`bytes`

### external_catalog.BatchWritableExternalCatalogProvider (1)

`write_table_batches`

### external_catalog.ExternalCatalogProvider (1)

`resolve_table`

### external_catalog.ExternalTableDescriptor (8)

`canonical_location`, `format`, `permissions_context`, `provider`, `schema`, `table_id`, `validate`, `version`

### external_catalog.ExternalTableFormat (2)

`DELTA`, `__str__`

### external_catalog.ExternalTableSaveMode (5)

`APPEND`, `ERROR_IF_EXISTS`, `IGNORE`, `OVERWRITE`, `__str__`

### external_catalog.ExternalTableWriteRequest (8)

`batches`, `data`, `format`, `identifier`, `mode`, `options`, `partition_by`, `table`

### external_catalog.ExternalTableWriteResult (3)

`committed_version`, `table`, `validate_against`

### external_catalog.WritableExternalCatalogProvider (1)

`write_table`

### functions (213)

`ColumnOrName`, `DataFrame`, `abs`, `acos`, `add_months`, `approx_count_distinct`, `array`, `array_compact`, `array_contains`, `array_distinct`, `array_except`, `array_intersect`, `array_join`, `array_max`, `array_min`, `array_position`, `array_remove`, `array_sort`, `array_union`, `arrays_overlap`, `asc`, `asc_nulls_last`, `ascii`, `asin`, `asinh`, `atan`, `atan2`, `atanh`, `avg`, `base64`, `bin`, `broadcast`, `cast`, `cbrt`, `ceil`, `ceiling`, `char`, `coalesce`, `col`, `collect_list`, `collect_set`, `concat`, `concat_ws`, `conv`, `corr`, `cos`, `cosh`, `count`, `countDistinct`, `count_distinct`, `covar_pop`, `covar_samp`, `crc32`, `create_map`, `cume_dist`, `current_catalog`, `current_database`, `current_date`, `current_schema`, `current_timestamp`, `current_user`, `date_add`, `date_format`, `date_sub`, `date_trunc`, `datediff`, `dayofmonth`, `dayofweek`, `dayofyear`, `decode`, `degrees`, `dense_rank`, `desc`, `desc_nulls_first`, `desc_nulls_last`, `element_at`, `encode`, `exp`, `explode`, `explode_outer`, `expr`, `factorial`, `first`, `first_value`, `flatten`, `floor`, `format_number`, `format_string`, `from_json`, `from_unixtime`, `get_json_object`, `greatest`, `hash`, `hex`, `hour`, `hypot`, `initcap`, `input_file_name`, `instr`, `isnan`, `isnotnull`, `isnull`, `json_tuple`, `lag`, `last`, `last_day`, `last_value`, `lead`, `least`, `length`, `levenshtein`, `lit`, `lit_bool`, `lit_f64`, `lit_i64`, `lit_null`, `lit_str`, `locate`, `log`, `log10`, `log2`, `lower`, `lpad`, `ltrim`, `make_date`, `map_from_arrays`, `map_keys`, `map_values`, `max`, `md5`, `mean`, `min`, `minute`, `monotonically_increasing_id`, `month`, `months_between`, `nanvl`, `next_day`, `ntile`, `nullif`, `nvl`, `overlay`, `parse_json`, `percent_rank`, `percentile_approx`, `pmod`, `posexplode`, `posexplode_outer`, `pow`, `power`, `printf`, `quarter`, `radians`, `rank`, `regexp_extract`, `regexp_extract_all`, `regexp_like`, `regexp_replace`, `repeat`, `reverse`, `round`, `row_number`, `rpad`, `rtrim`, `schema_of_json`, `second`, `sha1`, `sha2`, `signum`, `sin`, `sinh`, `size`, `slice`, `sort_array`, `soundex`, `spark_partition_id`, `split`, `sqrt`, `stddev`, `stddev_pop`, `stddev_samp`, `struct`, `substring`, `substring_index`, `sum`, `sumDistinct`, `tan`, `timestamp_seconds`, `to_date`, `to_json`, `to_timestamp`, `to_timestamp_ltz`, `to_timestamp_ntz`, `to_utc_timestamp`, `translate`, `trim`, `trunc`, `try_add`, `try_cast`, `typeof`, `udf`, `unbase64`, `unhex`, `unix_timestamp`, `upper`, `var_pop`, `var_samp`, `variance`, `weekofyear`, `when`, `window`, `xxhash64`, `year`

### functions._CumeDistExpr (1)

`over`

### functions._DenseRankExpr (1)

`over`

### functions._FirstValueExpr (2)

`__init__`, `over`

### functions._LagExpr (2)

`__init__`, `over`

### functions._LastValueExpr (2)

`__init__`, `over`

### functions._LeadExpr (2)

`__init__`, `over`

### functions._NtileExpr (2)

`__init__`, `over`

### functions._PercentRankExpr (1)

`over`

### functions._RankExpr (1)

`over`

### functions._RowNumberExpr (1)

`over`

### functions._SortKey (3)

`__init__`, `ascending`, `name`

### functions._SparklessUdfColumn (8)

`__eq__`, `__ge__`, `__getattr__`, `__gt__`, `__init__`, `__le__`, `__lt__`, `__ne__`

### na (1)

`DataFrameNaFunctions`

### na.DataFrameNaFunctions (4)

`__init__`, `drop`, `fill`, `replace`

### sql (10)

`Column`, `DataFrame`, `DataFrameReader`, `DataFrameWriter`, `GroupedData`, `Row`, `SparkSession`, `SparkSessionBuilder`, `SparklessCancelledError`, `SparklessError`

### types (28)

`ArrayType`, `BinaryType`, `BooleanType`, `CharType`, `DataType`, `DateType`, `DayTimeIntervalType`, `DecimalType`, `DoubleType`, `FloatType`, `IntegerType`, `IntervalType`, `LongType`, `MapType`, `Row`, `RowGetItemReturn`, `RowValue`, `ShortType`, `StringType`, `StructField`, `StructMetadata`, `StructType`, `TimestampNTZType`, `TimestampType`, `VarcharType`, `VariantType`, `YearMonthInterval`, `YearMonthIntervalType`

### types.ArrayType (5)

`__init__`, `containsNull`, `elementType`, `nullable`, `simpleString`

### types.BinaryType (1)

`simpleString`

### types.BooleanType (1)

`simpleString`

### types.CharType (3)

`__init__`, `length`, `simpleString`

### types.DataType (3)

`__eq__`, `simpleString`, `typeName`

### types.DateType (1)

`simpleString`

### types.DayTimeIntervalType (4)

`__init__`, `end_field`, `simpleString`, `start_field`

### types.DecimalType (4)

`__init__`, `precision`, `scale`, `simpleString`

### types.DoubleType (1)

`simpleString`

### types.FloatType (1)

`simpleString`

### types.IntegerType (2)

`__eq__`, `simpleString`

### types.IntervalType (4)

`__init__`, `end_field`, `simpleString`, `start_field`

### types.LongType (2)

`__eq__`, `simpleString`

### types.MapType (7)

`__init__`, `keyType`, `key_type`, `simpleString`, `valueContainsNull`, `valueType`, `value_type`

### types.Row (14)

`__contains__`, `__eq__`, `__ge__`, `__getattr__`, `__getitem__`, `__gt__`, `__iter__`, `__le__`, `__lt__`, `__new__`, `asDict`, `items`, `keys`, `values`

### types.ShortType (1)

`simpleString`

### types.StringType (1)

`simpleString`

### types.StructField (5)

`__init__`, `dataType`, `metadata`, `name`, `nullable`

### types.StructType (9)

`__getitem__`, `__init__`, `__iter__`, `__len__`, `fieldNames`, `fields`, `json`, `jsonValue`, `simpleString`

### types.TimestampNTZType (2)

`simpleString`, `typeName`

### types.TimestampType (1)

`simpleString`

### types.VarcharType (3)

`__init__`, `length`, `simpleString`

### types.VariantType (1)

`simpleString`

### types.YearMonthInterval (4)

`__eq__`, `__init__`, `__repr__`, `months`

### types.YearMonthIntervalType (4)

`__init__`, `end_field`, `simpleString`, `start_field`

### types._ColumnsList (1)

`__call__`

### utils (10)

`AnalysisException`, `ConfigurationException`, `IllegalArgumentException`, `ParseException`, `PySparkAttributeError`, `PySparkRuntimeError`, `PySparkTypeError`, `PySparkValueError`, `QueryExecutionException`, `SparkUpgradeException`

### window (3)

`PartitionOrOrderSpec`, `Window`, `WindowSpec`

### window.Window (7)

`currentRow`, `orderBy`, `partitionBy`, `rangeBetween`, `rowsBetween`, `unboundedFollowing`, `unboundedPreceding`

### window.WindowSpec (5)

`__init__`, `orderBy`, `partitionBy`, `rangeBetween`, `rowsBetween`

### window._SortKey (3)

`__init__`, `ascending`, `name`

## SQL language baseline

The Python `sql` entry includes every locally accepted SQL form, not just one callable. LSQL-01 must enumerate parser/translator cases, supported options and regression combinations from these sources:

| Source | Required coverage |
| --- | --- |
| `crates/spark-sql-parser/src/` | Spark-specific statements and expression parsing, aliases, identifiers, schemas and parser extensions actually accepted by the engine |
| `crates/robin-sparkless-polars/src/sql/mod.rs` | SHOW DATABASES/TABLES, DESCRIBE/EXTENDED/DETAIL, CREATE OR REPLACE TABLE AS and normal statement dispatch in supported contexts |
| `crates/robin-sparkless-polars/src/sql/translator.rs` and adjacent SQL modules | SELECT/CTE/subquery/join/set/group/window and function expressions, CREATE TABLE/VIEW/SCHEMA/DATABASE, CTAS, INSERT, UPDATE, DELETE and DROP with all locally implemented options |
| `tests/sql/`, `tests/unit/session/test_sql*.py`, SQL regressions under `tests/dataframe/`, native SQL tests | Existing behavior and negative cases, schemas/types/empty results, local deviations and supported compositions; include new regressions before candidate freeze |

Apply these to HTTP SQL and Connect SQL under V58. Worker/storage/connector dependencies use their own phase gates; native language coverage belongs to Phase 7. Do not infer support for a statement merely from parser acceptance.

## Snapshot totals

85 groups; 798 named entries before alias deduplication and behavior reconciliation. These are scope counts, not implemented feature counts, test counts or a parity percentage.

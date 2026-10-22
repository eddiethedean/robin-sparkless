#![cfg(all(feature = "delta", feature = "sql"))]

use polars::prelude::{
    DataFrame as PlDataFrame, IntoSeries, NamedFrom, Series as PlSeries, TimeUnit, TimeZone,
};
use robin_sparkless_polars::{
    DataType, EngineError, ExternalCatalogProvider, ExternalTableDescriptor, ExternalTableFormat,
    ExternalTableWriteRequest, ExternalTableWriteResult, SaveMode, SparkSession, StructField,
    StructType, WriteFormat,
};
use serde_json::json;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

struct Provider {
    table: ExternalTableDescriptor,
    calls: AtomicUsize,
    miss_first: bool,
    writes: Mutex<Vec<ExternalTableWriteRequest>>,
}
impl ExternalCatalogProvider for Provider {
    fn resolve_table(&self, _: &str) -> Result<Option<ExternalTableDescriptor>, EngineError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(if self.miss_first && call == 0 {
            None
        } else {
            Some(self.table.clone())
        })
    }
    fn write_table(
        &self,
        request: ExternalTableWriteRequest,
    ) -> Result<ExternalTableWriteResult, EngineError> {
        let mut table = request.table.clone();
        table.version = Some(table.version.unwrap_or(0) + 1);
        self.writes.lock().unwrap().push(request);
        Ok(ExternalTableWriteResult {
            committed_version: table.version.unwrap(),
            table,
        })
    }
}
fn attach(
    session: &SparkSession,
    path: &std::path::Path,
    schema: StructType,
    version: Option<i64>,
    miss_first: bool,
) -> Arc<Provider> {
    let provider = Arc::new(Provider {
        table: ExternalTableDescriptor {
            table_id: "review-id".into(),
            canonical_location: path.to_string_lossy().into_owned(),
            schema,
            version,
            provider: "test".into(),
            permissions_context: None,
            format: ExternalTableFormat::Delta,
        },
        calls: AtomicUsize::new(0),
        miss_first,
        writes: Mutex::new(Vec::new()),
    });
    session
        .set_external_catalog_provider(Some(provider.clone()))
        .unwrap();
    provider
}
fn frame(session: &SparkSession, id: i64) -> robin_sparkless_polars::DataFrame {
    session
        .create_dataframe(vec![(id, 2, "x".to_string())], vec!["id", "age", "name"])
        .unwrap()
}

#[test]
fn review_external_delta_timestamp_predicate_uses_physical_instant() {
    for (timezone, instant, value) in [
        ("UTC", "2026-10-18T03:30:00Z", "2026-10-18 03:30:00"),
        (
            "UTC",
            "2026-10-18T03:30:00.123456Z",
            "2026-10-18 03:30:00.123456",
        ),
        ("UTC", "2026-10-18T03:30:00Z", "2026-10-17T23:30:00-04:00"),
        (
            "America/New_York",
            "2026-10-18T07:30:00Z",
            "2026-10-18 03:30:00",
        ),
        (
            "America/New_York",
            "2026-03-08T07:30:00Z",
            "2026-03-08 02:30:00",
        ),
        ("+05:30", "2026-10-18T03:30:00Z", "2026-10-18 09:00:00"),
    ] {
        check_external_timestamp_boundary(timezone, instant, value);
    }
}

fn check_external_timestamp_boundary(timezone: &str, instant: &str, value: &str) {
    let dir = tempfile::tempdir().unwrap();
    let mut session = SparkSession::default();
    session.set_config("spark.sql.session.timeZone", timezone);

    let micros = chrono::DateTime::parse_from_rfc3339(instant)
        .unwrap()
        .timestamp_micros();
    let event_ts = PlSeries::new("event_ts".into(), [micros])
        .i64()
        .unwrap()
        .clone()
        .into_datetime(TimeUnit::Microseconds, Some(TimeZone::UTC))
        .into_series();
    let polars_frame = PlDataFrame::new(
        1,
        vec![
            PlSeries::new("order_id".into(), [1i64]).into(),
            event_ts.into(),
        ],
    )
    .unwrap();
    let input = session.create_dataframe_from_polars(polars_frame);
    input.write_delta(dir.path(), true, false).unwrap();
    attach(
        &session,
        dir.path(),
        input.schema().unwrap(),
        Some(0),
        false,
    );

    // BrickHouse's SQLGlot renderer turns the typed literal into a CAST.
    for boundary in [
        format!("TIMESTAMP '{value}'"),
        format!("CAST('{value}' AS TIMESTAMP)"),
    ] {
        for op in ["=", ">="] {
            let rows = session
                .sql(&format!(
                    "SELECT order_id FROM catalog.schema.orders WHERE event_ts {op} {boundary}"
                ))
                .unwrap()
                .collect_as_json_rows()
                .unwrap();
            assert_eq!(rows.len(), 1, "{timezone}: {op} {boundary}");
            assert_eq!(rows[0]["order_id"], 1);
        }

        let rows = session
            .sql(&format!(
                "SELECT order_id FROM catalog.schema.orders WHERE event_ts < {boundary}"
            ))
            .unwrap()
            .collect_as_json_rows()
            .unwrap();
        assert!(rows.is_empty(), "boundary: {boundary}");
    }
}

#[test]
fn review_writer_format_reaches_provider() {
    let session = SparkSession::default();
    let input = frame(&session, 1);
    let provider = attach(
        &session,
        std::path::Path::new("/unused"),
        input.schema().unwrap(),
        Some(0),
        false,
    );
    input
        .write()
        .format(WriteFormat::Csv)
        .save_as_table(&session, "catalog.schema.orders", SaveMode::Overwrite)
        .unwrap();
    assert_eq!(
        provider.writes.lock().unwrap()[0].format.as_deref(),
        Some("csv")
    );
}

#[test]
fn review_array_struct_is_not_logical_map() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("_delta_log");
    std::fs::create_dir(&log).unwrap();
    let schema = json!({"type":"struct","fields":[{"name":"m","type":{"type":"array","elementType":{"type":"struct","fields":[{"name":"key","type":"string","nullable":true,"metadata":{}},{"name":"value","type":"long","nullable":true,"metadata":{}}]},"containsNull":true},"nullable":true,"metadata":{}}]});
    let actions = [
        json!({"protocol":{"minReaderVersion":1,"minWriterVersion":2}}),
        json!({"metaData":{"id":"review-array","format":{"provider":"parquet","options":{}},"schemaString":schema.to_string(),"partitionColumns":[],"configuration":{},"createdTime":0}}),
    ];
    std::fs::write(
        log.join("00000000000000000000.json"),
        actions.iter().map(|v| format!("{v}\n")).collect::<String>(),
    )
    .unwrap();
    let session = SparkSession::default();
    attach(
        &session,
        dir.path(),
        StructType::new(vec![StructField::new(
            "m".into(),
            DataType::Map(Box::new(DataType::String), Box::new(DataType::Long)),
            true,
        )]),
        Some(0),
        false,
    );
    assert!(
        session.table("catalog.schema.orders").is_err(),
        "a logical array was accepted as a catalog map"
    );
}

#[test]
fn review_insert_provider_miss_is_not_retried_or_registered_locally() {
    let dir = tempfile::tempdir().unwrap();
    let session = SparkSession::default();
    let input = frame(&session, 1);
    input.write_delta(dir.path(), true, false).unwrap();
    let provider = attach(&session, dir.path(), input.schema().unwrap(), Some(0), true);
    assert!(
        session
            .sql("INSERT INTO catalog.schema.orders VALUES (3, 4, 'y')")
            .is_err()
    );
    assert_eq!(provider.writes.lock().unwrap().len(), 0,);
    assert!(!session.saved_table_exists("catalog.schema.orders"));
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
}

#[test]
fn review_external_time_travel_honors_requested_version() {
    let dir = tempfile::tempdir().unwrap();
    let session = SparkSession::default();
    let input = frame(&session, 1);
    input.write_delta(dir.path(), true, false).unwrap();
    frame(&session, 2)
        .write_delta(dir.path(), false, false)
        .unwrap();
    attach(
        &session,
        dir.path(),
        input.schema().unwrap(),
        Some(1),
        false,
    );
    assert_eq!(
        session
            .read_delta_path_with_version(dir.path(), Some(0))
            .unwrap()
            .count()
            .unwrap(),
        1
    );
    assert_eq!(
        session
            .read_delta_with_version("catalog.schema.orders", Some(0))
            .unwrap()
            .count()
            .unwrap(),
        1,
        "the catalog path ignored the requested snapshot version"
    );
}

#[test]
fn review_external_time_travel_accepts_the_historical_schema() {
    let dir = tempfile::tempdir().unwrap();
    let session = SparkSession::default();
    let version_zero = session
        .create_dataframe_from_rows(
            vec![vec![json!(1)]],
            vec![("id".into(), "bigint".into())],
            true,
            false,
        )
        .unwrap();
    version_zero.write_delta(dir.path(), true, false).unwrap();
    let version_one = session
        .create_dataframe_from_rows(
            vec![vec![json!("later")]],
            vec![("name".into(), "string".into())],
            true,
            false,
        )
        .unwrap();
    version_one
        .write_delta_with_save_options(dir.path(), SaveMode::Overwrite, false, true, &[])
        .unwrap();
    attach(
        &session,
        dir.path(),
        version_one.schema().unwrap(),
        Some(1),
        false,
    );

    let historical = session
        .read_delta_with_version("catalog.schema.orders", Some(0))
        .unwrap();
    assert_eq!(historical.schema().unwrap().fields()[0].name, "id");
    assert_eq!(historical.collect_as_json_rows().unwrap()[0]["id"], 1);
}

#[test]
fn review_unqualified_temp_view_does_not_capture_external_insert() {
    let dir = tempfile::tempdir().unwrap();
    let session = SparkSession::default();
    let input = frame(&session, 1);
    input.write_delta(dir.path(), true, false).unwrap();
    let provider = attach(
        &session,
        dir.path(),
        input.schema().unwrap(),
        Some(0),
        false,
    );
    session.create_or_replace_temp_view("orders", frame(&session, 99));
    assert_eq!(
        session
            .table("catalog.schema.orders")
            .unwrap()
            .collect_as_json_rows()
            .unwrap()[0]["id"],
        1
    );
    session
        .sql("INSERT INTO catalog.schema.orders VALUES (3, 4, 'y')")
        .unwrap();
    assert_eq!(
        provider.writes.lock().unwrap().len(),
        1,
        "INSERT bound the external relation but wrote a local qualified temp view; session rows={}, stored rows={}",
        session
            .table("catalog.schema.orders")
            .unwrap()
            .count()
            .unwrap(),
        session
            .read_delta_path(dir.path())
            .unwrap()
            .count()
            .unwrap()
    );
}

#[test]
fn review_create_if_not_exists_preserves_external_table() {
    let dir = tempfile::tempdir().unwrap();
    let session = SparkSession::default();
    let input = frame(&session, 1);
    input.write_delta(dir.path(), true, false).unwrap();
    attach(
        &session,
        dir.path(),
        input.schema().unwrap(),
        Some(0),
        false,
    );
    assert_eq!(
        session
            .table("catalog.schema.orders")
            .unwrap()
            .count()
            .unwrap(),
        1
    );
    session
        .sql(
            "CREATE TABLE IF NOT EXISTS catalog.schema.orders (id BIGINT, age BIGINT, name STRING)",
        )
        .unwrap();
    assert!(
        session
            .sql("CREATE TABLE `catalog`.`schema`.`orders` (id BIGINT)")
            .is_err()
    );
    session
        .sql("CREATE TABLE IF NOT EXISTS `catalog`.`schema`.`orders` (id BIGINT)")
        .unwrap();
    assert_eq!(
        session
            .table("catalog.schema.orders")
            .unwrap()
            .count()
            .unwrap(),
        1,
        "CREATE IF NOT EXISTS shadowed a real external table with an empty local table"
    );
}

#[test]
fn review_global_temp_write_does_not_call_external_provider() {
    let session = SparkSession::default();
    let input = frame(&session, 1);
    session.create_or_replace_global_temp_view("review_global", input.clone());
    let provider = attach(
        &session,
        std::path::Path::new("/unused"),
        input.schema().unwrap(),
        Some(0),
        false,
    );
    assert!(
        input
            .write()
            .save_as_table(&session, "global_temp.review_global", SaveMode::Append)
            .is_err()
    );
    assert_eq!(
        session
            .table("global_temp.review_global")
            .unwrap()
            .collect_as_json_rows()
            .unwrap()[0]["id"],
        1
    );
    assert!(provider.writes.lock().unwrap().is_empty());
}

#[test]
fn review_external_miss_does_not_make_qualified_local_suffix_exist() {
    struct Missing;
    impl ExternalCatalogProvider for Missing {
        fn resolve_table(&self, _: &str) -> Result<Option<ExternalTableDescriptor>, EngineError> {
            Ok(None)
        }
    }

    let session = SparkSession::default();
    session.create_or_replace_temp_view("orders", frame(&session, 1));
    session
        .set_external_catalog_provider(Some(Arc::new(Missing)))
        .unwrap();
    assert!(!session.table_exists("catalog.schema.orders"));
    assert!(session
        .sql("CREATE TABLE IF NOT EXISTS catalog.schema.orders (id BIGINT, age BIGINT, name STRING)")
        .is_ok());
    assert!(session.table("catalog.schema.orders").is_ok());
}

#[test]
fn review_table_exists_surfaces_provider_errors() {
    struct Unavailable;
    impl ExternalCatalogProvider for Unavailable {
        fn resolve_table(&self, _: &str) -> Result<Option<ExternalTableDescriptor>, EngineError> {
            Err(EngineError::User("catalog service unavailable".into()))
        }
    }

    let session = SparkSession::default();
    session
        .set_external_catalog_provider(Some(Arc::new(Unavailable)))
        .unwrap();
    let error = session
        .try_table_exists("catalog.schema.orders")
        .expect_err("a catalog failure must not be reported as a table miss");
    assert!(error.to_string().contains("catalog service unavailable"));
}

#[test]
fn review_external_map_validation_uses_the_pinned_delta_schema() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("_delta_log");
    std::fs::create_dir(&log).unwrap();
    let map_schema = json!({"type":"struct","fields":[{"name":"attrs","type":{"type":"map","keyType":"string","valueType":"long","valueContainsNull":true},"nullable":true,"metadata":{}}]});
    let array_schema = json!({"type":"struct","fields":[{"name":"attrs","type":{"type":"array","elementType":{"type":"struct","fields":[{"name":"key","type":"string","nullable":true,"metadata":{}},{"name":"value","type":"long","nullable":true,"metadata":{}}]},"containsNull":true},"nullable":true,"metadata":{}}]});
    let protocol = json!({"protocol":{"minReaderVersion":1,"minWriterVersion":2}});
    let metadata = |schema: &serde_json::Value| json!({"metaData":{"id":"review-map","format":{"provider":"parquet","options":{}},"schemaString":schema.to_string(),"partitionColumns":[],"configuration":{},"createdTime":0}});
    for (version, schema) in [(0, &map_schema), (1, &array_schema)] {
        let actions = if version == 0 {
            vec![protocol.clone(), metadata(schema)]
        } else {
            vec![metadata(schema)]
        };
        std::fs::write(
            log.join(format!("{version:020}.json")),
            actions
                .iter()
                .map(|action| format!("{action}\n"))
                .collect::<String>(),
        )
        .unwrap();
    }
    let session = SparkSession::default();
    let schema = StructType::new(vec![StructField::new(
        "attrs".into(),
        DataType::Map(Box::new(DataType::String), Box::new(DataType::Long)),
        true,
    )]);
    attach(&session, dir.path(), schema, Some(0), false);
    assert_eq!(
        session
            .table("catalog.schema.orders")
            .unwrap()
            .count()
            .unwrap(),
        0
    );
}

struct ErrorThenHit {
    inner: Arc<Provider>,
    calls: AtomicUsize,
}
impl ExternalCatalogProvider for ErrorThenHit {
    fn resolve_table(
        &self,
        identifier: &str,
    ) -> Result<Option<ExternalTableDescriptor>, EngineError> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            Err(EngineError::User("catalog denied the read".into()))
        } else {
            self.inner.resolve_table(identifier)
        }
    }
}

#[test]
fn review_named_delta_read_surfaces_provider_error() {
    let dir = tempfile::tempdir().unwrap();
    let session = SparkSession::default();
    let input = frame(&session, 1);
    input.write_delta(dir.path(), true, false).unwrap();
    let inner = attach(
        &session,
        dir.path(),
        input.schema().unwrap(),
        Some(0),
        false,
    );
    session
        .set_external_catalog_provider(Some(Arc::new(ErrorThenHit {
            inner,
            calls: AtomicUsize::new(0),
        })))
        .unwrap();
    assert!(
        session.read_delta("catalog.schema.orders").is_err(),
        "provider error was swallowed and a second lookup was attempted"
    );
}

#[test]
fn review_cancel_during_read_only_sql_planning_is_reported() {
    let dir = tempfile::tempdir().unwrap();
    let session = SparkSession::default();
    let input = frame(&session, 1);
    input.write_delta(dir.path(), true, false).unwrap();
    let inner = attach(
        &session,
        dir.path(),
        input.schema().unwrap(),
        Some(0),
        false,
    );
    let token = robin_sparkless_core::CancellationToken::new();
    struct CancellingProvider {
        inner: Arc<Provider>,
        token: robin_sparkless_core::CancellationToken,
    }
    impl ExternalCatalogProvider for CancellingProvider {
        fn resolve_table(
            &self,
            identifier: &str,
        ) -> Result<Option<ExternalTableDescriptor>, EngineError> {
            self.token.cancel();
            self.inner.resolve_table(identifier)
        }
    }
    session
        .set_external_catalog_provider(Some(Arc::new(CancellingProvider {
            inner,
            token: token.clone(),
        })))
        .unwrap();

    let error = match session.sql_with_cancellation("SELECT * FROM catalog.schema.orders", &token) {
        Ok(_) => panic!("cancellation during read-only SQL planning must be reported"),
        Err(error) => error,
    };
    assert!(matches!(error, EngineError::Cancelled(_)));
}

#[test]
fn review_strict_catalog_read_rejects_plain_parquet() {
    let dir = tempfile::tempdir().unwrap();
    let session = SparkSession::default();
    let input = frame(&session, 1);
    input
        .write()
        .parquet(dir.path().join("part.parquet"))
        .unwrap();
    attach(&session, dir.path(), input.schema().unwrap(), None, false);
    assert!(session.table("catalog.schema.orders").is_err());
}

#[test]
fn review_descriptor_version_reads_the_pinned_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let session = SparkSession::default();
    let input = frame(&session, 1);
    input.write_delta(dir.path(), true, false).unwrap();
    frame(&session, 2)
        .write_delta(dir.path(), false, false)
        .unwrap();
    attach(
        &session,
        dir.path(),
        input.schema().unwrap(),
        Some(0),
        false,
    );
    assert_eq!(
        session
            .table("catalog.schema.orders")
            .unwrap()
            .count()
            .unwrap(),
        1
    );
}

#[test]
fn review_writer_options_and_partition_columns_reach_provider() {
    let session = SparkSession::default();
    let input = frame(&session, 1);
    let provider = attach(
        &session,
        std::path::Path::new("/unused"),
        input.schema().unwrap(),
        Some(0),
        false,
    );
    let options = vec![
        ("mergeSchema".into(), "false".into()),
        ("mergeSchema".into(), "true".into()),
        ("sparkless.writer.format".into(), "delta".into()),
    ];
    input
        .write()
        .partition_by(["AGE"])
        .save_as_table_with_options(
            &session,
            "catalog.schema.orders",
            SaveMode::Append,
            &options,
        )
        .unwrap();
    let writes = provider.writes.lock().unwrap();
    assert_eq!(writes[0].partition_by, vec!["age"]);
    assert_eq!(writes[0].options, options[..2]);
    assert_eq!(writes[0].format.as_deref(), Some("delta"));
}

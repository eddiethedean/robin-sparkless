#![cfg(feature = "sql")]

use polars::prelude::*;
use robin_sparkless_polars::SparkSession;

fn session_with_timestamp(timestamp: &str, timezone: &str) -> SparkSession {
    let micros = chrono::NaiveDateTime::parse_from_str(timestamp, "%Y-%m-%d %H:%M:%S")
        .unwrap()
        .and_utc()
        .timestamp_micros();
    let event_ts = Series::new("event_ts".into(), [micros])
        .cast(&DataType::Datetime(
            TimeUnit::Microseconds,
            Some(TimeZone::UTC),
        ))
        .unwrap();
    let frame = DataFrame::new(
        1,
        vec![
            Series::new("order_id".into(), [1i64]).into(),
            event_ts.into(),
        ],
    )
    .unwrap();

    let mut session = SparkSession::default();
    session.set_config("spark.sql.session.timeZone", timezone);
    let dataframe = session.create_dataframe_from_polars(frame);
    session.create_or_replace_temp_view("orders", dataframe);
    session
}

#[test]
fn utc_column_matches_space_separated_timestamp_boundary() {
    let session = session_with_timestamp("2026-10-18 03:30:00", "UTC");

    let rows = session
        .sql("SELECT order_id FROM orders WHERE event_ts >= TIMESTAMP '2026-10-18 03:30:00'")
        .unwrap()
        .collect_as_json_rows()
        .unwrap();
    assert_eq!(rows.len(), 1);

    let rows = session
        .sql("SELECT order_id FROM orders WHERE event_ts < TIMESTAMP '2026-10-18 03:30:00'")
        .unwrap()
        .collect_as_json_rows()
        .unwrap();
    assert!(rows.is_empty());
}

#[test]
fn zone_free_timestamp_literal_uses_session_timezone() {
    let session = session_with_timestamp("2026-10-18 07:30:00", "America/New_York");
    let rows = session
        .sql("SELECT order_id FROM orders WHERE event_ts = TIMESTAMP '2026-10-18 03:30:00'")
        .unwrap()
        .collect_as_json_rows()
        .unwrap();

    assert_eq!(rows.len(), 1);
}

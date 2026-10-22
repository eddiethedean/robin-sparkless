#![cfg(feature = "sql")]

use polars::prelude::*;
use robin_sparkless_polars::SparkSession;

fn session_with_timestamp(timestamp: &str, timezone: &str) -> SparkSession {
    session_with_timestamp_dtype(
        timestamp,
        timezone,
        TimeUnit::Microseconds,
        Some(TimeZone::UTC),
    )
}

fn session_with_timestamp_unit(timestamp: &str, timezone: &str, unit: TimeUnit) -> SparkSession {
    session_with_timestamp_dtype(timestamp, timezone, unit, Some(TimeZone::UTC))
}

fn session_with_timestamp_dtype(
    timestamp: &str,
    timezone: &str,
    unit: TimeUnit,
    column_timezone: Option<TimeZone>,
) -> SparkSession {
    let micros = chrono::NaiveDateTime::parse_from_str(timestamp, "%Y-%m-%d %H:%M:%S")
        .unwrap()
        .and_utc()
        .timestamp_micros();
    let ticks = match unit {
        TimeUnit::Milliseconds => micros / 1_000,
        TimeUnit::Microseconds => micros,
        TimeUnit::Nanoseconds => micros * 1_000,
    };
    let event_ts = Series::new("event_ts".into(), [ticks])
        .i64()
        .unwrap()
        .clone()
        .into_datetime(unit, column_timezone.clone())
        .into_series();
    assert_eq!(event_ts.dtype(), &DataType::Datetime(unit, column_timezone));
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

#[test]
fn fixed_offset_session_timezone_is_supported() {
    let session = session_with_timestamp("2026-10-18 03:30:00", "+05:30");
    let rows = session
        .sql("SELECT order_id FROM orders WHERE event_ts = TIMESTAMP '2026-10-18 09:00:00'")
        .unwrap()
        .collect_as_json_rows()
        .unwrap();

    assert_eq!(rows.len(), 1);
}

#[test]
fn date_only_timestamp_literal_defaults_to_midnight() {
    let session = session_with_timestamp("2026-10-18 00:00:00", "UTC");
    let rows = session
        .sql("SELECT order_id FROM orders WHERE event_ts = TIMESTAMP '2026-10-18'")
        .unwrap()
        .collect_as_json_rows()
        .unwrap();

    assert_eq!(rows.len(), 1);
}

#[test]
fn timestamp_comparison_preserves_other_time_units() {
    for unit in [TimeUnit::Milliseconds, TimeUnit::Nanoseconds] {
        let session = session_with_timestamp_unit("2026-10-18 03:30:00", "UTC", unit);
        let rows = session
            .sql("SELECT order_id FROM orders WHERE event_ts = TIMESTAMP '2026-10-18 03:30:00'")
            .unwrap()
            .collect_as_json_rows()
            .unwrap();
        assert_eq!(rows.len(), 1, "comparison failed for {unit:?}");
    }
}

#[test]
fn naive_timestamp_column_matches_typed_literal() {
    let session =
        session_with_timestamp_dtype("2026-10-18 03:30:00", "UTC", TimeUnit::Microseconds, None);
    let rows = session
        .sql("SELECT order_id FROM orders WHERE event_ts = TIMESTAMP '2026-10-18 03:30:00'")
        .unwrap()
        .collect_as_json_rows()
        .unwrap();

    assert_eq!(rows.len(), 1);
}

#[test]
fn nanosecond_timestamp_compares_outside_nanosecond_epoch_range() {
    let session = session_with_timestamp_unit("2026-10-18 03:30:00", "UTC", TimeUnit::Nanoseconds);
    let rows = session
        .sql("SELECT order_id FROM orders WHERE event_ts < TIMESTAMP '2500-01-01 00:00:00'")
        .unwrap()
        .collect_as_json_rows()
        .unwrap();

    assert_eq!(rows.len(), 1);
}

#[test]
fn hour_only_timestamp_defaults_minutes_and_seconds() {
    let session = session_with_timestamp("2026-10-18 03:00:00", "UTC");
    let rows = session
        .sql("SELECT TIMESTAMP '2026-10-18 03' AS ts FROM orders")
        .unwrap()
        .collect_as_json_rows()
        .unwrap();

    assert_eq!(rows[0]["ts"], "2026-10-18T03:00:00.000000");
}

#[test]
fn explicit_region_timezone_after_fraction_is_honored() {
    let session = session_with_timestamp("2026-10-18 07:30:00", "UTC");
    let rows = session
        .sql("SELECT TIMESTAMP '2026-10-18 03:30:00.123456America/New_York' AS ts FROM orders")
        .unwrap()
        .collect_as_json_rows()
        .unwrap();

    assert_eq!(rows[0]["ts"], "2026-10-18T07:30:00.123456");

    let rows = session
        .sql("SELECT TIMESTAMP '2026-10-18 03:30:00+04:00' AS ts FROM orders")
        .unwrap()
        .collect_as_json_rows()
        .unwrap();
    assert_eq!(rows[0]["ts"], "2026-10-17T23:30:00.000000");
}

#[test]
fn date_literal_uses_session_timezone_when_compared_with_timestamp() {
    let session = session_with_timestamp("2026-10-18 04:00:00", "America/New_York");
    let rows = session
        .sql("SELECT order_id FROM orders WHERE event_ts = DATE '2026-10-18'")
        .unwrap()
        .collect_as_json_rows()
        .unwrap();

    assert_eq!(rows.len(), 1);
}

#[test]
fn cast_timestamp_expression_compares_with_typed_literal() {
    let session = session_with_timestamp("2026-10-18 03:30:00", "UTC");
    let rows = session
        .sql("SELECT order_id FROM orders WHERE CAST(event_ts AS TIMESTAMP) = TIMESTAMP '2026-10-18 03:30:00'")
        .unwrap()
        .collect_as_json_rows()
        .unwrap();

    assert_eq!(rows.len(), 1);
}

#[test]
fn non_utc_column_timezone_compares_by_instant() {
    let session = session_with_timestamp_dtype(
        "2026-10-18 07:30:00",
        "America/New_York",
        TimeUnit::Microseconds,
        TimeZone::opt_try_new(Some("America/New_York")).unwrap(),
    );
    let rows = session
        .sql("SELECT order_id FROM orders WHERE event_ts = TIMESTAMP '2026-10-18 03:30:00'")
        .unwrap()
        .collect_as_json_rows()
        .unwrap();

    assert_eq!(rows.len(), 1);
}

#[test]
fn date_and_timestamp_literals_compare_in_session_timezone() {
    let session = session_with_timestamp("2026-10-18 04:00:00", "America/New_York");
    let rows = session
        .sql(
            "SELECT order_id FROM orders WHERE DATE '2026-10-18' = TIMESTAMP '2026-10-18 00:00:00'",
        )
        .unwrap()
        .collect_as_json_rows()
        .unwrap();

    assert_eq!(rows.len(), 1);
}

#[test]
fn cast_timestamp_expression_compares_inside_not() {
    let session = session_with_timestamp("2026-10-18 03:30:00", "UTC");
    let rows = session
        .sql("SELECT order_id FROM orders WHERE NOT (CAST(event_ts AS TIMESTAMP) < TIMESTAMP '2026-10-18 03:30:00')")
        .unwrap()
        .collect_as_json_rows()
        .unwrap();

    assert_eq!(rows.len(), 1);
}

#[test]
fn date_at_midnight_timezone_gap_resolves_to_first_valid_instant() {
    let session = session_with_timestamp("2018-11-04 03:00:00", "America/Sao_Paulo");
    let rows = session
        .sql("SELECT order_id FROM orders WHERE event_ts = DATE '2018-11-04'")
        .unwrap()
        .collect_as_json_rows()
        .unwrap();

    assert_eq!(rows.len(), 1);
}

#[test]
fn timestamp_inside_timezone_gap_shifts_forward_by_gap() {
    let session = session_with_timestamp("2026-03-08 07:30:00", "America/New_York");
    let rows = session
        .sql("SELECT TIMESTAMP '2026-03-08 02:30:00' AS ts FROM orders")
        .unwrap()
        .collect_as_json_rows()
        .unwrap();

    assert_eq!(rows[0]["ts"], "2026-03-08T07:30:00.000000");
}

#[test]
fn invalid_timezone_after_fraction_is_rejected() {
    let session = session_with_timestamp("2026-10-18 03:30:00", "UTC");
    let result =
        session.sql("SELECT TIMESTAMP '2026-10-18 03:30:00.123456Invalid/Zone' AS ts FROM orders");

    assert!(result.is_err());
}

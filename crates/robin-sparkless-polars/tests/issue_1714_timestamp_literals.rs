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
fn compact_fixed_offset_session_timezone_is_supported() {
    let session = session_with_timestamp("2026-10-17 18:30:00", "UTC+0530");
    let rows = session
        .sql("SELECT order_id FROM orders WHERE event_ts = DATE '2026-10-18'")
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
fn timestamp_cast_comparisons_cover_reversed_nested_and_null_expressions() {
    let session = session_with_timestamp("2026-10-18 03:30:00", "UTC");
    let rows = session
        .sql(
            "SELECT event_ts = CAST('2026-10-18 03:30:00' AS TIMESTAMP) AS equal, \
             CAST('2026-10-18 03:30:00' AS TIMESTAMP) > event_ts AS before, \
             NOT (event_ts < CAST('2026-10-18 03:30:00' AS TIMESTAMP)) AS at_or_after, \
             event_ts = TRY_CAST('invalid' AS TIMESTAMP) AS invalid \
             FROM orders",
        )
        .unwrap()
        .collect_as_json_rows()
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["equal"], true);
    assert_eq!(rows[0]["before"], false);
    assert_eq!(rows[0]["at_or_after"], true);
    assert!(rows[0]["invalid"].is_null());

    let rows = session
        .sql(
            "SELECT order_id FROM orders \
             WHERE NOT (event_ts < CAST('2026-10-18 03:30:00' AS TIMESTAMP))",
        )
        .unwrap()
        .collect_as_json_rows()
        .unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn timestamp_cast_binds_timezone_before_execution_and_preserves_ntz() {
    let mut session = session_with_timestamp("2026-10-18 07:30:00", "America/New_York");
    let query = "SELECT CAST('2026-10-18 03:30:00' AS TIMESTAMP) AS instant, \
                 CAST('2026-10-18 03:30:00' AS TIMESTAMP_NTZ) AS local_time FROM orders";
    let new_york = session.sql(query).unwrap();
    session.set_config("spark.sql.session.timeZone", "UTC");
    let utc = session.sql(query).unwrap();

    let rows = new_york.collect_as_json_rows().unwrap();
    assert_eq!(rows[0]["instant"], "2026-10-18T07:30:00.000000");
    assert_eq!(rows[0]["local_time"], "2026-10-18T03:30:00.000000");
    let rows = utc.collect_as_json_rows().unwrap();
    assert_eq!(rows[0]["instant"], "2026-10-18T03:30:00.000000");
    assert_eq!(rows[0]["local_time"], "2026-10-18T03:30:00.000000");
}

#[test]
fn timestamp_cast_rejects_invalid_suffixes_and_preserves_nulls() {
    let mut session = session_with_timestamp("2026-10-18 03:30:00", "UTC");
    session.set_config("spark.sql.ansi.enabled", "true");
    for value in ["not-a-timestamp", "2026-10-18 03:30:00.123456Invalid/Zone"] {
        let result = session
            .sql(&format!(
                "SELECT CAST('{value}' AS TIMESTAMP) AS ts FROM orders"
            ))
            .and_then(|frame| frame.collect_as_json_rows());
        assert!(result.is_err(), "CAST accepted {value:?}");
        let rows = session
            .sql(&format!(
                "SELECT TRY_CAST('{value}' AS TIMESTAMP) AS ts FROM orders"
            ))
            .unwrap()
            .collect_as_json_rows()
            .unwrap();
        assert!(rows[0]["ts"].is_null());
    }
    session.set_config("spark.sql.ansi.enabled", "false");
    let rows = session
        .sql("SELECT CAST('invalid' AS TIMESTAMP) AS invalid, CAST(NULL AS TIMESTAMP) AS missing FROM orders")
        .unwrap()
        .collect_as_json_rows()
        .unwrap();
    assert!(rows[0]["invalid"].is_null());
    assert!(rows[0]["missing"].is_null());
}

#[test]
fn timestamp_cast_handles_unicode_offsets_and_legacy_datetime_formats() {
    let mut session = session_with_timestamp("2026-10-18 03:30:00", "UTC");
    session.set_config("spark.sql.ansi.enabled", "true");
    for value in [
        "2026-10-18 03:30:00+1€",
        "-0044-03-15 12:34:56",
        "-0001-01-01T00:00:00",
        "2026-10-18   03:30:00",
        "2026-10-18\t03:30:00",
    ] {
        let rows = session
            .sql(&format!(
                "SELECT CAST(CAST('{value}' AS TIMESTAMP) AS STRING) AS ts FROM orders"
            ))
            .and_then(|frame| frame.collect_as_json_rows());
        if value.contains('€') {
            assert!(rows.is_err(), "CAST accepted malformed offset {value:?}");
            let rows = session
                .sql(&format!(
                    "SELECT TRY_CAST('{value}' AS TIMESTAMP) AS ts FROM orders"
                ))
                .unwrap()
                .collect_as_json_rows()
                .unwrap();
            assert!(rows[0]["ts"].is_null());
        } else {
            assert!(
                rows.is_ok(),
                "CAST rejected previously accepted value {value:?}"
            );
        }
    }
}

#[test]
fn timestamp_cast_chains_preserve_session_local_and_ntz_values() {
    let session = session_with_timestamp("2026-10-18 07:30:00", "America/New_York");
    let rows = session
        .sql(
            "SELECT CAST(DATE '2026-10-18' AS TIMESTAMP) = CAST('2026-10-18' AS TIMESTAMP) AS same_date, \
             CAST(CAST(CAST('2026-10-18 03:30:00' AS TIMESTAMP) AS TIMESTAMP_NTZ) AS TIMESTAMP) = CAST('2026-10-18 03:30:00' AS TIMESTAMP) AS ntz_round_trip, \
             CAST(CAST('2026-10-18 03:30:00' AS TIMESTAMP) AS STRING) AS timestamp_string, \
             CAST(CAST(CAST('2026-10-18 03:30:00' AS TIMESTAMP) AS TIMESTAMP_NTZ) AS STRING) AS ntz_string \
             FROM orders",
        )
        .unwrap()
        .collect_as_json_rows()
        .unwrap();
    assert_eq!(rows[0]["same_date"], true);
    assert_eq!(rows[0]["ntz_round_trip"], true);
    assert_eq!(rows[0]["timestamp_string"], "2026-10-18 03:30:00.000000");
    assert_eq!(rows[0]["ntz_string"], "2026-10-18 03:30:00.000000");
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

#[test]
fn string_timestamp_column_compares_using_session_timezone() {
    let mut session = SparkSession::default();
    session.set_config("spark.sql.session.timeZone", "America/New_York");
    let frame = DataFrame::new(
        1,
        vec![
            Series::new("order_id".into(), [1i64]).into(),
            Series::new("event_ts".into(), ["2026-10-18 03:30:00"]).into(),
        ],
    )
    .unwrap();
    session.create_or_replace_temp_view("orders", session.create_dataframe_from_polars(frame));

    let rows = session
        .sql("SELECT order_id FROM orders WHERE event_ts = TIMESTAMP '2026-10-18 07:30:00Z'")
        .unwrap()
        .collect_as_json_rows()
        .unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn timestamp_literal_accepts_compact_utc_offset_suffix() {
    let session = session_with_timestamp("2026-10-17 22:00:00", "UTC");
    let rows = session
        .sql("SELECT TIMESTAMP '2026-10-18 03:30:00.123456UTC+0530' AS ts FROM orders")
        .unwrap()
        .collect_as_json_rows()
        .unwrap();
    assert_eq!(rows[0]["ts"], "2026-10-17T22:00:00.123456");
}

#[test]
fn date_midnight_gap_resolves_to_first_valid_local_instant() {
    // Toronto's 1919 spring transition began at 23:30 on the previous date and
    // ended at 00:30, so the start of March 31 was 00:30 rather than 01:00.
    let session = session_with_timestamp("1919-03-31 04:30:00", "America/Toronto");
    let rows = session
        .sql("SELECT order_id FROM orders WHERE event_ts = DATE '1919-03-31'")
        .unwrap()
        .collect_as_json_rows()
        .unwrap();
    assert_eq!(rows.len(), 1);
}

//! Datetime expression builders.

use super::imports::*;

/// Extract year from datetime column (PySpark year)
pub fn year(column: &Column) -> Column {
    column.clone().year()
}

/// Extract month from datetime column (PySpark month)
pub fn month(column: &Column) -> Column {
    column.clone().month()
}

/// Extract day of month from datetime column (PySpark day)
pub fn day(column: &Column) -> Column {
    column.clone().day()
}

/// Cast or parse to date (PySpark to_date). When format is None: cast date/datetime to date, parse string with default formats. When format is Some: parse string with given format.
pub fn to_date(column: &Column, format: Option<&str>) -> Result<Column, String> {
    let fmt = format.map(|s| s.to_string());
    let out_name = match format {
        None => format!("to_date({})", column.name()),
        Some(f) => format!("to_date({}, {f})", column.name()),
    };
    let out_name2 = out_name.clone();
    let expr = column.expr().clone().map(
        move |col| {
            crate::column::expect_col(crate::udfs::apply_string_to_date_format(
                col,
                fmt.as_deref(),
                true, // strict type checking: reject non-string/non-timestamp/non-date inputs (#1115)
            ))
        },
        move |_schema, _field| Ok(Field::new(out_name2.clone().into(), DataType::Date)),
    );
    Ok(Column::from_expr(expr.alias(&out_name), Some(out_name)))
}

/// Format date/datetime as string (PySpark date_format). Accepts PySpark/Java SimpleDateFormat style (e.g. "yyyy-MM") and converts to chrono strftime internally.
pub fn date_format(column: &Column, format: &str) -> Column {
    use polars::prelude::*;
    let out_name = format!("date_format({}, {format})", column.name());
    let fmt = format.to_string();
    let expr = column
        .expr()
        .clone()
        .map(
            move |s| crate::column::expect_col(crate::udfs::apply_date_format(s, &fmt)),
            |_schema, field| Ok(Field::new(field.name().clone(), DataType::String)),
        )
        .alias(&out_name);
    crate::column::Column::from_expr(expr, Some(out_name))
}

/// Current date (evaluation time). PySpark current_date.
pub fn current_date() -> Column {
    use polars::prelude::*;
    let today = chrono::Utc::now().date_naive();
    let days = (today - robin_sparkless_core::date_utils::epoch_naive_date()).num_days() as i32;
    crate::column::Column::from_expr(
        Expr::Literal(LiteralValue::Scalar(Scalar::new_date(days))),
        None,
    )
}

/// Current timestamp (evaluation time). PySpark current_timestamp.
pub fn current_timestamp() -> Column {
    use polars::prelude::*;
    let ts = chrono::Utc::now().timestamp_micros();
    crate::column::Column::from_expr(
        Expr::Literal(LiteralValue::Scalar(Scalar::new_datetime(
            ts,
            TimeUnit::Microseconds,
            None,
        ))),
        None,
    )
}

/// Alias for current_date (PySpark curdate).
pub fn curdate() -> Column {
    current_date()
}

/// Alias for current_timestamp (PySpark now).
pub fn now() -> Column {
    current_timestamp()
}

/// Alias for current_timestamp (PySpark localtimestamp).
pub fn localtimestamp() -> Column {
    current_timestamp()
}

/// Alias for datediff (PySpark date_diff). date_diff(end, start).
pub fn date_diff(end: &Column, start: &Column) -> Column {
    datediff(end, start)
}

/// Alias for date_add (PySpark dateadd).
pub fn dateadd(column: &Column, n: i32) -> Column {
    date_add(column, n)
}

/// Extract field from date/datetime (PySpark extract). field: year, month, day, hour, minute, second, quarter, week, dayofweek, dayofyear.
pub fn extract(column: &Column, field: &str) -> Column {
    column.clone().extract(field)
}

/// Alias for extract (PySpark date_part).
pub fn date_part(column: &Column, field: &str) -> Column {
    extract(column, field)
}

/// Alias for extract (PySpark datepart).
pub fn datepart(column: &Column, field: &str) -> Column {
    extract(column, field)
}

/// Timestamp to microseconds since epoch (PySpark unix_micros).
pub fn unix_micros(column: &Column) -> Column {
    column.clone().unix_micros()
}

/// Timestamp to milliseconds since epoch (PySpark unix_millis).
pub fn unix_millis(column: &Column) -> Column {
    column.clone().unix_millis()
}

/// Timestamp to seconds since epoch (PySpark unix_seconds).
pub fn unix_seconds(column: &Column) -> Column {
    column.clone().unix_seconds()
}

/// Weekday name "Mon","Tue",... (PySpark dayname).
pub fn dayname(column: &Column) -> Column {
    column.clone().dayname()
}

/// Weekday 0=Mon, 6=Sun (PySpark weekday).
pub fn weekday(column: &Column) -> Column {
    column.clone().weekday()
}

/// Extract hour from datetime column (PySpark hour).
pub fn hour(column: &Column) -> Column {
    column.clone().hour()
}

/// Extract minute from datetime column (PySpark minute).
pub fn minute(column: &Column) -> Column {
    column.clone().minute()
}

/// Extract second from datetime column (PySpark second).
pub fn second(column: &Column) -> Column {
    column.clone().second()
}

/// Add n days to date column (PySpark date_add).
pub fn date_add(column: &Column, n: i32) -> Column {
    column.clone().date_add(n)
}

/// Subtract n days from date column (PySpark date_sub).
pub fn date_sub(column: &Column, n: i32) -> Column {
    column.clone().date_sub(n)
}

/// Number of days between two date columns (PySpark datediff).
pub fn datediff(end: &Column, start: &Column) -> Column {
    start.clone().datediff(end)
}

/// Last day of month for date column (PySpark last_day).
pub fn last_day(column: &Column) -> Column {
    column.clone().last_day()
}

/// Truncate date/datetime to unit (PySpark trunc).
pub fn trunc(column: &Column, format: &str) -> Column {
    column.clone().trunc(format)
}

/// Alias for trunc (PySpark date_trunc).
pub fn date_trunc(format: &str, column: &Column) -> Column {
    trunc(column, format)
}

/// Extract quarter (1-4) from date/datetime (PySpark quarter).
pub fn quarter(column: &Column) -> Column {
    column.clone().quarter()
}

/// Extract ISO week of year (1-53) (PySpark weekofyear).
pub fn weekofyear(column: &Column) -> Column {
    column.clone().weekofyear()
}

/// Extract day of week: 1=Sunday..7=Saturday (PySpark dayofweek).
pub fn dayofweek(column: &Column) -> Column {
    column.clone().dayofweek()
}

/// Extract day of year (1-366) (PySpark dayofyear).
pub fn dayofyear(column: &Column) -> Column {
    column.clone().dayofyear()
}

/// Add n months to date column (PySpark add_months).
pub fn add_months(column: &Column, n: i32) -> Column {
    column.clone().add_months(n)
}

/// Months between end and start dates as fractional (PySpark months_between).
/// When round_off is true, rounds to 8 decimal places (PySpark default).
pub fn months_between(end: &Column, start: &Column, round_off: bool) -> Column {
    end.clone().months_between(start, round_off)
}

/// Next date that is the given weekday (e.g. "Mon") (PySpark next_day).
pub fn next_day(column: &Column, day_of_week: &str) -> Column {
    column.clone().next_day(day_of_week)
}

/// Current Unix timestamp in seconds (PySpark unix_timestamp with no args).
pub fn unix_timestamp_now() -> Column {
    use polars::prelude::*;
    let secs = chrono::Utc::now().timestamp();
    crate::column::Column::from_expr(lit(secs), None)
}

/// Parse string timestamp to seconds since epoch (PySpark unix_timestamp). format defaults to yyyy-MM-dd HH:mm:ss.
pub fn unix_timestamp(column: &Column, format: Option<&str>) -> Column {
    column.clone().unix_timestamp(format)
}

/// Alias for unix_timestamp.
pub fn to_unix_timestamp(column: &Column, format: Option<&str>) -> Column {
    unix_timestamp(column, format)
}

/// Convert seconds since epoch to formatted string (PySpark from_unixtime).
pub fn from_unixtime(column: &Column, format: Option<&str>) -> Column {
    column.clone().from_unixtime(format)
}

/// Build date from year, month, day columns (PySpark make_date).
pub fn make_date(year: &Column, month: &Column, day: &Column) -> Column {
    use polars::prelude::*;
    let args = [month.expr().clone(), day.expr().clone()];
    let expr = year.expr().clone().map_many(
        |cols| crate::column::expect_col(crate::udfs::apply_make_date(cols)),
        &args,
        |_schema, fields| Ok(Field::new(fields[0].name().clone(), DataType::Date)),
    );
    crate::column::Column::from_expr(expr, None)
}

/// make_timestamp(year, month, day, hour, min, sec, timezone?) - six columns to timestamp (PySpark make_timestamp).
/// When timezone is Some(tz), components are interpreted as local time in that zone, then converted to UTC.
pub fn make_timestamp(
    year: &Column,
    month: &Column,
    day: &Column,
    hour: &Column,
    minute: &Column,
    sec: &Column,
    timezone: Option<&str>,
) -> Column {
    use polars::prelude::*;
    let tz_owned = timezone.map(|s| s.to_string());
    let args = [
        month.expr().clone(),
        day.expr().clone(),
        hour.expr().clone(),
        minute.expr().clone(),
        sec.expr().clone(),
    ];
    let expr = year.expr().clone().map_many(
        move |cols| {
            crate::column::expect_col(crate::udfs::apply_make_timestamp(cols, tz_owned.as_deref()))
        },
        &args,
        |_schema, fields| {
            Ok(Field::new(
                fields[0].name().clone(),
                DataType::Datetime(TimeUnit::Microseconds, None),
            ))
        },
    );
    crate::column::Column::from_expr(expr, None)
}

/// Add amount of unit to timestamp (PySpark timestampadd).
pub fn timestampadd(unit: &str, amount: &Column, ts: &Column) -> Column {
    ts.clone().timestampadd(unit, amount)
}

/// Difference between timestamps in unit (PySpark timestampdiff).
pub fn timestampdiff(unit: &str, start: &Column, end: &Column) -> Column {
    start.clone().timestampdiff(unit, end)
}

/// Interval of n days (PySpark days). For use in date_add, timestampadd, etc.
pub fn days(n: i64) -> Column {
    make_interval(0, 0, 0, n, 0, 0, 0)
}

/// Interval of n hours (PySpark hours).
pub fn hours(n: i64) -> Column {
    make_interval(0, 0, 0, 0, n, 0, 0)
}

/// Interval of n minutes (PySpark minutes).
pub fn minutes(n: i64) -> Column {
    make_interval(0, 0, 0, 0, 0, n, 0)
}

/// Interval of n months (PySpark months). Approximated as 30*n days.
pub fn months(n: i64) -> Column {
    make_interval(0, n, 0, 0, 0, 0, 0)
}

/// Interval of n years (PySpark years). Approximated as 365*n days.
pub fn years(n: i64) -> Column {
    make_interval(n, 0, 0, 0, 0, 0, 0)
}

/// Interpret timestamp as UTC, convert to tz (PySpark from_utc_timestamp).
pub fn from_utc_timestamp(column: &Column, tz: &str) -> Column {
    column.clone().from_utc_timestamp(tz)
}

/// Interpret timestamp as in tz, convert to UTC (PySpark to_utc_timestamp).
pub fn to_utc_timestamp(column: &Column, tz: &str) -> Column {
    column.clone().to_utc_timestamp(tz)
}

/// Convert timestamp between timezones (PySpark convert_timezone).
pub fn convert_timezone(source_tz: &str, target_tz: &str, column: &Column) -> Column {
    let source_tz = source_tz.to_string();
    let target_tz = target_tz.to_string();
    let expr = column.expr().clone().map(
        move |s| {
            crate::column::expect_col(crate::udfs::apply_convert_timezone(
                s, &source_tz, &target_tz,
            ))
        },
        |_schema, field| Ok(field.clone()),
    );
    crate::column::Column::from_expr(expr, None)
}

/// Current session timezone (PySpark current_timezone). Default "UTC". Returns literal column.
pub fn current_timezone() -> Column {
    use polars::prelude::*;
    crate::column::Column::from_expr(lit("UTC"), None)
}

/// Create interval duration (PySpark make_interval). Optional args; 0 for omitted.
pub fn make_interval(
    years: i64,
    months: i64,
    weeks: i64,
    days: i64,
    hours: i64,
    mins: i64,
    secs: i64,
) -> Column {
    use polars::prelude::*;
    // Approximate: 1 year = 365 days, 1 month = 30 days
    let total_days = years * 365 + months * 30 + weeks * 7 + days;
    let args = DurationArgs::new()
        .with_days(lit(total_days))
        .with_hours(lit(hours))
        .with_minutes(lit(mins))
        .with_seconds(lit(secs));
    let dur = duration(args);
    crate::column::Column::from_expr(dur, None)
}

/// Day-time interval: days, hours, minutes, seconds (PySpark make_dt_interval). All optional; 0 for omitted.
pub fn make_dt_interval(days: i64, hours: i64, minutes: i64, seconds: i64) -> Column {
    use polars::prelude::*;
    let args = DurationArgs::new()
        .with_days(lit(days))
        .with_hours(lit(hours))
        .with_minutes(lit(minutes))
        .with_seconds(lit(seconds));
    let dur = duration(args);
    crate::column::Column::from_expr(dur, None)
}

/// Year-month interval (PySpark make_ym_interval). Polars has no native YM type; return months as Int32 (years*12 + months).
pub fn make_ym_interval(years: i32, months: i32) -> Column {
    use polars::prelude::*;
    let total_months = years * 12 + months;
    crate::column::Column::from_expr(lit(total_months), None)
}

/// Alias for make_timestamp (PySpark make_timestamp_ntz - no timezone).
pub fn make_timestamp_ntz(
    year: &Column,
    month: &Column,
    day: &Column,
    hour: &Column,
    minute: &Column,
    sec: &Column,
) -> Column {
    let timestamp = make_timestamp(year, month, day, hour, minute, sec, None);
    crate::cast(&timestamp, "timestamp_ntz")
        .expect("make_timestamp_ntz casts its timestamp result to TIMESTAMP_NTZ")
}

/// Convert seconds since epoch to timestamp (PySpark timestamp_seconds).
pub fn timestamp_seconds(column: &Column) -> Column {
    column.clone().timestamp_seconds()
}

/// Convert milliseconds since epoch to timestamp (PySpark timestamp_millis).
pub fn timestamp_millis(column: &Column) -> Column {
    column.clone().timestamp_millis()
}

/// Convert microseconds since epoch to timestamp (PySpark timestamp_micros).
pub fn timestamp_micros(column: &Column) -> Column {
    column.clone().timestamp_micros()
}

/// Date to days since 1970-01-01 (PySpark unix_date).
pub fn unix_date(column: &Column) -> Column {
    column.clone().unix_date()
}

/// Days since epoch to date (PySpark date_from_unix_date).
pub fn date_from_unix_date(column: &Column) -> Column {
    column.clone().date_from_unix_date()
}

/// Positive modulus (PySpark pmod).
pub fn pmod(dividend: &Column, divisor: &Column) -> Column {
    dividend.clone().pmod(divisor)
}

/// Factorial n! (PySpark factorial). n in 0..=20; null for negative or overflow.
pub fn factorial(column: &Column) -> Column {
    column.clone().factorial()
}

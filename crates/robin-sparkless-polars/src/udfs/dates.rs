//! Dates UDF implementations.

use super::*;

// --- Datetime: add_months, months_between, next_day ---

pub(super) fn date_series_to_days(series: &Series) -> PolarsResult<Int32Chunked> {
    let casted = series.cast(&DataType::Date)?;
    let days_series = casted.cast(&DataType::Int32)?;
    days_series
        .i32()
        .map_err(|e| compute_err("date_series_to_days", e))
        .cloned()
}

pub(super) fn days_to_naive_date(days: i32) -> Option<chrono::NaiveDate> {
    let base = robin_sparkless_core::date_utils::epoch_naive_date();
    base.checked_add_signed(chrono::TimeDelta::days(days as i64))
}

pub(super) fn naivedate_to_days(d: chrono::NaiveDate) -> i32 {
    let base = robin_sparkless_core::date_utils::epoch_naive_date();
    (d.signed_duration_since(base).num_days()) as i32
}

/// add_months(date_column, n) - add n months to each date.
pub fn apply_add_months(column: Column, n: i32) -> PolarsResult<Option<Column>> {
    use chrono::Months;
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = date_series_to_days(&series)?;
    let u = n.unsigned_abs();
    let out = ca.into_iter().map(|opt_d| {
        opt_d.and_then(|days| {
            let d = days_to_naive_date(days)?;
            let next = if n >= 0 {
                d.checked_add_months(Months::new(u))?
            } else {
                d.checked_sub_months(Months::new(u))?
            };
            Some(naivedate_to_days(next))
        })
    });
    let out = Int32Chunked::from_iter_options(name.as_str().into(), out);
    let out_series = out.into_series().cast(&DataType::Date)?;
    Ok(Some(Column::new(name, out_series)))
}

/// Add a per-row integer number of months to a date column.
pub fn apply_add_months_dynamic(columns: &mut [Column]) -> PolarsResult<Option<Column>> {
    if columns.len() < 2 {
        return Err(PolarsError::ComputeError(
            "add_months needs a date column and month-count column".into(),
        ));
    }
    use chrono::Months;
    let name = columns[0].field().into_owned().name;
    let dates = std::mem::take(&mut columns[0]).take_materialized_series();
    let counts = std::mem::take(&mut columns[1])
        .take_materialized_series()
        .cast(&DataType::Int64)?;
    let days = date_series_to_days(&dates)?;
    let counts = counts.i64().map_err(|e| compute_err("add_months", e))?;
    let out_len =
        if (days.is_empty() && counts.len() <= 1) || (counts.is_empty() && days.len() <= 1) {
            0
        } else {
            days.len().max(counts.len())
        };
    if (days.len() != 1 && days.len() != out_len) || (counts.len() != 1 && counts.len() != out_len)
    {
        return Err(PolarsError::ShapeMismatch(
            "add_months: inputs must have equal lengths or be scalars".into(),
        ));
    }
    let count_at = |idx: usize| counts.get(if counts.len() == 1 { 0 } else { idx });
    let day_at = |idx: usize| days.get(if days.len() == 1 { 0 } else { idx });
    let out = (0..out_len).map(|idx| {
        let opt_days = day_at(idx);
        opt_days.and_then(|day| {
            let months = count_at(idx)?;
            let date = days_to_naive_date(day)?;
            let next = if months >= 0 {
                date.checked_add_months(Months::new(u32::try_from(months).ok()?))?
            } else {
                date.checked_sub_months(Months::new(u32::try_from(months.unsigned_abs()).ok()?))?
            };
            Some(naivedate_to_days(next))
        })
    });
    let out = Int32Chunked::from_iter_options(name.as_str().into(), out)
        .into_series()
        .cast(&DataType::Date)?;
    Ok(Some(Column::new(name, out)))
}

/// next_day(date_column, "Mon") - next occurrence of weekday (Sun=1..Sat=7; "Mon","Tue",...).
fn parse_day_of_week(s: &str) -> Option<u32> {
    let s = s.trim().to_lowercase();
    match s.as_str() {
        "sun" | "sunday" => Some(1),
        "mon" | "monday" => Some(2),
        "tue" | "tuesday" => Some(3),
        "wed" | "wednesday" => Some(4),
        "thu" | "thursday" => Some(5),
        "fri" | "friday" => Some(6),
        "sat" | "saturday" => Some(7),
        _ => None,
    }
}

/// PySpark next_day: 1=Sunday, 2=Monday, ... 7=Saturday. chrono Weekday: Mon=0, ..., Sun=6.
fn chrono_weekday_to_dayofweek(w: chrono::Weekday) -> u32 {
    match w {
        chrono::Weekday::Mon => 2,
        chrono::Weekday::Tue => 3,
        chrono::Weekday::Wed => 4,
        chrono::Weekday::Thu => 5,
        chrono::Weekday::Fri => 6,
        chrono::Weekday::Sat => 7,
        chrono::Weekday::Sun => 1,
    }
}

pub fn apply_next_day(column: Column, day_of_week: &str) -> PolarsResult<Option<Column>> {
    let target = parse_day_of_week(day_of_week).ok_or_else(|| {
        PolarsError::ComputeError(format!("next_day: invalid day '{day_of_week}'").into())
    })?;
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = date_series_to_days(&series)?;
    let out = ca.into_iter().map(|opt_d| {
        opt_d.and_then(|days| {
            let d = days_to_naive_date(days)?;
            let current = chrono_weekday_to_dayofweek(d.weekday());
            let diff = if target >= current {
                (target - current) as i64
            } else {
                (7 - (current - target)) as i64
            };
            let days_to_add = if diff == 0 { 7 } else { diff }; // same day -> next week
            let next = d.checked_add_signed(chrono::TimeDelta::days(days_to_add))?;
            Some(naivedate_to_days(next))
        })
    });
    let out = Int32Chunked::from_iter_options(name.as_str().into(), out);
    let out_series = out.into_series().cast(&DataType::Date)?;
    Ok(Some(Column::new(name, out_series)))
}

/// dayname(date_col) - weekday name "Mon","Tue",... (PySpark dayname).
pub fn apply_dayname(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = date_series_to_days(&series)?;
    let out = StringChunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter().map(|opt_d| {
            opt_d.and_then(|days| {
                let d = days_to_naive_date(days)?;
                Some(d.weekday().to_string())
            })
        }),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// weekday(date_col) - 0=Mon, 6=Sun (PySpark weekday).
pub fn apply_weekday(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = date_series_to_days(&series)?;
    let out = Int32Chunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter().map(|opt_d| {
            opt_d.and_then(|days| {
                let d = days_to_naive_date(days)?;
                Some(d.weekday().num_days_from_monday() as i32)
            })
        }),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// months_between(end, start, round_off) - returns fractional number of months.
/// PySpark formula (issue #1481): (year1-year2)*12 + (month1-month2) + (day1-day2)/31.
/// Not total_days/31. When round_off is true, rounds to 8 decimal places (PySpark default).
pub fn apply_months_between(
    columns: &mut [Column],
    round_off: bool,
) -> PolarsResult<Option<Column>> {
    if columns.len() < 2 {
        return Err(PolarsError::ComputeError(
            "months_between needs two columns (end, start)".into(),
        ));
    }
    let name = columns[0].field().into_owned().name;
    let end_series = std::mem::take(&mut columns[0]).take_materialized_series();
    let start_series = std::mem::take(&mut columns[1]).take_materialized_series();
    let end_ca = date_series_to_days(&end_series)?;
    let start_ca = date_series_to_days(&start_series)?;
    let out = end_ca
        .into_iter()
        .zip(&start_ca)
        .map(|(oe, os)| match (oe, os) {
            (Some(e), Some(s)) => {
                let end_d = days_to_naive_date(e)?;
                let start_d = days_to_naive_date(s)?;
                let month_diff = (end_d.year() - start_d.year()) * 12
                    + (end_d.month() as i32 - start_d.month() as i32);
                let day_diff = end_d.day() as i32 - start_d.day() as i32;
                let months = month_diff as f64 + (day_diff as f64 / 31.0);
                Some(if round_off {
                    (months * 1e8).round() / 1e8
                } else {
                    months
                })
            }
            _ => None,
        });
    let out = Float64Chunked::from_iter_options(name.as_str().into(), out);
    Ok(Some(Column::new(name, out.into_series())))
}

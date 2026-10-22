//! Timestamp parsing shared by SQL literals and cast expressions.

use polars::prelude::PolarsError;

pub(crate) fn parse_sql_date(value: &str) -> Option<chrono::NaiveDate> {
    let value = value.trim();
    let (unsigned, sign) = value
        .strip_prefix('-')
        .map_or((value, 1), |unsigned| (unsigned, -1));
    let (year, rest) = unsigned.split_once('-')?;
    let year = year.parse::<i32>().ok()?.checked_mul(sign)?;
    let mut date_parts = rest.split('-');
    let month = match date_parts.next() {
        Some(month) => month.parse::<u32>().ok()?,
        None => 1,
    };
    let day = match date_parts.next() {
        Some(day) => day.parse::<u32>().ok()?,
        None => 1,
    };
    if date_parts.next().is_some() {
        return None;
    }
    chrono::NaiveDate::from_ymd_opt(year, month, day)
}

fn parse_sql_date_time(value: &str) -> Option<chrono::NaiveDateTime> {
    let value = value.trim();
    let separator = value
        .char_indices()
        .find(|(_, ch)| *ch == 'T' || ch.is_ascii_whitespace());
    let (date_part, time_part) = separator.map_or((value, None), |(index, ch)| {
        (&value[..index], Some(value[index + ch.len_utf8()..].trim()))
    });
    let date = parse_sql_date(date_part)?;

    let Some(time_part) = time_part else {
        return date.and_hms_opt(0, 0, 0);
    };
    if time_part.is_empty() {
        return date.and_hms_opt(0, 0, 0);
    }
    let mut time_parts = time_part.split(':');
    let hour = time_parts.next()?.parse::<u32>().ok()?;
    let minute = match time_parts.next() {
        Some(minute) => minute.parse::<u32>().ok()?,
        None => 0,
    };
    let (second, micros) = match time_parts.next() {
        Some(seconds) => {
            let (whole, fraction) = seconds.split_once('.').unwrap_or((seconds, ""));
            let second = whole.parse::<u32>().ok()?;
            let fraction = if fraction.is_empty() {
                0
            } else {
                if !fraction.bytes().all(|byte| byte.is_ascii_digit()) {
                    return None;
                }
                let digits = fraction.chars().take(6).collect::<String>();
                let parsed = digits.parse::<u32>().ok()?;
                parsed * 10_u32.pow((6 - digits.len()) as u32)
            };
            (second, fraction)
        }
        None => (0, 0),
    };
    if time_parts.next().is_some() {
        return None;
    }
    date.and_hms_micro_opt(hour, minute, second, micros)
}

pub(crate) fn parse_sql_fixed_offset(value: &str) -> Option<chrono::FixedOffset> {
    let offset = ["UTC", "GMT", "UT"]
        .iter()
        .find_map(|prefix| {
            value
                .strip_prefix(prefix)
                .filter(|rest| rest.starts_with('+') || rest.starts_with('-'))
        })
        .unwrap_or(value);
    if value == "Z" {
        return chrono::FixedOffset::east_opt(0);
    }
    let (sign, magnitude) = match offset.as_bytes().first()? {
        b'+' => (1, &offset[1..]),
        b'-' => (-1, &offset[1..]),
        _ => return None,
    };
    if !magnitude.is_ascii()
        || !magnitude
            .bytes()
            .all(|byte| byte.is_ascii_digit() || byte == b':')
    {
        return None;
    }
    let compact = !magnitude.contains(':') && magnitude.len() > 2;
    let compact_parts = if compact {
        match magnitude.len() {
            4 => Some((&magnitude[..2], Some(&magnitude[2..]), None)),
            6 => Some((
                &magnitude[..2],
                Some(&magnitude[2..4]),
                Some(&magnitude[4..]),
            )),
            _ => None,
        }
    } else {
        None
    };
    let parts = if let Some((hours, minutes, seconds)) = compact_parts {
        vec![hours, minutes.unwrap_or("0"), seconds.unwrap_or("0")]
    } else {
        magnitude.split(':').collect::<Vec<_>>()
    };
    if parts.is_empty() || parts.len() > 3 {
        return None;
    }
    let hours = parts[0].parse::<i32>().ok()?;
    let minutes = parts
        .get(1)
        .map(|s| s.parse::<i32>())
        .transpose()
        .ok()?
        .unwrap_or(0);
    let seconds = parts
        .get(2)
        .map(|s| s.parse::<i32>())
        .transpose()
        .ok()?
        .unwrap_or(0);
    if hours > 18 || minutes > 59 || seconds > 59 || (hours == 18 && (minutes != 0 || seconds != 0))
    {
        return None;
    }
    chrono::FixedOffset::east_opt(sign * (hours * 3600 + minutes * 60 + seconds))
}

pub(crate) fn parse_sql_timestamp_micros(
    value: &str,
    session_timezone: &str,
) -> Result<i64, PolarsError> {
    let value = value.trim();
    if let Ok(datetime) = chrono::DateTime::parse_from_rfc3339(value) {
        return Ok(datetime.timestamp_micros());
    }
    // Spark permits an explicit region or UTC offset after the timestamp body,
    // including after fractional seconds (for example `...00.123456America/New_York`).
    // Parse that suffix before parsing the fractional part so it cannot be silently
    // truncated as excess fractional digits.
    let (local_value, explicit_timezone) = split_sql_timestamp_timezone(value);
    let naive = parse_sql_date_time(local_value).ok_or_else(|| {
        PolarsError::InvalidOperation(format!("SQL: invalid TIMESTAMP literal {value:?}").into())
    })?;

    use chrono::TimeZone;
    let timezone_name = explicit_timezone.unwrap_or(session_timezone);
    if let Ok(timezone) = timezone_name.parse::<chrono_tz::Tz>() {
        return resolve_local_timestamp(timezone, &naive).ok_or_else(|| {
            PolarsError::InvalidOperation(
                format!("SQL: TIMESTAMP literal {value:?} cannot be resolved in timezone {timezone_name:?}").into(),
            )
        });
    }
    if let Some(offset) = parse_sql_fixed_offset(timezone_name) {
        return Ok(offset
            .from_local_datetime(&naive)
            .single()
            .unwrap()
            .timestamp_micros());
    }
    Err(PolarsError::InvalidOperation(
        format!("SQL: invalid timezone {timezone_name:?} in TIMESTAMP literal").into(),
    ))
}

pub(crate) fn resolve_local_timestamp(
    timezone: chrono_tz::Tz,
    naive: &chrono::NaiveDateTime,
) -> Option<i64> {
    use chrono::{Offset, TimeZone};

    let instant = |local: &chrono::NaiveDateTime| match timezone.from_local_datetime(local) {
        chrono::LocalResult::Single(value) => Some(value.timestamp_micros()),
        chrono::LocalResult::Ambiguous(first, second) => {
            Some(first.timestamp_micros().min(second.timestamp_micros()))
        }
        chrono::LocalResult::None => None,
    };
    if let Some(value) = instant(naive) {
        return Some(value);
    }

    // Spark resolves a local time inside a daylight saving gap by shifting it
    // forward by the gap size (02:30 becomes 03:30 for a one-hour gap).
    let before = naive.checked_sub_signed(chrono::TimeDelta::days(2))?;
    let after = naive.checked_add_signed(chrono::TimeDelta::days(2))?;
    let offset_before = timezone
        .from_local_datetime(&before)
        .earliest()?
        .offset()
        .fix()
        .local_minus_utc();
    let offset_after = timezone
        .from_local_datetime(&after)
        .earliest()?
        .offset()
        .fix()
        .local_minus_utc();
    let gap_seconds = offset_after - offset_before;
    if gap_seconds <= 0 {
        return None;
    }
    let shifted = naive.checked_add_signed(chrono::TimeDelta::seconds(i64::from(gap_seconds)))?;
    instant(&shifted)
}

fn split_sql_timestamp_timezone(value: &str) -> (&str, Option<&str>) {
    // Region IDs and Spark's UTC/GMT/UT-prefixed offsets are adjacent to the
    // timestamp body in SQL literals. Find the first suffix candidate whose
    // prefix is itself a complete local timestamp.
    for (index, ch) in value.char_indices() {
        let prefix = &value[..index];
        let has_time = prefix.contains(['T', ' ']);
        if index == 0
            || !(ch.is_ascii_alphabetic() || ch == '/' || (has_time && matches!(ch, '+' | '-')))
        {
            continue;
        }
        let suffix = &value[index..];
        let valid =
            suffix.parse::<chrono_tz::Tz>().is_ok() || parse_sql_fixed_offset(suffix).is_some();
        if valid && parse_sql_date_time(prefix).is_some() {
            return (prefix, Some(suffix));
        }
    }
    (value, None)
}

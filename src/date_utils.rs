//! Shared date helpers (e.g. Unix epoch).

use chrono::NaiveDate;

/// Unix epoch (1970-01-01) as NaiveDate. Used for date/timestamp conversions.
#[inline]
pub(crate) fn epoch_naive_date() -> NaiveDate {
    NaiveDate::from_ymd_opt(1970, 1, 1).expect("1970-01-01 is a valid date")
}

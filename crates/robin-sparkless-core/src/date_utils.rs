//! Shared date helpers (e.g. Unix epoch).

use chrono::NaiveDate;

/// Unix epoch (1970-01-01) as NaiveDate. Used for date/timestamp conversions.
#[inline]
pub fn epoch_naive_date() -> NaiveDate {
    NaiveDate::from_ymd_opt(1970, 1, 1).expect("1970-01-01 is a valid date")
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Datelike;

    #[test]
    fn epoch_naive_date_is_1970_01_01() {
        let d = epoch_naive_date();
        assert_eq!(d.year(), 1970);
        assert_eq!(d.month(), 1);
        assert_eq!(d.day(), 1);
    }
}

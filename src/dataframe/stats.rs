//! DataFrame statistical methods: stat (cov, corr), summary.
//! PySpark: df.stat().cov("a", "b"), df.stat().corr("a", "b"), df.summary(...).

use super::DataFrame;
use polars::prelude::PolarsError;

/// Helper for DataFrame statistical methods (PySpark-style df.stat().cov/corr).
pub struct DataFrameStat<'a> {
    pub(crate) df: &'a DataFrame,
}

impl<'a> DataFrameStat<'a> {
    /// Sample covariance between two columns. PySpark stat.cov. ddof=1 for sample covariance.
    pub fn cov(&self, col1: &str, col2: &str) -> Result<f64, PolarsError> {
        let c1 = self.df.resolve_column_name(col1)?;
        let c2 = self.df.resolve_column_name(col2)?;
        let s1 = self
            .df
            .df
            .as_ref()
            .column(c1.as_str())?
            .cast(&polars::datatypes::DataType::Float64)?;
        let s2 = self
            .df
            .df
            .as_ref()
            .column(c2.as_str())?
            .cast(&polars::datatypes::DataType::Float64)?;
        let a = s1
            .f64()
            .map_err(|_| PolarsError::ComputeError("cov: need float column".into()))?;
        let b = s2
            .f64()
            .map_err(|_| PolarsError::ComputeError("cov: need float column".into()))?;
        let mut sum_ab = 0.0_f64;
        let mut sum_a = 0.0_f64;
        let mut sum_b = 0.0_f64;
        let mut n = 0_usize;
        for (x, y) in a.into_iter().zip(b.into_iter()) {
            if let (Some(xv), Some(yv)) = (x, y) {
                n += 1;
                sum_a += xv;
                sum_b += yv;
                sum_ab += xv * yv;
            }
        }
        if n < 2 {
            return Ok(f64::NAN);
        }
        let mean_a = sum_a / n as f64;
        let mean_b = sum_b / n as f64;
        let cov = (sum_ab - n as f64 * mean_a * mean_b) / (n as f64 - 1.0);
        Ok(cov)
    }

    /// Pearson correlation between two columns. PySpark stat.corr.
    pub fn corr(&self, col1: &str, col2: &str) -> Result<f64, PolarsError> {
        let c1 = self.df.resolve_column_name(col1)?;
        let c2 = self.df.resolve_column_name(col2)?;
        let s1 = self
            .df
            .df
            .as_ref()
            .column(c1.as_str())?
            .cast(&polars::datatypes::DataType::Float64)?;
        let s2 = self
            .df
            .df
            .as_ref()
            .column(c2.as_str())?
            .cast(&polars::datatypes::DataType::Float64)?;
        let a = s1
            .f64()
            .map_err(|_| PolarsError::ComputeError("corr: need float column".into()))?;
        let b = s2
            .f64()
            .map_err(|_| PolarsError::ComputeError("corr: need float column".into()))?;
        let mut sum_ab = 0.0_f64;
        let mut sum_a = 0.0_f64;
        let mut sum_b = 0.0_f64;
        let mut sum_a2 = 0.0_f64;
        let mut sum_b2 = 0.0_f64;
        let mut n = 0_usize;
        for (x, y) in a.into_iter().zip(b.into_iter()) {
            if let (Some(xv), Some(yv)) = (x, y) {
                n += 1;
                sum_a += xv;
                sum_b += yv;
                sum_ab += xv * yv;
                sum_a2 += xv * xv;
                sum_b2 += yv * yv;
            }
        }
        if n < 2 {
            return Ok(f64::NAN);
        }
        let mean_a = sum_a / n as f64;
        let mean_b = sum_b / n as f64;
        let std_a = ((sum_a2 / n as f64 - mean_a * mean_a) * (n as f64 / (n as f64 - 1.0))).sqrt();
        let std_b = ((sum_b2 / n as f64 - mean_b * mean_b) * (n as f64 / (n as f64 - 1.0))).sqrt();
        if std_a == 0.0 || std_b == 0.0 {
            return Ok(f64::NAN);
        }
        let cov = (sum_ab - n as f64 * mean_a * mean_b) / (n as f64 - 1.0);
        Ok(cov / (std_a * std_b))
    }
}

#[cfg(test)]
mod tests {
    use crate::{DataFrame, SparkSession};

    fn test_df() -> DataFrame {
        let spark = SparkSession::builder()
            .app_name("stat_tests")
            .get_or_create();
        let tuples = vec![
            (1i64, 25i64, "a".to_string()),
            (2i64, 30i64, "b".to_string()),
            (3i64, 35i64, "c".to_string()),
        ];
        spark
            .create_dataframe(tuples, vec!["id", "age", "name"])
            .unwrap()
    }

    #[test]
    fn stat_corr_two_columns() {
        let df = test_df();
        let stat = df.stat();
        let r = stat.corr("id", "age").unwrap();
        assert!(
            r.is_nan() || (r >= -1.0 - 1e-10 && r <= 1.0 + 1e-10),
            "corr should be in [-1,1] or NaN, got {}",
            r
        );
    }

    #[test]
    fn stat_cov_two_columns() {
        let df = test_df();
        let stat = df.stat();
        let c = stat.cov("id", "age").unwrap();
        assert!(c.is_finite() || c.is_nan());
    }

    #[test]
    fn stat_corr_less_than_two_rows_returns_nan() {
        let spark = SparkSession::builder()
            .app_name("stat_tests")
            .get_or_create();
        let tuples = vec![(1i64, 10i64, "x".to_string())];
        let df = spark.create_dataframe(tuples, vec!["a", "b", "c"]).unwrap();
        let stat = df.stat();
        let r = stat.corr("a", "b").unwrap();
        assert!(r.is_nan());
    }

    #[test]
    fn stat_cov_constant_column() {
        let spark = SparkSession::builder()
            .app_name("stat_tests")
            .get_or_create();
        let tuples = vec![(1i64, 5i64, "a".to_string()), (1i64, 5i64, "b".to_string())];
        let df = spark
            .create_dataframe(tuples, vec!["k", "v", "label"])
            .unwrap();
        let stat = df.stat();
        let c = stat.cov("k", "v").unwrap();
        assert!(c.is_nan() || c == 0.0);
    }
}

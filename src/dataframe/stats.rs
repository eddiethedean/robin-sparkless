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
        let s1 = self.df.df.as_ref().column(c1.as_str())?.cast(&polars::datatypes::DataType::Float64)?;
        let s2 = self.df.df.as_ref().column(c2.as_str())?.cast(&polars::datatypes::DataType::Float64)?;
        let a = s1.f64().map_err(|_| PolarsError::ComputeError("cov: need float column".into()))?;
        let b = s2.f64().map_err(|_| PolarsError::ComputeError("cov: need float column".into()))?;
        let mut sum_ab = 0.0_f64;
        let mut sum_a = 0.0_f64;
        let mut sum_b = 0.0_f64;
        let mut n = 0_usize;
        for (x, y) in a.into_iter().zip(b.into_iter()) {
            if let (Some(xv), Some(yv)) = (x, y) {
                let xv = xv;
                let yv = yv;
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
        let s1 = self.df.df.as_ref().column(c1.as_str())?.cast(&polars::datatypes::DataType::Float64)?;
        let s2 = self.df.df.as_ref().column(c2.as_str())?.cast(&polars::datatypes::DataType::Float64)?;
        let a = s1.f64().map_err(|_| PolarsError::ComputeError("corr: need float column".into()))?;
        let b = s2.f64().map_err(|_| PolarsError::ComputeError("corr: need float column".into()))?;
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

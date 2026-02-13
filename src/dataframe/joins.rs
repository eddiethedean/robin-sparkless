//! Join operations for DataFrame.

use super::DataFrame;
use crate::type_coercion::find_common_type;
use polars::prelude::Expr;
use polars::prelude::JoinType as PlJoinType;
use polars::prelude::PolarsError;

/// Join type for DataFrame joins (PySpark-compatible)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Outer,
    /// Rows from left that have a match in right; only left columns (PySpark left_semi).
    LeftSemi,
    /// Rows from left that have no match in right; only left columns (PySpark left_anti).
    LeftAnti,
}

/// Join with another DataFrame on the given columns. Preserves case_sensitive on result.
/// When join key types differ (e.g. str vs int), coerces both sides to a common type (PySpark parity #274).
/// For Right and Outer, reorders columns to match PySpark: key(s), then left non-key, then right non-key.
pub fn join(
    left: &DataFrame,
    right: &DataFrame,
    on: Vec<&str>,
    how: JoinType,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    use polars::prelude::{col, IntoLazy, JoinBuilder, JoinCoalesce};
    let mut left_lf = left.df.as_ref().clone().lazy();
    let mut right_lf = right.df.as_ref().clone().lazy();

    // Coerce join keys to a common type when left/right dtypes differ (PySpark #274).
    let mut left_casts: Vec<Expr> = Vec::new();
    let mut right_casts: Vec<Expr> = Vec::new();
    for key in &on {
        let left_dtype = left.get_column_dtype(key).ok_or_else(|| {
            PolarsError::ComputeError(format!("join key '{key}' not found on left").into())
        })?;
        let right_dtype = right.get_column_dtype(key).ok_or_else(|| {
            PolarsError::ComputeError(format!("join key '{key}' not found on right").into())
        })?;
        if left_dtype != right_dtype {
            let common = find_common_type(&left_dtype, &right_dtype)?;
            left_casts.push(col(*key).cast(common.clone()).alias(*key));
            right_casts.push(col(*key).cast(common).alias(*key));
        }
    }
    if !left_casts.is_empty() {
        left_lf = left_lf.with_columns(left_casts);
        right_lf = right_lf.with_columns(right_casts);
    }

    let on_set: std::collections::HashSet<&str> = on.iter().copied().collect();
    let on_exprs: Vec<polars::prelude::Expr> = on.iter().map(|name| col(*name)).collect();
    let polars_how: PlJoinType = match how {
        JoinType::Inner => PlJoinType::Inner,
        JoinType::Left => PlJoinType::Left,
        JoinType::Right => PlJoinType::Right,
        JoinType::Outer => PlJoinType::Full, // PySpark Outer = Polars Full
        JoinType::LeftSemi => PlJoinType::Semi,
        JoinType::LeftAnti => PlJoinType::Anti,
    };
    let joined = JoinBuilder::new(left_lf)
        .with(right_lf)
        .how(polars_how)
        .on(&on_exprs)
        .coalesce(JoinCoalesce::CoalesceColumns)
        .finish();
    let mut pl_df = joined.collect()?;
    if matches!(how, JoinType::Right | JoinType::Outer) {
        let left_names: Vec<String> = left
            .df
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();
        let right_names: Vec<String> = right
            .df
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();
        let result_names: std::collections::HashSet<String> = pl_df
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();
        let mut order: Vec<String> = Vec::new();
        for k in &on {
            order.push((*k).to_string());
        }
        for n in &left_names {
            if !on_set.contains(n.as_str()) {
                order.push(n.clone());
            }
        }
        for n in &right_names {
            let use_name = if left_names.iter().any(|l| l == n) {
                format!("{n}_right")
            } else {
                n.clone()
            };
            if result_names.contains(&use_name) {
                order.push(use_name);
            }
        }
        if order.len() == result_names.len() {
            let select_refs: Vec<&str> = order.iter().map(String::as_str).collect();
            pl_df = pl_df.select(select_refs).map_err(|e| {
                PolarsError::ComputeError(format!("join column reorder: {e}").into())
            })?;
        }
    }
    Ok(super::DataFrame::from_polars_with_options(
        pl_df,
        case_sensitive,
    ))
}

#[cfg(test)]
mod tests {
    use super::{join, JoinType};
    use crate::{DataFrame, SparkSession};

    fn left_df() -> DataFrame {
        let spark = SparkSession::builder()
            .app_name("join_tests")
            .get_or_create();
        spark
            .create_dataframe(
                vec![
                    (1i64, 10i64, "a".to_string()),
                    (2i64, 20i64, "b".to_string()),
                ],
                vec!["id", "v", "label"],
            )
            .unwrap()
    }

    fn right_df() -> DataFrame {
        let spark = SparkSession::builder()
            .app_name("join_tests")
            .get_or_create();
        spark
            .create_dataframe(
                vec![
                    (1i64, 100i64, "x".to_string()),
                    (3i64, 300i64, "z".to_string()),
                ],
                vec!["id", "w", "tag"],
            )
            .unwrap()
    }

    #[test]
    fn inner_join() {
        let left = left_df();
        let right = right_df();
        let out = join(&left, &right, vec!["id"], JoinType::Inner, false).unwrap();
        assert_eq!(out.count().unwrap(), 1);
        let cols = out.columns().unwrap();
        assert!(cols.iter().any(|c| c == "id" || c.ends_with("_right")));
    }

    #[test]
    fn left_join() {
        let left = left_df();
        let right = right_df();
        let out = join(&left, &right, vec!["id"], JoinType::Left, false).unwrap();
        assert_eq!(out.count().unwrap(), 2);
    }

    #[test]
    fn outer_join() {
        let left = left_df();
        let right = right_df();
        let out = join(&left, &right, vec!["id"], JoinType::Outer, false).unwrap();
        assert_eq!(out.count().unwrap(), 3);
    }

    #[test]
    fn join_empty_right() {
        let spark = SparkSession::builder()
            .app_name("join_tests")
            .get_or_create();
        let left = left_df();
        let right = spark
            .create_dataframe(vec![] as Vec<(i64, i64, String)>, vec!["id", "w", "tag"])
            .unwrap();
        let out = join(&left, &right, vec!["id"], JoinType::Inner, false).unwrap();
        assert_eq!(out.count().unwrap(), 0);
    }

    /// Join when key types differ (str on left, int on right): coerces to common type (#274).
    #[test]
    fn join_key_type_coercion_str_int() {
        use polars::prelude::df;
        let spark = SparkSession::builder()
            .app_name("join_tests")
            .get_or_create();
        let left_pl = df!("id" => &["1"], "label" => &["a"]).unwrap();
        let right_pl = df!("id" => &[1i64], "x" => &[10i64]).unwrap();
        let left = spark.create_dataframe_from_polars(left_pl);
        let right = spark.create_dataframe_from_polars(right_pl);
        let out = join(&left, &right, vec!["id"], JoinType::Inner, false).unwrap();
        assert_eq!(out.count().unwrap(), 1);
        let rows = out.collect().unwrap();
        assert_eq!(rows.height(), 1);
        // Join key was coerced to common type (string); row matched id "1" with id 1.
        assert!(rows.column("label").is_ok());
        assert!(rows.column("x").is_ok());
    }
}

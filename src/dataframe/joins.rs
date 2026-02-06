//! Join operations for DataFrame.

use super::DataFrame;
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
pub fn join(
    left: &DataFrame,
    right: &DataFrame,
    on: Vec<&str>,
    how: JoinType,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    use polars::prelude::{col, IntoLazy, JoinBuilder, JoinCoalesce};
    let left_lf = left.df.as_ref().clone().lazy();
    let right_lf = right.df.as_ref().clone().lazy();
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
        .coalesce(JoinCoalesce::KeepColumns)
        .finish();
    let pl_df = joined.collect()?;
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
}

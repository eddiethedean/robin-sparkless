//! Join operations for DataFrame.

use super::DataFrame;
use crate::type_coercion::coerce_expr_pair;
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
/// When both tables have the same join key column name(s), renames the right's keys to temp names and
/// uses left_on/right_on so Polars does not error with "duplicate column" (issue #580, PySpark parity).
/// When left/right key names differ in casing (e.g. "id" vs "ID"), aliases right keys to left names
/// so the result has one key column name and col("ID")/col("id") both resolve (PySpark parity #604).
/// For Right and Outer, reorders columns to match PySpark: key(s), then left non-key, then right non-key.
pub fn join(
    left: &DataFrame,
    right: &DataFrame,
    on: Vec<&str>,
    how: JoinType,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    use polars::prelude::{JoinBuilder, JoinCoalesce, col};
    let mut left_lf = left.lazy_frame();
    let mut right_lf = right.lazy_frame();

    // Resolve key names on both sides so we can alias right keys to left names (#604).
    let left_key_names: Vec<String> = on
        .iter()
        .map(|k| {
            left.resolve_column_name(k).map_err(|e| {
                PolarsError::ComputeError(format!("join key '{k}' on left: {e}").into())
            })
        })
        .collect::<Result<_, _>>()?;
    let right_key_names: Vec<String> = on
        .iter()
        .map(|k| {
            right.resolve_column_name(k).map_err(|e| {
                PolarsError::ComputeError(format!("join key '{k}' on right: {e}").into())
            })
        })
        .collect::<Result<_, _>>()?;

    // Coerce join keys to a common type when left/right dtypes differ (PySpark #274).
    // Alias right keys to left key names so result has one key column name (#604).
    let mut left_casts: Vec<Expr> = Vec::new();
    let mut right_casts: Vec<Expr> = Vec::new();
    for (i, key) in on.iter().enumerate() {
        let left_name = &left_key_names[i];
        let right_name = &right_key_names[i];
        let left_dtype = left.get_column_dtype(left_name.as_str()).ok_or_else(|| {
            PolarsError::ComputeError(format!("join key '{key}' not found on left").into())
        })?;
        let right_dtype = right.get_column_dtype(right_name.as_str()).ok_or_else(|| {
            PolarsError::ComputeError(format!("join key '{key}' not found on right").into())
        })?;
        let target_name = left_name.as_str();
        if left_dtype != right_dtype {
            let (l, r) = coerce_expr_pair(
                left_name.as_str(),
                right_name.as_str(),
                &left_dtype,
                &right_dtype,
                target_name,
            )?;
            left_casts.push(l);
            right_casts.push(r);
        } else if left_name != right_name {
            right_casts.push(col(right_name.as_str()).alias(target_name));
        }
    }
    if !left_casts.is_empty() {
        left_lf = left_lf.with_columns(left_casts);
    }
    if !right_casts.is_empty() {
        right_lf = right_lf.with_columns(right_casts);
        // #614: Drop right's original key columns when we aliased to left names, so the result
        // has only the left key name (e.g. "id") and collect does not fail with "not found: ID".
        let drop_right: std::collections::HashSet<String> = on
            .iter()
            .enumerate()
            .filter(|(i, _)| left_key_names[*i] != right_key_names[*i])
            .map(|(i, _)| right_key_names[i].clone())
            .collect();
        if !drop_right.is_empty() {
            let right_names = right.columns()?;
            let mut keep_names: Vec<&str> = right_names
                .iter()
                .filter(|n| !drop_right.contains(*n))
                .map(String::as_str)
                .collect();
            for (i, name) in left_key_names.iter().enumerate() {
                if left_key_names[i] != right_key_names[i] {
                    keep_names.push(name.as_str());
                }
            }
            let keep: Vec<Expr> = keep_names.iter().map(|s| col(*s)).collect();
            right_lf = right_lf.select(&keep);
        }
    }

    let on_set: std::collections::HashSet<String> = left_key_names.iter().cloned().collect();
    let on_exprs: Vec<polars::prelude::Expr> = left_key_names
        .iter()
        .map(|name| col(name.as_str()))
        .collect();
    let polars_how: PlJoinType = match how {
        JoinType::Inner => PlJoinType::Inner,
        JoinType::Left => PlJoinType::Left,
        JoinType::Right => PlJoinType::Right,
        JoinType::Outer => PlJoinType::Full, // PySpark Outer = Polars Full
        JoinType::LeftSemi => PlJoinType::Semi,
        JoinType::LeftAnti => PlJoinType::Anti,
    };

    let mut joined = JoinBuilder::new(left_lf)
        .with(right_lf)
        .how(polars_how)
        .on(&on_exprs)
        .coalesce(JoinCoalesce::CoalesceColumns)
        .finish();
    // For Right/Outer, reorder columns: keys, left non-keys, right non-keys (PySpark order).
    let result_lf = if matches!(how, JoinType::Right | JoinType::Outer) {
        let left_names = left.columns()?;
        let right_names = right.columns()?;
        let result_schema = joined.collect_schema()?;
        let result_names: std::collections::HashSet<String> =
            result_schema.iter_names().map(|s| s.to_string()).collect();
        let mut order: Vec<String> = Vec::new();
        for k in &left_key_names {
            order.push(k.clone());
        }
        for n in &left_names {
            if !on_set.contains(n) {
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
            let select_exprs: Vec<polars::prelude::Expr> =
                order.iter().map(|s| col(s.as_str())).collect();
            joined.select(select_exprs.as_slice())
        } else {
            joined
        }
    } else {
        joined
    };
    Ok(super::DataFrame::from_lazy_with_options(
        result_lf,
        case_sensitive,
    ))
}

#[cfg(test)]
mod tests {
    use super::{JoinType, join};
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
    fn right_join() {
        let left = left_df();
        let right = right_df();
        let out = join(&left, &right, vec!["id"], JoinType::Right, false).unwrap();
        assert_eq!(out.count().unwrap(), 2); // right has id 1,3; left matches 1
    }

    #[test]
    fn outer_join() {
        let left = left_df();
        let right = right_df();
        let out = join(&left, &right, vec!["id"], JoinType::Outer, false).unwrap();
        assert_eq!(out.count().unwrap(), 3);
    }

    #[test]
    fn left_semi_join() {
        let left = left_df();
        let right = right_df();
        let out = join(&left, &right, vec!["id"], JoinType::LeftSemi, false).unwrap();
        assert_eq!(out.count().unwrap(), 1); // left rows with match in right (id 1)
    }

    #[test]
    fn left_anti_join() {
        let left = left_df();
        let right = right_df();
        let out = join(&left, &right, vec!["id"], JoinType::LeftAnti, false).unwrap();
        assert_eq!(out.count().unwrap(), 1); // left rows with no match (id 2)
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

    /// Issue #604: join when key names differ in case (left "id", right "ID"); collect must not fail with "not found: ID".
    #[test]
    fn join_column_resolution_case_insensitive() {
        use polars::prelude::df;
        let spark = SparkSession::builder()
            .app_name("join_tests")
            .get_or_create();
        let left_pl = df!("id" => &[1i64, 2i64], "val" => &["a", "b"]).unwrap();
        let right_pl = df!("ID" => &[1i64], "other" => &["x"]).unwrap();
        let left = spark.create_dataframe_from_polars(left_pl);
        let right = spark.create_dataframe_from_polars(right_pl);
        let out = join(&left, &right, vec!["id"], JoinType::Inner, false)
            .expect("issue #604: join on id/ID must succeed");
        assert_eq!(out.count().unwrap(), 1);
        let rows = out
            .collect()
            .expect("issue #604: collect must not fail with 'not found: ID'");
        assert_eq!(rows.height(), 1);
        assert!(rows.column("id").is_ok());
        assert!(rows.column("val").is_ok());
        assert!(rows.column("other").is_ok());
        // Resolving "ID" (case-insensitive) must work.
        assert!(out.resolve_column_name("ID").is_ok());
    }
}

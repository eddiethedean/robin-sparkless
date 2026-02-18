//! Join operations for DataFrame.

use super::DataFrame;
use crate::type_coercion::find_common_type;
use polars::prelude::Expr;
use polars::prelude::IntoLazy;
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
/// For Right and Outer, reorders columns to match PySpark: key(s), then left non-key, then right non-key.
pub fn join(
    left: &DataFrame,
    right: &DataFrame,
    on: Vec<&str>,
    how: JoinType,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    use polars::prelude::{col, JoinBuilder, JoinCoalesce};
    let mut left_lf = left.lazy_frame();
    let mut right_lf = right.lazy_frame();

    // Resolve right-side key column names (case-sensitive resolution).
    let right_key_names: Vec<String> = on
        .iter()
        .map(|key| {
            right.resolve_column_name(key).map_err(|_| {
                PolarsError::ComputeError(format!("join key '{key}' not found on right").into())
            })
        })
        .collect::<Result<Vec<_>, PolarsError>>()?;

    // When both tables have the same key names, rename right's keys to avoid Polars "duplicate column" (#580).
    let right_join_key_temps: Vec<String> = (0..on.len())
        .map(|i| format!("__right_join_key_{i}"))
        .collect();
    let right_has_same_key_names = on
        .iter()
        .zip(right_key_names.iter())
        .any(|(l, r)| *l == r.as_str());
    if right_has_same_key_names {
        right_lf = right_lf.rename(
            right_key_names
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>(),
            right_join_key_temps.clone(),
            true,
        );
    }

    // Coerce join keys to a common type when left/right dtypes differ (PySpark #274).
    let mut left_casts: Vec<Expr> = Vec::new();
    let mut right_casts: Vec<Expr> = Vec::new();
    for (i, key) in on.iter().enumerate() {
        let left_dtype = left.get_column_dtype(key).ok_or_else(|| {
            PolarsError::ComputeError(format!("join key '{key}' not found on left").into())
        })?;
        let right_dtype = right.get_column_dtype(key).ok_or_else(|| {
            PolarsError::ComputeError(format!("join key '{key}' not found on right").into())
        })?;
        if left_dtype != right_dtype {
            let common = find_common_type(&left_dtype, &right_dtype)?;
            left_casts.push(col(*key).cast(common.clone()).alias(*key));
            let right_key = if right_has_same_key_names {
                right_join_key_temps[i].as_str()
            } else {
                right_key_names[i].as_str()
            };
            right_casts.push(col(right_key).cast(common).alias(right_key));
        }
    }
    if !left_casts.is_empty() {
        left_lf = left_lf.with_columns(left_casts);
        right_lf = right_lf.with_columns(right_casts);
    }

    let on_set: std::collections::HashSet<&str> = on.iter().copied().collect();
    let polars_how: PlJoinType = match how {
        JoinType::Inner => PlJoinType::Inner,
        JoinType::Left => PlJoinType::Left,
        JoinType::Right => PlJoinType::Right,
        JoinType::Outer => PlJoinType::Full, // PySpark Outer = Polars Full
        JoinType::LeftSemi => PlJoinType::Semi,
        JoinType::LeftAnti => PlJoinType::Anti,
    };

    let left_on_exprs: Vec<polars::prelude::Expr> = on.iter().map(|name| col(*name)).collect();
    let right_on_exprs: Vec<polars::prelude::Expr> = if right_has_same_key_names {
        right_join_key_temps
            .iter()
            .map(|s| col(s.as_str()))
            .collect()
    } else {
        right_key_names.iter().map(|s| col(s.as_str())).collect()
    };

    let mut joined = if right_has_same_key_names {
        JoinBuilder::new(left_lf)
            .with(right_lf)
            .how(polars_how)
            .left_on(left_on_exprs)
            .right_on(right_on_exprs)
            .coalesce(JoinCoalesce::CoalesceColumns)
            .finish()
    } else {
        JoinBuilder::new(left_lf)
            .with(right_lf)
            .how(polars_how)
            .on(&left_on_exprs)
            .coalesce(JoinCoalesce::CoalesceColumns)
            .finish()
    };

    // When we renamed right keys, result may have __right_join_key_* (e.g. Right join); alias them back to key names.
    // For Right/Outer, lazy collect_schema() can report the left key name while execution outputs __right_join_key_*,
    // so we use the executed schema and return an eager DataFrame to avoid schema/plan mismatch.
    if right_has_same_key_names && matches!(how, JoinType::Right | JoinType::Outer) {
        let pl_df = joined.clone().collect()?;
        let schema = pl_df.schema();
        let has_temp = schema
            .iter_names()
            .any(|n| n.to_string().starts_with("__right_join_key_"));
        if has_temp {
            let exprs: Vec<polars::prelude::Expr> = schema
                .iter_names()
                .map(|name| {
                    let s = name.to_string();
                    for (i, key) in on.iter().enumerate() {
                        if s == format!("__right_join_key_{i}") {
                            return col(s.as_str()).alias(*key);
                        }
                    }
                    col(s.as_str())
                })
                .collect();
            let fixed = pl_df.lazy().select(exprs.as_slice()).collect()?;
            // Reorder to PySpark order: keys, left non-keys, right non-keys.
            let left_names = left.columns()?;
            let right_names = right.columns()?;
            let fixed_names_set: std::collections::HashSet<String> = fixed
                .get_column_names()
                .iter()
                .map(|s| s.to_string())
                .collect();
            let mut order: Vec<String> = Vec::new();
            for k in on {
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
                if fixed_names_set.contains(&use_name) {
                    order.push(use_name);
                }
            }
            let fixed_names: Vec<String> = fixed
                .get_column_names()
                .iter()
                .map(|s| s.to_string())
                .collect();
            if order.len() == fixed_names.len()
                && order.iter().all(|o| fixed_names.iter().any(|f| f == o))
            {
                let reordered = fixed.select(order.iter().map(|s| s.as_str()))?;
                return Ok(super::DataFrame::from_polars_with_options(
                    reordered,
                    case_sensitive,
                ));
            }
            return Ok(super::DataFrame::from_polars_with_options(
                fixed,
                case_sensitive,
            ));
        }
    }

    // When we renamed right keys (non-Right/Outer), alias temp key columns if present in lazy schema.
    if right_has_same_key_names {
        let result_schema = joined.collect_schema()?;
        let has_temp_keys = result_schema
            .iter_names()
            .any(|n| n.to_string().starts_with("__right_join_key_"));
        if has_temp_keys {
            let exprs: Vec<polars::prelude::Expr> = result_schema
                .iter_names()
                .map(|name| {
                    let s = name.to_string();
                    for (i, key) in on.iter().enumerate() {
                        if s == format!("__right_join_key_{i}") {
                            return col(s.as_str()).alias(*key);
                        }
                    }
                    col(s.as_str())
                })
                .collect();
            joined = joined.select(exprs.as_slice());
        }
    }

    // For Right/Outer, reorder columns: keys, left non-keys, right non-keys (PySpark order).
    let result_lf = if matches!(how, JoinType::Right | JoinType::Outer) && !right_has_same_key_names
    {
        let left_names = left.columns()?;
        let right_names = right.columns()?;
        let result_schema = joined.collect_schema()?;
        let result_names: std::collections::HashSet<String> =
            result_schema.iter_names().map(|s| s.to_string()).collect();
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

    /// Issue #580: join with column expression when both tables have same key name (e.g. dept_id) must not fail with "duplicate column".
    #[test]
    fn join_same_key_name_both_tables() {
        use polars::prelude::df;
        let spark = SparkSession::builder()
            .app_name("join_tests")
            .get_or_create();
        let emp = df![
            "id" => [1i64, 2i64, 3i64, 4i64],
            "name" => ["Alice", "Bob", "Charlie", "David"],
            "dept_id" => [10i64, 20i64, 10i64, 30i64],
            "salary" => [50000i64, 60000i64, 70000i64, 55000i64],
        ]
        .unwrap();
        let dept = df![
            "dept_id" => [10i64, 20i64, 40i64],
            "name" => ["IT", "HR", "Finance"],
            "location" => ["NYC", "LA", "Chicago"],
        ]
        .unwrap();
        let left = spark.create_dataframe_from_polars(emp);
        let right = spark.create_dataframe_from_polars(dept);
        let out = join(&left, &right, vec!["dept_id"], JoinType::Inner, false).unwrap();
        assert_eq!(
            out.count().unwrap(),
            3,
            "Alice, Bob, Charlie match dept 10, 20"
        );
        let cols = out.columns().unwrap();
        assert!(
            cols.iter().any(|c| c == "dept_id"),
            "one dept_id column in result"
        );
        assert!(cols.iter().any(|c| c == "location"));
    }
}

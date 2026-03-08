//! Join operations for DataFrame.

use std::collections::HashSet;

use super::DataFrame;
use crate::schema_conv::data_type_to_polars_type;
use crate::type_coercion::coerce_expr_pair_for_join;
use polars::prelude::{
    coalesce as pl_coalesce, DataType as PlDataType, Expr, JoinType as PlJoinType, Operator,
    PolarsError, SchemaNamesAndDtypes,
};
use polars_plan::dsl::functions::nth;

fn expr_to_column_name(expr: &Expr) -> Option<String> {
    use polars::prelude::Expr as PlExpr;
    let mut e = expr;
    loop {
        match e {
            PlExpr::Column(n) => return Some(n.as_str().to_string()),
            PlExpr::Alias(inner, _) | PlExpr::Cast { expr: inner, .. } => e = inner.as_ref(),
            _ => return None,
        }
    }
}

/// If `expr` contains an equality between two column refs (e.g. left.dept_id == right.dept_id),
/// returns Some((left_col_name, right_col_name)) so the caller can use key-based join.
/// Peels Alias and matches Eq or EqValidity, and also walks simple AND trees so that
/// compound conditions like (a.id == b.id) & (a.amount > 30) still yield the key pair.
/// Used for PySpark parity (#1049, #380).
pub fn try_extract_join_eq_columns(expr: &Expr) -> Option<(String, String)> {
    try_extract_join_eq_columns_all(expr).into_iter().next()
}

/// Collects all (left_col, right_col) equality pairs from an expression (e.g. AND of (a.id == b.id) & (a.x == b.x)).
/// Used so condition joins on multiple keys use a single join with all keys (#1148).
pub fn try_extract_join_eq_columns_all(expr: &Expr) -> Vec<(String, String)> {
    use polars::prelude::Expr as PlExpr;

    fn inner_extract_all(e: &Expr, out: &mut Vec<(String, String)>) {
        let mut current = e;
        while let PlExpr::Alias(inner, _) = current {
            current = inner.as_ref();
        }
        match current {
            PlExpr::BinaryExpr {
                left,
                op: Operator::Eq | Operator::EqValidity,
                right,
            } => {
                if let (Some(l), Some(r)) = (
                    expr_to_column_name(left.as_ref()),
                    expr_to_column_name(right.as_ref()),
                ) {
                    out.push((l, r));
                }
            }
            PlExpr::BinaryExpr {
                left,
                op: Operator::And,
                right,
            } => {
                inner_extract_all(left.as_ref(), out);
                inner_extract_all(right.as_ref(), out);
            }
            _ => {}
        }
    }

    let mut pairs = Vec::new();
    inner_extract_all(expr, &mut pairs);
    pairs
}

/// Returns true if the expression is only AND and Eq (column refs). When true, a key-based join
/// already enforces the condition, so we must not filter after the join (left/right/outer would
/// otherwise lose unmatched rows). Used for PySpark parity (#1242).
pub fn expr_contains_only_join_key_equalities(expr: &Expr) -> bool {
    use polars::prelude::Expr as PlExpr;
    fn only_join_equalities(e: &Expr) -> bool {
        let mut current = e;
        while let PlExpr::Alias(inner, _) = current {
            current = inner.as_ref();
        }
        match current {
            PlExpr::BinaryExpr {
                left,
                op: Operator::Eq | Operator::EqValidity,
                right,
            } => {
                expr_to_column_name(left.as_ref()).is_some()
                    && expr_to_column_name(right.as_ref()).is_some()
            }
            PlExpr::BinaryExpr {
                left,
                op: Operator::And,
                right,
            } => only_join_equalities(left.as_ref()) && only_join_equalities(right.as_ref()),
            _ => false,
        }
    }
    only_join_equalities(expr)
}

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
/// When left/right key names differ in casing or name (e.g. "id" vs "ID" or "id" vs "other_id"), aliases right keys to left names
/// so the result has one key column name (PySpark parity #604, #743).
/// For Right and Outer, reorders columns to match PySpark: key(s), then left non-key, then right non-key.
/// `left_on` and `right_on` must have the same length; keys are matched by position.
///
/// When `coalesce_same_name_keys` is true (e.g. join(right, "id") or join(right, [col("id")])), duplicate
/// key columns are coalesced into one so the result has a single key column (PySpark parity #1049, #353).
/// When false (e.g. condition join left.x == right.x), both key columns are kept (dept_id, dept_id_right).
///
/// When `mark_join_keys_ambiguous` is true and left/right key names are the same, unqualified references
/// to those key names are treated as ambiguous (PySpark parity for condition join: #1230).
pub fn join(
    left: &DataFrame,
    right: &DataFrame,
    left_on: Vec<&str>,
    right_on: Vec<&str>,
    how: JoinType,
    case_sensitive: bool,
    coalesce_same_name_keys: bool,
    mark_join_keys_ambiguous: bool,
) -> Result<DataFrame, PolarsError> {
    use polars::prelude::{JoinBuilder, JoinCoalesce, col};
    if left_on.len() != right_on.len() {
        return Err(PolarsError::ComputeError(
            "join: left_on and right_on must have the same length".into(),
        ));
    }
    let mut left_lf = left.lazy_frame();
    let mut right_lf = right.lazy_frame();
    // For full outer joins we preserve the left-side join key values in temporary columns so we
    // can use them as the canonical join key after the join (unmatched right rows get null key).
    let mut outer_left_key_copies: Vec<(String, String)> = Vec::new();

    // Resolve key names on both sides so we can alias right keys to left names (#604, #743).
    let left_key_names: Vec<String> = left_on
        .iter()
        .map(|k| {
            left.resolve_column_name(k).map_err(|e| {
                PolarsError::ComputeError(format!("join key '{k}' on left: {e}").into())
            })
        })
        .collect::<Result<_, _>>()?;
    let mut right_key_names: Vec<String> = right_on
        .iter()
        .map(|k| {
            right.resolve_column_name(k).map_err(|e| {
                PolarsError::ComputeError(format!("join key '{k}' on right: {e}").into())
            })
        })
        .collect::<Result<_, _>>()?;
    // For outer joins invoked via column-name based join (coalesce_same_name_keys = true),
    // add temp copies of left join keys so we can restore them as canonical keys after the join
    // (PySpark parity for grouping/selection on join keys when using join(on=...)).
    if matches!(how, JoinType::Outer) && coalesce_same_name_keys {
        use polars::prelude::col;
        let mut copy_exprs: Vec<Expr> = Vec::new();
        for name in &left_key_names {
            let temp = format!("__rs_outer_key_{}", name);
            outer_left_key_copies.push((name.clone(), temp.clone()));
            copy_exprs.push(col(name.as_str()).alias(temp.as_str()));
        }
        if !copy_exprs.is_empty() {
            left_lf = left_lf.with_columns(copy_exprs);
        }
    }
    // For full outer joins (via join(on=...)) where left/right use the same key names,
    // rename right keys to a suffixed form (e.g. key -> key_right) so that we preserve
    // both columns internally while building the join, but then drop the suffixed right
    // key columns from the final result so the public schema matches PySpark (single
    // join key column). Condition-based joins (on=Column) keep both key columns.
    let mut outer_same_name_keys = false;
    if matches!(how, JoinType::Outer)
        && coalesce_same_name_keys
        && left_key_names == right_key_names
    {
        use polars::prelude::col;
        use std::collections::HashMap;
        let mut rename_map: HashMap<String, String> = HashMap::new();
        for name in &right_key_names {
            rename_map.insert(name.clone(), format!("{name}_right"));
        }
        if !rename_map.is_empty() {
            let current_names: Vec<String> = right.columns()?.into_iter().collect();
            let exprs: Vec<Expr> = current_names
                .iter()
                .map(|n| {
                    if let Some(new_name) = rename_map.get(n) {
                        col(n.as_str()).alias(new_name.as_str())
                    } else {
                        col(n.as_str())
                    }
                })
                .collect();
            right_lf = right_lf.select(&exprs);
            // Update right_key_names to the new suffixed names.
            for rk in &mut right_key_names {
                if let Some(new_name) = rename_map.get(rk) {
                    *rk = new_name.clone();
                }
            }
            outer_same_name_keys = true;
        }
    }

    let keys_differ = left_key_names != right_key_names;
    // When coalesce_same_name_keys and !case_sensitive, treat keys as same if they match case-insensitively (#297).
    let keys_match_for_coalesce = !keys_differ
        || (coalesce_same_name_keys
            && !case_sensitive
            && left_key_names.len() == right_key_names.len()
            && left_key_names
                .iter()
                .zip(right_key_names.iter())
                .all(|(a, b)| a.eq_ignore_ascii_case(b)));

    if keys_match_for_coalesce {
        // #1009, #1019: When aliasing right key to left name, right may already have a column with that name (e.g. self-join).
        let right_names: Vec<String> = right.columns()?.into_iter().collect();
        let mut renames: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        for (i, _) in left_on.iter().enumerate() {
            let target_name = &left_key_names[i];
            let right_key = &right_key_names[i];
            if target_name != right_key && right_names.iter().any(|n| n == target_name) {
                renames.insert(target_name.clone(), format!("{}_right", target_name));
            }
        }
        if !renames.is_empty() {
            let exprs: Vec<Expr> = right_names
                .iter()
                .map(|n| {
                    if let Some(suffix) = renames.get(n) {
                        col(n.as_str()).alias(suffix.as_str())
                    } else {
                        col(n.as_str())
                    }
                })
                .collect();
            right_lf = right_lf.select(&exprs);
        }

        // Coerce join keys to a common type when left/right dtypes differ (PySpark #274).
        // Alias right keys to left key names so result has one key column name (#604, #743).
        let mut left_casts: Vec<Expr> = Vec::new();
        let mut right_casts: Vec<Expr> = Vec::new();
        for (i, key) in left_on.iter().enumerate() {
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
                let (l, r) = coerce_expr_pair_for_join(
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
            let drop_right: std::collections::HashSet<String> = left_on
                .iter()
                .enumerate()
                .filter(|(i, _)| left_key_names[*i] != right_key_names[*i])
                .map(|(i, _)| right_key_names[i].clone())
                .collect();
            if !drop_right.is_empty() {
                let current_right_names: Vec<String> = right_lf
                    .collect_schema()
                    .map(|s| s.iter_names().map(|n| n.to_string()).collect())?;
                let keep_names: Vec<&str> = current_right_names
                    .iter()
                    .filter(|n| !drop_right.contains(*n))
                    .map(String::as_str)
                    .collect();
                let keep: Vec<Expr> = keep_names.iter().map(|s| col(*s)).collect();
                right_lf = right_lf.select(&keep);
                // Right keys were aliased to left names; use left names for join (#297).
                right_key_names = left_key_names.clone();
            }
        }
    }

    let on_set: std::collections::HashSet<String> = left_key_names.iter().cloned().collect();
    let polars_how: PlJoinType = match how {
        JoinType::Inner => PlJoinType::Inner,
        JoinType::Left => PlJoinType::Left,
        JoinType::Right => PlJoinType::Right,
        JoinType::Outer => PlJoinType::Full, // PySpark Outer = Polars Full
        JoinType::LeftSemi => PlJoinType::Semi,
        JoinType::LeftAnti => PlJoinType::Anti,
    };

    // Build join key expressions, coercing types when needed.
    let mut left_on_exprs: Vec<Expr> = Vec::with_capacity(left_key_names.len());
    let mut right_on_exprs: Vec<Expr> = Vec::with_capacity(right_key_names.len());

    if keys_differ {
        // left_on/right_on or condition join: coerce to common type but keep distinct column names
        // so both key columns remain visible (PySpark parity #241, #1106).
        use crate::type_coercion::find_common_type_for_join;
        let right_schema = right_lf.collect_schema()?;
        for i in 0..left_key_names.len() {
            let left_name = &left_key_names[i];
            let right_name = &right_key_names[i];
            let left_dtype = left.get_column_dtype(left_name.as_str()).ok_or_else(|| {
                PolarsError::ComputeError(
                    format!("join key '{}' not found on left", left_name).into(),
                )
            })?;
            let right_dtype = right_schema
                .get(right_name.as_str())
                .cloned()
                .ok_or_else(|| {
                    PolarsError::ComputeError(
                        format!("join key '{}' not found on right", right_name).into(),
                    )
                })?;
            if left_dtype == right_dtype {
                left_on_exprs.push(col(left_name.as_str()));
                right_on_exprs.push(col(right_name.as_str()));
            } else {
                let common = find_common_type_for_join(&left_dtype, &right_dtype)?;
                left_on_exprs.push(col(left_name.as_str()).cast(common.clone()));
                right_on_exprs.push(col(right_name.as_str()).cast(common));
            }
        }
    } else {
        left_on_exprs = left_key_names.iter().map(|n| col(n.as_str())).collect();
        right_on_exprs = right_key_names.iter().map(|n| col(n.as_str())).collect();
    }

    // When same-named keys (e.g. join on "id" or left.id == right.id), coalesce so result has one
    // key column and no _right in row keys (PySpark parity #1049, #353, #1148). Use keys_match_for_coalesce
    // so case-insensitive match (e.g. name/NAME) also coalesces (#297).
    // Outer joins keep separate key columns so canonical key comes from left (issue #280).
    let coalesce = if !keys_match_for_coalesce {
        JoinCoalesce::KeepColumns
    } else if matches!(how, JoinType::Inner | JoinType::Left | JoinType::Right) {
        JoinCoalesce::CoalesceColumns
    } else if matches!(how, JoinType::Outer) {
        JoinCoalesce::KeepColumns
    } else {
        JoinCoalesce::CoalesceColumns
    };
    let mut did_coalesce_same_name_columns = false;
    let mut joined = JoinBuilder::new(left_lf)
        .with(right_lf)
        .how(polars_how)
        .left_on(&left_on_exprs)
        .right_on(&right_on_exprs)
        .coalesce(coalesce)
        .finish();

    // For full outer joins, restore canonical left join key values from the temporary copies we
    // added before the join so that grouping/selecting on the join key matches PySpark semantics
    // (unmatched right rows have null key).
    if matches!(how, JoinType::Outer) && !outer_left_key_copies.is_empty() {
        use polars::prelude::col;
        // Overwrite the public key columns with the temp copies.
        for (left_name, temp) in &outer_left_key_copies {
            joined = joined.with_column(col(temp.as_str()).alias(left_name.as_str()));
        }
        // Drop the temp columns from the result.
        let schema = joined.collect_schema()?;
        let all_names: Vec<String> = schema.iter_names().map(|n| n.to_string()).collect();
        let temp_set: std::collections::HashSet<&str> = outer_left_key_copies
            .iter()
            .map(|(_, t)| t.as_str())
            .collect();
        let keep_exprs: Vec<Expr> = all_names
            .iter()
            .filter(|n| !temp_set.contains(n.as_str()))
            .map(|n| col(n.as_str()))
            .collect();
        joined = joined.select(&keep_exprs);
    }

    let result_schema = joined.collect_schema()?;
    let mut names: Vec<String> = result_schema.iter_names().map(|s| s.to_string()).collect();
    // For outer joins with same-named keys, drop the suffixed right key columns (e.g. key_right)
    // so the public schema exposes a single join key column, matching PySpark parity fixtures.
    if matches!(how, JoinType::Outer) && coalesce_same_name_keys && outer_same_name_keys {
        let drop_set: std::collections::HashSet<&str> =
            right_key_names.iter().map(|s| s.as_str()).collect();
        let keep_exprs: Vec<Expr> = names
            .iter()
            .filter(|n| !drop_set.contains(n.as_str()))
            .map(|n| col(n.as_str()))
            .collect();
        joined = joined.select(&keep_exprs);
        let result_schema = joined.collect_schema()?;
        names = result_schema.iter_names().map(|s| s.to_string()).collect();
    }
    // When same-named keys and Inner/Left/Right, select exactly: keys (once), left non-keys,
    // Column order: left columns in original order, then right non-keys with _right for overlap
    // (PySpark parity: same as fixture join_inner_dept_issue510 / join_on_string_issue513). Use
    // keys_match_for_coalesce so case-insensitive key match (e.g. name/NAME) gets single key (#297).
    let mut any_duplicate_coalesced = false;
    if keys_match_for_coalesce && matches!(how, JoinType::Inner | JoinType::Left | JoinType::Right) {
        let left_names: Vec<String> = left.columns()?.into_iter().collect();
        let right_names: Vec<String> = right.columns()?.into_iter().collect();
        let key_set: std::collections::HashSet<&str> =
            left_key_names.iter().map(|s| s.as_str()).collect();
        let result_schema_ref = joined.collect_schema()?;
        let result_names_vec: Vec<String> = result_schema_ref
            .iter_names()
            .map(|s| s.to_string())
            .collect();
        let result_names_set: std::collections::HashSet<String> =
            result_names_vec.iter().cloned().collect();
        // When !case_sensitive, coalesce columns that match case-insensitively (e.g. age + AGE) so
        // select("age") and df1["age"] resolve to one column (#297). Only set did_coalesce_same_name_columns
        // when we actually merge duplicates (matches.len() > 1); same-case keys (id/id) alone stay ambiguous (#374).
        let cast_exprs: Vec<Expr> = if !case_sensitive {
            let left_struct = left.schema().ok();
            let right_struct = right.schema().ok();
            let mut exprs: Vec<Expr> = Vec::new();
            for left_name in &left_names {
                let matches: Vec<&String> = result_names_vec
                    .iter()
                    .filter(|r| r.eq_ignore_ascii_case(left_name))
                    .collect();
                if matches.is_empty() {
                    continue;
                }
                if matches.len() > 1 {
                    any_duplicate_coalesced = true;
                }
                let dtype = key_set
                    .contains(left_name.as_str())
                    .then(|| {
                        left_struct
                            .as_ref()
                            .and_then(|s| {
                                s.fields()
                                    .iter()
                                    .find(|f| f.name.as_str() == left_name.as_str())
                                    .map(|f| data_type_to_polars_type(&f.data_type))
                            })
                            .or_else(|| left.get_column_dtype(left_name.as_str()))
                    })
                    .flatten()
                    .or_else(|| left.get_column_dtype(left_name.as_str()));
                let parts: Vec<Expr> = matches.iter().map(|m| col(m.as_str())).collect();
                let e = if parts.len() == 1 {
                    col(matches[0].as_str())
                } else {
                    pl_coalesce(&parts)
                };
                let e = match dtype {
                    Some(dt) => e.cast(dt),
                    None => e,
                };
                exprs.push(e.alias(left_name.as_str()));
            }
            for right_name in &right_names {
                if key_set.contains(right_name.as_str()) {
                    continue;
                }
                let matches_left = left_names
                    .iter()
                    .any(|l| l.eq_ignore_ascii_case(right_name));
                if matches_left {
                    continue;
                }
                if !result_names_set.contains(right_name) {
                    continue;
                }
                let dtype = right_struct
                    .as_ref()
                    .and_then(|s| {
                        s.fields()
                            .iter()
                            .find(|f| f.name.as_str() == right_name.as_str())
                            .map(|f| data_type_to_polars_type(&f.data_type))
                    })
                    .or_else(|| right.get_column_dtype(right_name.as_str()));
                let e = match dtype {
                    Some(dt) => col(right_name.as_str()).cast(dt),
                    None => col(right_name.as_str()),
                };
                exprs.push(e.alias(right_name.as_str()));
            }
            exprs
        } else {
            let left_set: std::collections::HashSet<&str> =
                left_names.iter().map(|s| s.as_str()).collect();
            let mut desired: Vec<String> = left_names.clone();
            for n in &right_names {
                if key_set.contains(n.as_str()) {
                    continue;
                }
                let use_name = if left_set.contains(n.as_str()) {
                    format!("{n}_right")
                } else {
                    n.clone()
                };
                desired.push(use_name);
            }
            let keep: Vec<String> = desired
                .into_iter()
                .filter(|n| result_names_set.contains(n))
                .collect();
            let left_struct = left.schema().ok();
            let right_struct = right.schema().ok();
            keep.iter()
                .map(|n| {
                    let dtype = if key_set.contains(n.as_str()) {
                        left_struct
                            .as_ref()
                            .and_then(|s| {
                                s.fields()
                                    .iter()
                                    .find(|f| f.name.as_str() == n.as_str())
                                    .map(|f| data_type_to_polars_type(&f.data_type))
                            })
                            .or_else(|| left.get_column_dtype(n.as_str()))
                    } else if let Some(base) = n.strip_suffix("_right") {
                        right_struct
                            .as_ref()
                            .and_then(|s| {
                                s.fields()
                                    .iter()
                                    .find(|f| f.name.as_str() == base)
                                    .map(|f| data_type_to_polars_type(&f.data_type))
                            })
                            .or_else(|| right.get_column_dtype(base))
                    } else if left_names.iter().any(|l| l.as_str() == n.as_str()) {
                        left_struct
                            .as_ref()
                            .and_then(|s| {
                                s.fields()
                                    .iter()
                                    .find(|f| f.name.as_str() == n.as_str())
                                    .map(|f| data_type_to_polars_type(&f.data_type))
                            })
                            .or_else(|| left.get_column_dtype(n.as_str()))
                    } else {
                        right_struct
                            .as_ref()
                            .and_then(|s| {
                                s.fields()
                                    .iter()
                                    .find(|f| f.name.as_str() == n.as_str())
                                    .map(|f| data_type_to_polars_type(&f.data_type))
                            })
                            .or_else(|| right.get_column_dtype(n.as_str()))
                    };
                    match dtype {
                        Some(dt) => col(n.as_str()).cast(dt).alias(n.as_str()),
                        None => col(n.as_str()),
                    }
                })
                .collect()
        };
        if !cast_exprs.is_empty() {
            joined = joined.select(&cast_exprs);
            let result_schema = joined.collect_schema()?;
            names = result_schema.iter_names().map(|s| s.to_string()).collect();
            did_coalesce_same_name_columns = any_duplicate_coalesced;
        }
    }
    let mut seen = std::collections::HashSet::new();
    let mut unique_order: Vec<String> = Vec::new();
    for n in &names {
        if seen.insert(n.clone()) {
            unique_order.push(n.clone());
        }
    }
    if unique_order.len() < names.len() {
        // Preserve column dtypes when deduplicating by position (#1165). nth(idx) can lose
        // type in the logical schema; cast to the join result dtype so collect() returns
        // correct types (e.g. v=10, w=20 as int, not string).
        let schema_before_nth = joined.collect_schema()?;
        let dtypes_by_index: Vec<PlDataType> = schema_before_nth
            .iter_names_and_dtypes()
            .map(|(_name, dt): (_, &PlDataType)| dt.clone())
            .collect();
        let exprs: Vec<Expr> = unique_order
            .iter()
            .map(|name| {
                let idx = names.iter().position(|n| n == name).unwrap();
                let e = nth(idx as i64).as_expr();
                if let Some(dt) = dtypes_by_index.get(idx) {
                    e.cast(dt.clone()).alias(name.as_str())
                } else {
                    e.alias(name.as_str())
                }
            })
            .collect();
        joined = joined.select(&exprs);
    }
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
    // When we coalesced same-named columns, result has one column per logical name; do not mark
    // any as ambiguous so select(df1["age"]) works (#297).
    let ambiguous_columns = if did_coalesce_same_name_columns {
        None
    } else if keys_match_for_coalesce && mark_join_keys_ambiguous {
        Some(left_key_names.iter().cloned().collect::<HashSet<String>>())
    } else {
        None
    };
    Ok(super::DataFrame::from_lazy_with_options_and_ambiguous(
        result_lf,
        case_sensitive,
        ambiguous_columns,
    ))
}

#[cfg(test)]
mod tests {
    use super::{
        expr_contains_only_join_key_equalities, join, try_extract_join_eq_columns,
        try_extract_join_eq_columns_all, JoinType,
    };
    use crate::functions::col;
    use crate::{DataFrame, SparkSession};
    use std::collections::HashMap;

    #[test]
    fn extract_join_eq_columns_from_eq_expr() {
        let left = col("dept_id");
        let right = col("dept_id");
        let eq_expr = left.eq(right.into_expr());
        let expr = eq_expr.into_expr();
        let out = try_extract_join_eq_columns(&expr);
        assert_eq!(out, Some(("dept_id".to_string(), "dept_id".to_string())));
    }

    #[test]
    fn extract_join_eq_columns_all_from_and_of_equalities() {
        // (a == a) & (b == b) yields both pairs (#1148).
        let right = col("b").eq(col("b").into_expr());
        let expr = col("a").eq(col("a").into_expr()).and_(&right).into_expr();
        let out = try_extract_join_eq_columns_all(&expr);
        assert_eq!(
            out,
            vec![
                ("a".to_string(), "a".to_string()),
                ("b".to_string(), "b".to_string()),
            ]
        );
    }

    #[test]
    fn extract_join_eq_columns_from_aliased_eq() {
        let eq_expr = col("a").eq(col("b").into_expr());
        let expr = eq_expr.into_expr(); // adds Alias(..., "<expr>")
        let out = try_extract_join_eq_columns(&expr);
        assert_eq!(out, Some(("a".to_string(), "b".to_string())));
    }

    #[test]
    fn expr_contains_only_join_key_equalities_simple_and_compound() {
        // Only key equalities -> true (so we skip post-join filter for left/right/outer #1242).
        let eq_expr = col("Key").eq(col("Name").into_expr()).into_expr();
        assert!(expr_contains_only_join_key_equalities(&eq_expr));
        let and_expr = col("a")
            .eq(col("b").into_expr())
            .and_(&col("c").eq(col("d").into_expr()))
            .into_expr();
        assert!(expr_contains_only_join_key_equalities(&and_expr));
        // Compound (equality + other) -> false so we still apply filter (#380).
        let gt_expr = col("a")
            .eq(col("b").into_expr())
            .and_(&col("x").gt(col("y").into_expr()))
            .into_expr();
        assert!(!expr_contains_only_join_key_equalities(&gt_expr));
    }

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
        let out = join(
            &left,
            &right,
            vec!["id"],
            vec!["id"],
            JoinType::Inner,
            false,
            false,
            false,
        )
        .unwrap();
        assert_eq!(out.count().unwrap(), 1);
        let cols = out.columns().unwrap();
        assert!(cols.iter().any(|c| c == "id" || c.ends_with("_right")));
    }

    /// #1165: Join with same-named keys and coalesce: non-key columns keep correct dtypes in schema and collect.
    #[test]
    fn join_coalesce_preserves_non_key_column_types() {
        use robin_sparkless_core::DataType as CoreDataType;
        let left = left_df();
        let right = right_df();
        let out = join(
            &left,
            &right,
            vec!["id"],
            vec!["id"],
            JoinType::Inner,
            false,
            true, // coalesce_same_name_keys
            false,
        )
        .unwrap();
        assert_eq!(out.count().unwrap(), 1);
        let schema = out.schema().unwrap();
        let v_field = schema.fields().iter().find(|f| f.name == "v");
        let w_field = schema.fields().iter().find(|f| f.name == "w");
        assert!(
            matches!(v_field.map(|f| &f.data_type), Some(CoreDataType::Long)),
            "v should be Long"
        );
        assert!(
            matches!(w_field.map(|f| &f.data_type), Some(CoreDataType::Long)),
            "w should be Long"
        );
        let rows = out.collect_as_json_rows().unwrap();
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert!(
            row.get("v").and_then(|v| v.as_i64()).is_some(),
            "v should be number in JSON"
        );
        assert!(
            row.get("w").and_then(|v| v.as_i64()).is_some(),
            "w should be number in JSON"
        );
    }

    #[test]
    fn left_join() {
        let left = left_df();
        let right = right_df();
        let out = join(
            &left,
            &right,
            vec!["id"],
            vec!["id"],
            JoinType::Left,
            false,
            false,
            false,
        )
        .unwrap();
        assert_eq!(out.count().unwrap(), 2);
    }

    #[test]
    fn right_join() {
        let left = left_df();
        let right = right_df();
        let out = join(
            &left,
            &right,
            vec!["id"],
            vec!["id"],
            JoinType::Right,
            false,
            false,
            false,
        )
        .unwrap();
        assert_eq!(out.count().unwrap(), 2); // right has id 1,3; left matches 1
    }

    #[test]
    fn outer_join() {
        let left = left_df();
        let right = right_df();
        let out = join(
            &left,
            &right,
            vec!["id"],
            vec!["id"],
            JoinType::Outer,
            false,
            false,
            false,
        )
        .unwrap();
        assert_eq!(out.count().unwrap(), 3);
    }

    #[test]
    fn left_semi_join() {
        let left = left_df();
        let right = right_df();
        let out = join(
            &left,
            &right,
            vec!["id"],
            vec!["id"],
            JoinType::LeftSemi,
            false,
            false,
            false,
        )
        .unwrap();
        assert_eq!(out.count().unwrap(), 1); // left rows with match in right (id 1)
    }

    #[test]
    fn left_anti_join() {
        let left = left_df();
        let right = right_df();
        let out = join(
            &left,
            &right,
            vec!["id"],
            vec!["id"],
            JoinType::LeftAnti,
            false,
            false,
            false,
        )
        .unwrap();
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
        let out = join(
            &left,
            &right,
            vec!["id"],
            vec!["id"],
            JoinType::Inner,
            false,
            false,
            false,
        )
        .unwrap();
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
        let out = join(
            &left,
            &right,
            vec!["id"],
            vec!["id"],
            JoinType::Inner,
            false,
            false,
            false,
        )
        .unwrap();
        assert_eq!(out.count().unwrap(), 1);
        let rows = out.collect().unwrap();
        assert_eq!(rows.height(), 1);
        // Join key was coerced to common type (string); row matched id "1" with id 1.
        assert!(rows.column("label").is_ok());
        assert!(rows.column("x").is_ok());
    }

    /// #681: Join when key types differ (Int64 on left, String on right): coerces to common type (String).
    #[test]
    fn join_key_type_coercion_int_str() {
        use polars::prelude::df;
        let spark = SparkSession::builder()
            .app_name("join_tests")
            .get_or_create();
        let left_pl = df!("id" => &[1i64, 2i64], "name" => &["alice", "bob"]).unwrap();
        let right_pl = df!("id" => &["1", "3"], "value" => &[100i64, 300i64]).unwrap();
        let left = spark.create_dataframe_from_polars(left_pl);
        let right = spark.create_dataframe_from_polars(right_pl);
        let out = join(
            &left,
            &right,
            vec!["id"],
            vec!["id"],
            JoinType::Inner,
            false,
            false,
            false,
        )
        .unwrap();
        assert_eq!(out.count().unwrap(), 1, "inner join on id: 1 match (id=1)");
        let rows = out.collect().unwrap();
        assert_eq!(rows.height(), 1);
        assert!(rows.column("id").is_ok());
        assert!(rows.column("name").is_ok());
        assert!(rows.column("value").is_ok());
    }

    #[test]
    fn outer_join_then_groupby_on_key_matches_pyspark_semantics() {
        // Mirror tests/test_issue_280_join_groupby_ambiguity.py::test_outer_join_then_groupby:
        // left keys: 1, 3; right keys: 1, 2. Canonical join key must come from the left side
        // so grouping on "key" yields {1: 1, 3: 1, None: 1} and the unmatched right row uses
        // null for the join key (not 2) for PySpark parity.
        let spark = SparkSession::builder()
            .app_name("outer_join_groupby_tests")
            .get_or_create();

        let left_tuples = vec![
            (1i64, 0i64, "L1".to_string()),
            (3i64, 0i64, "L3".to_string()),
        ];
        let right_tuples = vec![
            (1i64, 0i64, "R1".to_string()),
            (2i64, 0i64, "R2".to_string()),
        ];

        let left = spark
            .create_dataframe(left_tuples, vec!["key", "extra_left", "left_val"])
            .unwrap();
        let right = spark
            .create_dataframe(right_tuples, vec!["key", "extra_right", "right_val"])
            .unwrap();

        let joined = join(
            &left,
            &right,
            vec!["key"],
            vec!["key"],
            JoinType::Outer,
            false,
            false,
            false,
        )
        .unwrap();

        let grouped = joined.group_by(vec!["key"]).unwrap();
        let out = grouped.count().unwrap();
        let pl_df = out.collect().unwrap();

        let key_col = pl_df.column("key").unwrap().i64().unwrap();
        let count_col = pl_df.column("count").unwrap().u32().unwrap();

        let mut by_key: HashMap<Option<i64>, u32> = HashMap::new();
        for idx in 0..key_col.len() {
            let key = key_col.get(idx);
            let cnt = count_col.get(idx).unwrap_or(0);
            by_key.insert(key, cnt);
        }

        // Expect exactly three groups: key=1, key=3, and key=None for the unmatched right row.
        assert_eq!(by_key.len(), 3);
        assert_eq!(by_key.get(&Some(1)).copied(), Some(1));
        assert_eq!(by_key.get(&Some(3)).copied(), Some(1));
        assert_eq!(by_key.get(&None).copied(), Some(1));
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
        let out = join(
            &left,
            &right,
            vec!["id"],
            vec!["id"],
            JoinType::Inner,
            false,
            false,
            false,
        )
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

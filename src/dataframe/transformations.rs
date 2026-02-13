//! DataFrame transformation operations: filter, select, with_column, order_by,
//! union, distinct, drop, dropna, fillna, limit, with_column_renamed,
//! replace, cross_join, describe, subtract, intersect,
//! sample, random_split, first, head, take, tail, is_empty, to_df.

use super::DataFrame;
use crate::functions::SortOrder;
use polars::prelude::{
    Expr, IntoLazy, IntoSeries, NamedFrom, PolarsError, Series, UnionArgs, UniqueKeepStrategy,
};
use std::collections::HashMap;

/// Select columns (returns a new DataFrame). Preserves case_sensitive on result.
pub fn select(
    df: &DataFrame,
    cols: Vec<&str>,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    let selected = df.df.select(cols)?;
    Ok(super::DataFrame::from_polars_with_options(
        selected,
        case_sensitive,
    ))
}

/// Select using column expressions (e.g. F.regexp_extract_all(...).alias("m")). Preserves case_sensitive.
/// Column names in expressions are resolved per df's case sensitivity (PySpark parity).
/// Duplicate output names are disambiguated with _1, _2, ... so select(col("num").cast("string"), col("num").cast("int")) works (issue #213).
pub fn select_with_exprs(
    df: &DataFrame,
    exprs: Vec<Expr>,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    let exprs: Vec<Expr> = exprs
        .into_iter()
        .map(|e| df.resolve_expr_column_names(e))
        .collect::<Result<Vec<_>, _>>()?;
    let mut name_count: HashMap<String, u32> = HashMap::new();
    let exprs: Vec<Expr> = exprs
        .into_iter()
        .map(|e| {
            let base_name = polars_plan::utils::expr_output_name(&e)
                .map(|s| s.to_string())
                .unwrap_or_else(|_| "_".to_string());
            let count = name_count.entry(base_name.clone()).or_insert(0);
            *count += 1;
            let final_name = if *count == 1 {
                base_name
            } else {
                format!("{}_{}", base_name, *count - 1)
            };
            if *count == 1 {
                e
            } else {
                e.alias(final_name.as_str())
            }
        })
        .collect();
    let lf = df.df.as_ref().clone().lazy();
    let out_df = lf.select(exprs).collect()?;
    Ok(super::DataFrame::from_polars_with_options(
        out_df,
        case_sensitive,
    ))
}

/// Filter rows using a Polars expression. Preserves case_sensitive on result.
/// Column names in the condition are resolved per df's case sensitivity (PySpark parity).
pub fn filter(
    df: &DataFrame,
    condition: Expr,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    let condition = df.resolve_expr_column_names(condition)?;
    let condition = df.coerce_string_numeric_comparisons(condition)?;
    let lf = df.df.as_ref().clone().lazy().filter(condition);
    let out_df = lf.collect()?;
    Ok(super::DataFrame::from_polars_with_options(
        out_df,
        case_sensitive,
    ))
}

/// Add or replace a column. Handles deferred rand/randn and Python UDF (UdfCall).
pub fn with_column(
    df: &DataFrame,
    column_name: &str,
    column: &crate::column::Column,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    // Python UDF: eager execution at UDF boundary
    #[cfg(feature = "pyo3")]
    if let Some((ref udf_name, ref args)) = column.udf_call {
        if let Some(session) = crate::session::get_thread_udf_session() {
            return crate::python::execute_python_udf(
                df,
                column_name,
                udf_name,
                args,
                case_sensitive,
                &session,
            );
        }
    }

    if let Some(deferred) = column.deferred {
        match deferred {
            crate::column::DeferredRandom::Rand(seed) => {
                let mut pl_df = df.df.as_ref().clone();
                let n = pl_df.height();
                let series = crate::udfs::series_rand_n(column_name, n, seed);
                pl_df.with_column(series)?;
                return Ok(super::DataFrame::from_polars_with_options(
                    pl_df,
                    case_sensitive,
                ));
            }
            crate::column::DeferredRandom::Randn(seed) => {
                let mut pl_df = df.df.as_ref().clone();
                let n = pl_df.height();
                let series = crate::udfs::series_randn_n(column_name, n, seed);
                pl_df.with_column(series)?;
                return Ok(super::DataFrame::from_polars_with_options(
                    pl_df,
                    case_sensitive,
                ));
            }
        }
    }
    let expr = df.resolve_expr_column_names(column.expr().clone())?;
    let expr = df.coerce_string_numeric_comparisons(expr)?;
    let lf = df.df.as_ref().clone().lazy();
    let lf_with_col = lf.with_column(expr.alias(column_name));
    let pl_df = lf_with_col.collect()?;
    Ok(super::DataFrame::from_polars_with_options(
        pl_df,
        case_sensitive,
    ))
}

/// Order by columns (sort). Preserves case_sensitive on result.
pub fn order_by(
    df: &DataFrame,
    column_names: Vec<&str>,
    ascending: Vec<bool>,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    use polars::prelude::*;
    let mut asc = ascending;
    while asc.len() < column_names.len() {
        asc.push(true);
    }
    asc.truncate(column_names.len());
    let lf = df.df.as_ref().clone().lazy();
    let exprs: Vec<Expr> = column_names.iter().map(|name| col(*name)).collect();
    let descending: Vec<bool> = asc.iter().map(|&a| !a).collect();
    let sorted = lf.sort_by_exprs(
        exprs,
        SortMultipleOptions::new().with_order_descending_multi(descending),
    );
    let pl_df = sorted.collect()?;
    Ok(super::DataFrame::from_polars_with_options(
        pl_df,
        case_sensitive,
    ))
}

/// Order by sort expressions (asc/desc with nulls_first/last). Preserves case_sensitive on result.
/// Column names in sort expressions are resolved per df's case sensitivity (PySpark parity).
pub fn order_by_exprs(
    df: &DataFrame,
    sort_orders: Vec<SortOrder>,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    use polars::prelude::*;
    if sort_orders.is_empty() {
        return Ok(super::DataFrame::from_polars_with_options(
            df.df.as_ref().clone(),
            case_sensitive,
        ));
    }
    let exprs: Vec<Expr> = sort_orders
        .iter()
        .map(|s| df.resolve_expr_column_names(s.expr().clone()))
        .collect::<Result<Vec<_>, _>>()?;
    let descending: Vec<bool> = sort_orders.iter().map(|s| s.descending).collect();
    let nulls_last: Vec<bool> = sort_orders.iter().map(|s| s.nulls_last).collect();
    let opts = SortMultipleOptions::new()
        .with_order_descending_multi(descending)
        .with_nulls_last_multi(nulls_last);
    let lf = df.df.as_ref().clone().lazy();
    let sorted = lf.sort_by_exprs(exprs, opts);
    let pl_df = sorted.collect()?;
    Ok(super::DataFrame::from_polars_with_options(
        pl_df,
        case_sensitive,
    ))
}

/// Union (unionAll): stack another DataFrame vertically. Schemas must match (same columns, same order).
pub fn union(
    left: &DataFrame,
    right: &DataFrame,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    let lf1 = left.df.as_ref().clone().lazy();
    let lf2 = right.df.as_ref().clone().lazy();
    let out = polars::prelude::concat([lf1, lf2], UnionArgs::default())?.collect()?;
    Ok(super::DataFrame::from_polars_with_options(
        out,
        case_sensitive,
    ))
}

/// Union by name: stack vertically, aligning columns by name. Right columns are reordered to match left; missing columns in right become nulls.
pub fn union_by_name(
    left: &DataFrame,
    right: &DataFrame,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    use polars::prelude::*;
    let left_names = left.df.get_column_names();
    let right_df = right.df.as_ref();
    let right_names = right_df.get_column_names();
    let resolve_right = |name: &str| -> Option<String> {
        if case_sensitive {
            right_names
                .iter()
                .find(|n| n.as_str() == name)
                .map(|s| s.as_str().to_string())
        } else {
            let name_lower = name.to_lowercase();
            right_names
                .iter()
                .find(|n| n.as_str().to_lowercase() == name_lower)
                .map(|s| s.as_str().to_string())
        }
    };
    let mut exprs: Vec<Expr> = Vec::with_capacity(left_names.len());
    for left_col in left_names.iter() {
        let left_str = left_col.as_str();
        if let Some(r) = resolve_right(left_str) {
            exprs.push(col(r.as_str()));
        } else {
            exprs.push(Expr::Literal(polars::prelude::LiteralValue::Null).alias(left_str));
        }
    }
    let right_aligned = right_df.clone().lazy().select(exprs).collect()?;
    let lf1 = left.df.as_ref().clone().lazy();
    let lf2 = right_aligned.lazy();
    let out = polars::prelude::concat([lf1, lf2], UnionArgs::default())?.collect()?;
    Ok(super::DataFrame::from_polars_with_options(
        out,
        case_sensitive,
    ))
}

/// Distinct: drop duplicate rows (all columns or subset).
pub fn distinct(
    df: &DataFrame,
    subset: Option<Vec<&str>>,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    let lf = df.df.as_ref().clone().lazy();
    let subset_names: Option<Vec<String>> =
        subset.map(|cols| cols.iter().map(|s| (*s).to_string()).collect());
    let lf = lf.unique(subset_names, UniqueKeepStrategy::First);
    let pl_df = lf.collect()?;
    Ok(super::DataFrame::from_polars_with_options(
        pl_df,
        case_sensitive,
    ))
}

/// Drop one or more columns.
pub fn drop(
    df: &DataFrame,
    columns: Vec<&str>,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    let resolved: Vec<String> = columns
        .iter()
        .map(|c| df.resolve_column_name(c))
        .collect::<Result<Vec<_>, _>>()?;
    let all_names = df.df.get_column_names();
    let to_keep: Vec<&str> = all_names
        .iter()
        .filter(|n| !resolved.iter().any(|r| r == n.as_str()))
        .map(|n| n.as_str())
        .collect();
    let pl_df = df.df.select(to_keep)?;
    Ok(super::DataFrame::from_polars_with_options(
        pl_df,
        case_sensitive,
    ))
}

/// Drop rows with nulls (all columns or subset). PySpark na.drop(subset, how, thresh).
/// - how: "any" (default) = drop if any null in subset; "all" = drop only if all null in subset.
/// - thresh: if set, keep row if it has at least this many non-null values in subset (overrides how).
pub fn dropna(
    df: &DataFrame,
    subset: Option<Vec<&str>>,
    how: &str,
    thresh: Option<usize>,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    use polars::prelude::*;
    let lf = df.df.as_ref().clone().lazy();
    let cols: Vec<&str> = match &subset {
        Some(c) => c.as_slice().to_vec(),
        None => df
            .df
            .get_column_names()
            .iter()
            .map(|n| n.as_str())
            .collect(),
    };
    let col_exprs: Vec<Expr> = cols.iter().map(|c| col(*c)).collect();
    let lf = if let Some(n) = thresh {
        // Keep row if number of non-null in subset >= n
        let count_expr: Expr = col_exprs
            .iter()
            .map(|e| e.clone().is_not_null().cast(DataType::Int32))
            .fold(lit(0i32), |a, b| a + b);
        lf.filter(count_expr.gt_eq(lit(n as i32)))
    } else if how.eq_ignore_ascii_case("all") {
        // Drop only when all subset columns are null → keep when any is not null
        let any_not_null: Expr = col_exprs
            .into_iter()
            .map(|e| e.is_not_null())
            .fold(lit(false), |a, b| a.or(b));
        lf.filter(any_not_null)
    } else {
        // how == "any" (default): drop if any null in subset
        lf.drop_nulls(Some(col_exprs))
    };
    let pl_df = lf.collect()?;
    Ok(super::DataFrame::from_polars_with_options(
        pl_df,
        case_sensitive,
    ))
}

/// Fill nulls with a literal expression. If subset is Some, only those columns are filled; else all.
/// PySpark na.fill(value, subset=...).
pub fn fillna(
    df: &DataFrame,
    value_expr: Expr,
    subset: Option<Vec<&str>>,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    use polars::prelude::*;
    let exprs: Vec<Expr> = match subset {
        Some(cols) => cols
            .iter()
            .map(|n| {
                let resolved = df.resolve_column_name(n)?;
                Ok(col(resolved.as_str()).fill_null(value_expr.clone()))
            })
            .collect::<Result<Vec<_>, PolarsError>>()?,
        None => df
            .df
            .get_column_names()
            .iter()
            .map(|n| col(n.as_str()).fill_null(value_expr.clone()))
            .collect(),
    };
    let pl_df = df
        .df
        .as_ref()
        .clone()
        .lazy()
        .with_columns(exprs)
        .collect()?;
    Ok(super::DataFrame::from_polars_with_options(
        pl_df,
        case_sensitive,
    ))
}

/// Limit: return first n rows.
pub fn limit(df: &DataFrame, n: usize, case_sensitive: bool) -> Result<DataFrame, PolarsError> {
    let pl_df = df.df.as_ref().clone().head(Some(n));
    Ok(super::DataFrame::from_polars_with_options(
        pl_df,
        case_sensitive,
    ))
}

/// Rename a column (old_name -> new_name).
pub fn with_column_renamed(
    df: &DataFrame,
    old_name: &str,
    new_name: &str,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    let resolved = df.resolve_column_name(old_name)?;
    let mut pl_df = df.df.as_ref().clone();
    pl_df.rename(resolved.as_str(), new_name.into())?;
    Ok(super::DataFrame::from_polars_with_options(
        pl_df,
        case_sensitive,
    ))
}

/// Replace values in a column: where column == old_value, use new_value. PySpark replace (single column).
pub fn replace(
    df: &DataFrame,
    column_name: &str,
    old_value: Expr,
    new_value: Expr,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    use polars::prelude::*;
    let resolved = df.resolve_column_name(column_name)?;
    let repl = when(col(resolved.as_str()).eq(old_value))
        .then(new_value)
        .otherwise(col(resolved.as_str()));
    let pl_df = df
        .df
        .as_ref()
        .clone()
        .lazy()
        .with_column(repl.alias(resolved.as_str()))
        .collect()?;
    Ok(super::DataFrame::from_polars_with_options(
        pl_df,
        case_sensitive,
    ))
}

/// Cross join: cartesian product of two DataFrames. PySpark crossJoin.
pub fn cross_join(
    left: &DataFrame,
    right: &DataFrame,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    let lf_left = left.df.as_ref().clone().lazy();
    let lf_right = right.df.as_ref().clone().lazy();
    let out = lf_left.cross_join(lf_right, None);
    let pl_df = out.collect()?;
    Ok(super::DataFrame::from_polars_with_options(
        pl_df,
        case_sensitive,
    ))
}

/// Summary statistics (count, mean, std, min, max). PySpark describe.
/// Builds a summary DataFrame with a "summary" column (PySpark name) and one column per numeric input column.
pub fn describe(df: &DataFrame, case_sensitive: bool) -> Result<DataFrame, PolarsError> {
    use polars::prelude::*;
    let pl_df = df.df.as_ref().clone();
    let mut stat_values: Vec<Column> = Vec::new();
    for col in pl_df.get_columns() {
        let s = col.as_materialized_series();
        let dtype = s.dtype();
        if dtype.is_numeric() {
            let name = s.name().clone();
            let count = s.len() as i64 - s.null_count() as i64;
            let mean_f = s.mean().unwrap_or(f64::NAN);
            let std_f = s.std(1).unwrap_or(f64::NAN);
            let s_f64 = s.cast(&DataType::Float64)?;
            let ca = s_f64
                .f64()
                .map_err(|_| PolarsError::ComputeError("cast to f64 failed".into()))?;
            let min_f = ca.min().unwrap_or(f64::NAN);
            let max_f = ca.max().unwrap_or(f64::NAN);
            // PySpark describe/summary returns string type for value columns
            let is_float = matches!(dtype, DataType::Float64 | DataType::Float32);
            let count_s = count.to_string();
            let mean_s = if mean_f.is_nan() {
                "None".to_string()
            } else {
                format!("{:.1}", mean_f)
            };
            let std_s = if std_f.is_nan() {
                "None".to_string()
            } else {
                format!("{:.1}", std_f)
            };
            let min_s = if min_f.is_nan() {
                "None".to_string()
            } else if min_f.fract() == 0.0 && is_float {
                format!("{:.1}", min_f)
            } else if min_f.fract() == 0.0 {
                format!("{:.0}", min_f)
            } else {
                format!("{min_f}")
            };
            let max_s = if max_f.is_nan() {
                "None".to_string()
            } else if max_f.fract() == 0.0 && is_float {
                format!("{:.1}", max_f)
            } else if max_f.fract() == 0.0 {
                format!("{:.0}", max_f)
            } else {
                format!("{max_f}")
            };
            let series = Series::new(
                name,
                [
                    count_s.as_str(),
                    mean_s.as_str(),
                    std_s.as_str(),
                    min_s.as_str(),
                    max_s.as_str(),
                ],
            );
            stat_values.push(series.into());
        }
    }
    if stat_values.is_empty() {
        // No numeric columns: return minimal describe with just summary column (PySpark name)
        let stat_col = Series::new(
            "summary".into(),
            &["count", "mean", "stddev", "min", "max" as &str],
        )
        .into();
        let empty: Vec<f64> = Vec::new();
        let empty_series = Series::new("placeholder".into(), empty).into();
        let out_pl = polars::prelude::DataFrame::new(vec![stat_col, empty_series])?;
        return Ok(super::DataFrame::from_polars_with_options(
            out_pl,
            case_sensitive,
        ));
    }
    let summary_col = Series::new(
        "summary".into(),
        &["count", "mean", "stddev", "min", "max" as &str],
    )
    .into();
    let mut cols: Vec<Column> = vec![summary_col];
    cols.extend(stat_values);
    let out_pl = polars::prelude::DataFrame::new(cols)?;
    Ok(super::DataFrame::from_polars_with_options(
        out_pl,
        case_sensitive,
    ))
}

/// Set difference: rows in left that are not in right (by all columns). PySpark subtract / except.
pub fn subtract(
    left: &DataFrame,
    right: &DataFrame,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    use polars::prelude::*;
    let left_names = left.df.get_column_names();
    let left_on: Vec<Expr> = left_names.iter().map(|n| col(n.as_str())).collect();
    let right_on: Vec<Expr> = left_names.iter().map(|n| col(n.as_str())).collect();
    let right_lf = right.df.as_ref().clone().lazy();
    let left_lf = left.df.as_ref().clone().lazy();
    let anti = left_lf.join(right_lf, left_on, right_on, JoinArgs::new(JoinType::Anti));
    let pl_df = anti.collect()?;
    Ok(super::DataFrame::from_polars_with_options(
        pl_df,
        case_sensitive,
    ))
}

/// Set intersection: rows that appear in both DataFrames (by all columns). PySpark intersect.
pub fn intersect(
    left: &DataFrame,
    right: &DataFrame,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    use polars::prelude::*;
    let left_names = left.df.get_column_names();
    let left_on: Vec<Expr> = left_names.iter().map(|n| col(n.as_str())).collect();
    let right_on: Vec<Expr> = left_names.iter().map(|n| col(n.as_str())).collect();
    let left_lf = left.df.as_ref().clone().lazy();
    let right_lf = right.df.as_ref().clone().lazy();
    let semi = left_lf.join(right_lf, left_on, right_on, JoinArgs::new(JoinType::Semi));
    let pl_df = semi.collect()?;
    Ok(super::DataFrame::from_polars_with_options(
        pl_df,
        case_sensitive,
    ))
}

// ---------- Batch A: sample, first/head/take/tail, is_empty, to_df ----------

/// Sample a fraction of rows. PySpark sample(withReplacement, fraction, seed).
pub fn sample(
    df: &DataFrame,
    with_replacement: bool,
    fraction: f64,
    seed: Option<u64>,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    use polars::prelude::Series;
    let n = df.df.height();
    if n == 0 {
        return Ok(super::DataFrame::from_polars_with_options(
            df.df.as_ref().clone(),
            case_sensitive,
        ));
    }
    let take_n = (n as f64 * fraction).round() as usize;
    let take_n = take_n.min(n).max(0);
    if take_n == 0 {
        return Ok(super::DataFrame::from_polars_with_options(
            df.df.as_ref().clone().head(Some(0)),
            case_sensitive,
        ));
    }
    let idx_series = Series::new("idx".into(), (0..n).map(|i| i as u32).collect::<Vec<_>>());
    let sampled_idx = idx_series.sample_n(take_n, with_replacement, true, seed)?;
    let idx_ca = sampled_idx
        .u32()
        .map_err(|_| PolarsError::ComputeError("sample: expected u32 indices".into()))?;
    let pl_df = df.df.as_ref().take(idx_ca)?;
    Ok(super::DataFrame::from_polars_with_options(
        pl_df,
        case_sensitive,
    ))
}

/// Split DataFrame by weights (random split). PySpark randomSplit(weights, seed).
/// Returns one DataFrame per weight; weights are normalized to fractions.
/// Each row is assigned to exactly one split (disjoint partitions).
pub fn random_split(
    df: &DataFrame,
    weights: &[f64],
    seed: Option<u64>,
    case_sensitive: bool,
) -> Result<Vec<DataFrame>, PolarsError> {
    let total: f64 = weights.iter().sum();
    if total <= 0.0 || weights.is_empty() {
        return Ok(Vec::new());
    }
    let n = df.df.height();
    if n == 0 {
        return Ok(weights.iter().map(|_| super::DataFrame::empty()).collect());
    }
    // Normalize weights to cumulative fractions: e.g. [0.25, 0.25, 0.5] -> [0.25, 0.5, 1.0]
    let mut cum = Vec::with_capacity(weights.len());
    let mut acc = 0.0_f64;
    for w in weights {
        acc += w / total;
        cum.push(acc);
    }
    // Assign each row index to one bucket using a single seeded RNG (disjoint split).
    use polars::prelude::Series;
    use rand::Rng;
    use rand::SeedableRng;
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed.unwrap_or(0));
    let mut bucket_indices: Vec<Vec<u32>> = (0..weights.len()).map(|_| Vec::new()).collect();
    for i in 0..n {
        let r: f64 = rng.gen();
        let bucket = cum
            .iter()
            .position(|&c| r < c)
            .unwrap_or(weights.len().saturating_sub(1));
        bucket_indices[bucket].push(i as u32);
    }
    let pl = df.df.as_ref();
    let mut out = Vec::with_capacity(weights.len());
    for indices in bucket_indices {
        if indices.is_empty() {
            out.push(super::DataFrame::from_polars_with_options(
                pl.clone().head(Some(0)),
                case_sensitive,
            ));
        } else {
            let idx_series = Series::new("idx".into(), indices);
            let idx_ca = idx_series.u32().map_err(|_| {
                PolarsError::ComputeError("random_split: expected u32 indices".into())
            })?;
            let taken = pl.take(idx_ca)?;
            out.push(super::DataFrame::from_polars_with_options(
                taken,
                case_sensitive,
            ));
        }
    }
    Ok(out)
}

/// Stratified sample by column value. PySpark sampleBy(col, fractions, seed).
/// fractions: list of (value as Expr literal, fraction to sample for that value).
pub fn sample_by(
    df: &DataFrame,
    col_name: &str,
    fractions: &[(Expr, f64)],
    seed: Option<u64>,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    use polars::prelude::*;
    if fractions.is_empty() {
        return Ok(super::DataFrame::from_polars_with_options(
            df.df.as_ref().clone().head(Some(0)),
            case_sensitive,
        ));
    }
    let resolved = df.resolve_column_name(col_name)?;
    let mut parts = Vec::with_capacity(fractions.len());
    for (value_expr, frac) in fractions {
        let cond = col(resolved.as_str()).eq(value_expr.clone());
        let filtered = df.df.as_ref().clone().lazy().filter(cond).collect()?;
        if filtered.height() == 0 {
            parts.push(filtered.head(Some(0)));
            continue;
        }
        let sampled = sample(
            &super::DataFrame::from_polars_with_options(filtered, case_sensitive),
            false,
            *frac,
            seed,
            case_sensitive,
        )?;
        parts.push(sampled.df.as_ref().clone());
    }
    let mut out = parts
        .first()
        .ok_or_else(|| PolarsError::ComputeError("sample_by: no parts".into()))?
        .clone();
    for p in parts.iter().skip(1) {
        out.vstack_mut(p)?;
    }
    Ok(super::DataFrame::from_polars_with_options(
        out,
        case_sensitive,
    ))
}

/// First row as a DataFrame (one row). PySpark first().
pub fn first(df: &DataFrame, case_sensitive: bool) -> Result<DataFrame, PolarsError> {
    let pl_df = df.df.as_ref().clone().head(Some(1));
    Ok(super::DataFrame::from_polars_with_options(
        pl_df,
        case_sensitive,
    ))
}

/// First n rows. PySpark head(n). Same as limit.
pub fn head(df: &DataFrame, n: usize, case_sensitive: bool) -> Result<DataFrame, PolarsError> {
    limit(df, n, case_sensitive)
}

/// Take first n rows (alias for limit). PySpark take(n).
pub fn take(df: &DataFrame, n: usize, case_sensitive: bool) -> Result<DataFrame, PolarsError> {
    limit(df, n, case_sensitive)
}

/// Last n rows. PySpark tail(n).
pub fn tail(df: &DataFrame, n: usize, case_sensitive: bool) -> Result<DataFrame, PolarsError> {
    let total = df.df.height();
    let skip = total.saturating_sub(n);
    let pl_df = df.df.as_ref().clone().slice(skip as i64, n);
    Ok(super::DataFrame::from_polars_with_options(
        pl_df,
        case_sensitive,
    ))
}

/// Whether the DataFrame has zero rows. PySpark isEmpty.
pub fn is_empty(df: &DataFrame) -> bool {
    df.df.height() == 0
}

/// Rename columns. PySpark toDF(*colNames). Names must match length of columns.
pub fn to_df(
    df: &DataFrame,
    names: &[&str],
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    let cols = df.df.get_column_names();
    if names.len() != cols.len() {
        return Err(PolarsError::ComputeError(
            format!(
                "toDF: expected {} column names, got {}",
                cols.len(),
                names.len()
            )
            .into(),
        ));
    }
    let mut pl_df = df.df.as_ref().clone();
    for (old, new) in cols.iter().zip(names.iter()) {
        pl_df.rename(old.as_str(), (*new).into())?;
    }
    Ok(super::DataFrame::from_polars_with_options(
        pl_df,
        case_sensitive,
    ))
}

// ---------- Batch B: toJSON, explain, printSchema ----------

fn any_value_to_serde_value(av: &polars::prelude::AnyValue) -> serde_json::Value {
    use polars::prelude::AnyValue;
    use serde_json::Number;
    match av {
        AnyValue::Null => serde_json::Value::Null,
        AnyValue::Boolean(v) => serde_json::Value::Bool(*v),
        AnyValue::Int8(v) => serde_json::Value::Number(Number::from(*v as i64)),
        AnyValue::Int32(v) => serde_json::Value::Number(Number::from(*v)),
        AnyValue::Int64(v) => serde_json::Value::Number(Number::from(*v)),
        AnyValue::UInt32(v) => serde_json::Value::Number(Number::from(*v)),
        AnyValue::Float64(v) => Number::from_f64(*v)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        AnyValue::String(v) => serde_json::Value::String(v.to_string()),
        _ => serde_json::Value::String(format!("{av:?}")),
    }
}

/// Collect rows as JSON strings (one JSON object per row). PySpark toJSON.
pub fn to_json(df: &DataFrame) -> Result<Vec<String>, PolarsError> {
    use polars::prelude::*;
    let pl = df.df.as_ref();
    let names = pl.get_column_names();
    let mut out = Vec::with_capacity(pl.height());
    for r in 0..pl.height() {
        let mut row = serde_json::Map::new();
        for (i, name) in names.iter().enumerate() {
            let col = pl
                .get_columns()
                .get(i)
                .ok_or_else(|| PolarsError::ComputeError("to_json: column index".into()))?;
            let series = col.as_materialized_series();
            let av = series
                .get(r)
                .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
            row.insert(name.to_string(), any_value_to_serde_value(&av));
        }
        out.push(
            serde_json::to_string(&row)
                .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?,
        );
    }
    Ok(out)
}

/// Return a string describing the execution plan. PySpark explain.
pub fn explain(_df: &DataFrame) -> String {
    "DataFrame (eager Polars backend)".to_string()
}

/// Return schema as a tree string. PySpark printSchema (we return string; caller can print).
pub fn print_schema(df: &DataFrame) -> Result<String, PolarsError> {
    let schema = df.schema()?;
    let mut s = "root\n".to_string();
    for f in schema.fields() {
        let dt = match &f.data_type {
            crate::schema::DataType::String => "string",
            crate::schema::DataType::Integer => "int",
            crate::schema::DataType::Long => "bigint",
            crate::schema::DataType::Double => "double",
            crate::schema::DataType::Boolean => "boolean",
            crate::schema::DataType::Date => "date",
            crate::schema::DataType::Timestamp => "timestamp",
            _ => "string",
        };
        s.push_str(&format!(" |-- {}: {}\n", f.name, dt));
    }
    Ok(s)
}

// ---------- Batch D: selectExpr, colRegex, withColumns, withColumnsRenamed, na ----------

/// Select by expression strings. Minimal support: comma-separated column names. PySpark selectExpr.
pub fn select_expr(
    df: &DataFrame,
    exprs: &[String],
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    let mut cols = Vec::new();
    for e in exprs {
        let e = e.trim();
        if let Some((left, right)) = e.split_once(" as ") {
            let col_name = left.trim();
            let _alias = right.trim();
            cols.push(df.resolve_column_name(col_name)?);
        } else {
            cols.push(df.resolve_column_name(e)?);
        }
    }
    let refs: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
    select(df, refs, case_sensitive)
}

/// Select columns whose names match the regex pattern. PySpark colRegex.
pub fn col_regex(
    df: &DataFrame,
    pattern: &str,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    let re = regex::Regex::new(pattern).map_err(|e| {
        PolarsError::ComputeError(format!("colRegex: invalid pattern {pattern:?}: {e}").into())
    })?;
    let names = df.df.get_column_names();
    let matched: Vec<&str> = names
        .iter()
        .filter(|n| re.is_match(n.as_str()))
        .map(|s| s.as_str())
        .collect();
    if matched.is_empty() {
        return Err(PolarsError::ComputeError(
            format!("colRegex: no columns matched pattern {pattern:?}").into(),
        ));
    }
    select(df, matched, case_sensitive)
}

/// Add or replace multiple columns. PySpark withColumns. Uses Column so deferred rand/randn get per-row values.
pub fn with_columns(
    df: &DataFrame,
    exprs: &[(String, crate::column::Column)],
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    let mut current =
        super::DataFrame::from_polars_with_options(df.df.as_ref().clone(), case_sensitive);
    for (name, col) in exprs {
        current = with_column(&current, name, col, case_sensitive)?;
    }
    Ok(current)
}

/// Rename multiple columns. PySpark withColumnsRenamed.
pub fn with_columns_renamed(
    df: &DataFrame,
    renames: &[(String, String)],
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    let mut out = df.df.as_ref().clone();
    for (old_name, new_name) in renames {
        let resolved = df.resolve_column_name(old_name)?;
        out.rename(resolved.as_str(), new_name.as_str().into())?;
    }
    Ok(super::DataFrame::from_polars_with_options(
        out,
        case_sensitive,
    ))
}

/// NA sub-API builder. PySpark df.na().fill(...) / .drop(...).
pub struct DataFrameNa<'a> {
    pub(crate) df: &'a DataFrame,
}

impl<'a> DataFrameNa<'a> {
    /// Fill nulls with the given value. PySpark na.fill(value, subset=...).
    pub fn fill(&self, value: Expr, subset: Option<Vec<&str>>) -> Result<DataFrame, PolarsError> {
        fillna(self.df, value, subset, self.df.case_sensitive)
    }

    /// Drop rows with nulls. PySpark na.drop(subset=..., how=..., thresh=...).
    pub fn drop(
        &self,
        subset: Option<Vec<&str>>,
        how: &str,
        thresh: Option<usize>,
    ) -> Result<DataFrame, PolarsError> {
        dropna(self.df, subset, how, thresh, self.df.case_sensitive)
    }
}

// ---------- Batch E: offset, transform, freqItems, approxQuantile, crosstab, melt, exceptAll, intersectAll ----------

/// Skip first n rows. PySpark offset(n).
pub fn offset(df: &DataFrame, n: usize, case_sensitive: bool) -> Result<DataFrame, PolarsError> {
    let total = df.df.height();
    let len = total.saturating_sub(n);
    let pl_df = df.df.as_ref().clone().slice(n as i64, len);
    Ok(super::DataFrame::from_polars_with_options(
        pl_df,
        case_sensitive,
    ))
}

/// Transform DataFrame by a function. PySpark transform(func).
pub fn transform<F>(df: &DataFrame, f: F) -> Result<DataFrame, PolarsError>
where
    F: FnOnce(DataFrame) -> Result<DataFrame, PolarsError>,
{
    let df_out = f(df.clone())?;
    Ok(df_out)
}

/// Frequent items. PySpark freqItems. Returns one row with columns {col}_freqItems (array of values with frequency >= support).
pub fn freq_items(
    df: &DataFrame,
    columns: &[&str],
    support: f64,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    use polars::prelude::SeriesMethods;
    if columns.is_empty() {
        return Ok(super::DataFrame::from_polars_with_options(
            df.df.as_ref().clone().head(Some(0)),
            case_sensitive,
        ));
    }
    let support = support.clamp(1e-4, 1.0);
    let pl_df = df.df.as_ref();
    let n_total = pl_df.height() as f64;
    if n_total == 0.0 {
        let mut out = Vec::with_capacity(columns.len());
        for col_name in columns {
            let resolved = df.resolve_column_name(col_name)?;
            let s = pl_df
                .column(resolved.as_str())?
                .as_series()
                .ok_or_else(|| PolarsError::ComputeError("column not a series".into()))?
                .clone();
            let empty_sub = s.head(Some(0));
            let list_chunked = polars::prelude::ListChunked::from_iter([empty_sub].into_iter())
                .with_name(format!("{resolved}_freqItems").into());
            out.push(list_chunked.into_series().into());
        }
        return Ok(super::DataFrame::from_polars_with_options(
            polars::prelude::DataFrame::new(out)?,
            case_sensitive,
        ));
    }
    let mut out_series = Vec::with_capacity(columns.len());
    for col_name in columns {
        let resolved = df.resolve_column_name(col_name)?;
        let s = pl_df
            .column(resolved.as_str())?
            .as_series()
            .ok_or_else(|| PolarsError::ComputeError("column not a series".into()))?
            .clone();
        let vc = s.value_counts(false, false, "counts".into(), false)?;
        let count_col = vc
            .column("counts")
            .map_err(|_| PolarsError::ComputeError("value_counts missing counts column".into()))?;
        let counts = count_col
            .u32()
            .map_err(|_| PolarsError::ComputeError("freq_items: counts column not u32".into()))?;
        let value_col_name = s.name();
        let values_col = vc
            .column(value_col_name.as_str())
            .map_err(|_| PolarsError::ComputeError("value_counts missing value column".into()))?;
        let threshold = (support * n_total).ceil() as u32;
        let indices: Vec<u32> = counts
            .into_iter()
            .enumerate()
            .filter_map(|(i, c)| {
                if c? >= threshold {
                    Some(i as u32)
                } else {
                    None
                }
            })
            .collect();
        let idx_series = Series::new("idx".into(), indices);
        let idx_ca = idx_series
            .u32()
            .map_err(|_| PolarsError::ComputeError("freq_items: index series not u32".into()))?;
        let values_series = values_col
            .as_series()
            .ok_or_else(|| PolarsError::ComputeError("value column not a series".into()))?;
        let filtered = values_series.take(idx_ca)?;
        let list_chunked = polars::prelude::ListChunked::from_iter([filtered].into_iter())
            .with_name(format!("{resolved}_freqItems").into());
        let list_row = list_chunked.into_series();
        out_series.push(list_row.into());
    }
    let out_df = polars::prelude::DataFrame::new(out_series)?;
    Ok(super::DataFrame::from_polars_with_options(
        out_df,
        case_sensitive,
    ))
}

/// Approximate quantiles. PySpark approxQuantile. Returns one column "quantile" with one row per probability.
pub fn approx_quantile(
    df: &DataFrame,
    column: &str,
    probabilities: &[f64],
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    use polars::prelude::{ChunkQuantile, QuantileMethod};
    if probabilities.is_empty() {
        return Ok(super::DataFrame::from_polars_with_options(
            polars::prelude::DataFrame::new(vec![Series::new(
                "quantile".into(),
                Vec::<f64>::new(),
            )
            .into()])?,
            case_sensitive,
        ));
    }
    let resolved = df.resolve_column_name(column)?;
    let s = df
        .df
        .as_ref()
        .column(resolved.as_str())?
        .as_series()
        .ok_or_else(|| PolarsError::ComputeError("approx_quantile: column not a series".into()))?
        .clone();
    let s_f64 = s.cast(&polars::prelude::DataType::Float64)?;
    let ca = s_f64
        .f64()
        .map_err(|_| PolarsError::ComputeError("approx_quantile: need numeric column".into()))?;
    let mut quantiles = Vec::with_capacity(probabilities.len());
    for &p in probabilities {
        let q = ca.quantile(p, QuantileMethod::Linear)?;
        quantiles.push(q.unwrap_or(f64::NAN));
    }
    let out_df =
        polars::prelude::DataFrame::new(vec![Series::new("quantile".into(), quantiles).into()])?;
    Ok(super::DataFrame::from_polars_with_options(
        out_df,
        case_sensitive,
    ))
}

/// Cross-tabulation. PySpark crosstab. Returns long format (col1, col2, count); for wide format use pivot on the result.
pub fn crosstab(
    df: &DataFrame,
    col1: &str,
    col2: &str,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    use polars::prelude::*;
    let c1 = df.resolve_column_name(col1)?;
    let c2 = df.resolve_column_name(col2)?;
    let pl_df = df.df.as_ref();
    let grouped = pl_df
        .clone()
        .lazy()
        .group_by([col(c1.as_str()), col(c2.as_str())])
        .agg([len().alias("count")])
        .collect()?;
    Ok(super::DataFrame::from_polars_with_options(
        grouped,
        case_sensitive,
    ))
}

/// Unpivot (melt). PySpark melt. Long format with id_vars kept, plus "variable" and "value" columns.
pub fn melt(
    df: &DataFrame,
    id_vars: &[&str],
    value_vars: &[&str],
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    use polars::prelude::*;
    let pl_df = df.df.as_ref();
    if value_vars.is_empty() {
        return Ok(super::DataFrame::from_polars_with_options(
            pl_df.head(Some(0)),
            case_sensitive,
        ));
    }
    let id_resolved: Vec<String> = id_vars
        .iter()
        .map(|s| df.resolve_column_name(s).map(|r| r.to_string()))
        .collect::<Result<Vec<_>, _>>()?;
    let value_resolved: Vec<String> = value_vars
        .iter()
        .map(|s| df.resolve_column_name(s).map(|r| r.to_string()))
        .collect::<Result<Vec<_>, _>>()?;
    let mut parts = Vec::with_capacity(value_vars.len());
    for vname in &value_resolved {
        let select_cols: Vec<&str> = id_resolved
            .iter()
            .map(|s| s.as_str())
            .chain([vname.as_str()])
            .collect();
        let mut part = pl_df.select(select_cols)?;
        let var_series = Series::new("variable".into(), vec![vname.as_str(); part.height()]);
        part.with_column(var_series)?;
        part.rename(vname.as_str(), "value".into())?;
        parts.push(part);
    }
    let mut out = parts
        .first()
        .ok_or_else(|| PolarsError::ComputeError("melt: no value columns".into()))?
        .clone();
    for p in parts.iter().skip(1) {
        out.vstack_mut(p)?;
    }
    let col_order: Vec<&str> = id_resolved
        .iter()
        .map(|s| s.as_str())
        .chain(["variable", "value"])
        .collect();
    let out = out.select(col_order)?;
    Ok(super::DataFrame::from_polars_with_options(
        out,
        case_sensitive,
    ))
}

/// Set difference keeping duplicates. PySpark exceptAll. Simple impl: same as subtract.
pub fn except_all(
    left: &DataFrame,
    right: &DataFrame,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    subtract(left, right, case_sensitive)
}

/// Set intersection keeping duplicates. PySpark intersectAll. Simple impl: same as intersect.
pub fn intersect_all(
    left: &DataFrame,
    right: &DataFrame,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    intersect(left, right, case_sensitive)
}

#[cfg(test)]
mod tests {
    use super::{distinct, drop, dropna, first, head, limit, offset};
    use crate::{DataFrame, SparkSession};

    fn test_df() -> DataFrame {
        let spark = SparkSession::builder()
            .app_name("transform_tests")
            .get_or_create();
        spark
            .create_dataframe(
                vec![
                    (1i64, 10i64, "a".to_string()),
                    (2i64, 20i64, "b".to_string()),
                    (3i64, 30i64, "c".to_string()),
                ],
                vec!["id", "v", "label"],
            )
            .unwrap()
    }

    #[test]
    fn limit_zero() {
        let df = test_df();
        let out = limit(&df, 0, false).unwrap();
        assert_eq!(out.count().unwrap(), 0);
    }

    #[test]
    fn limit_more_than_rows() {
        let df = test_df();
        let out = limit(&df, 10, false).unwrap();
        assert_eq!(out.count().unwrap(), 3);
    }

    #[test]
    fn distinct_on_empty() {
        let spark = SparkSession::builder()
            .app_name("transform_tests")
            .get_or_create();
        let df = spark
            .create_dataframe(vec![] as Vec<(i64, i64, String)>, vec!["a", "b", "c"])
            .unwrap();
        let out = distinct(&df, None, false).unwrap();
        assert_eq!(out.count().unwrap(), 0);
    }

    #[test]
    fn first_returns_one_row() {
        let df = test_df();
        let out = first(&df, false).unwrap();
        assert_eq!(out.count().unwrap(), 1);
    }

    #[test]
    fn head_n() {
        let df = test_df();
        let out = head(&df, 2, false).unwrap();
        assert_eq!(out.count().unwrap(), 2);
    }

    #[test]
    fn offset_skip_first() {
        let df = test_df();
        let out = offset(&df, 1, false).unwrap();
        assert_eq!(out.count().unwrap(), 2);
    }

    #[test]
    fn offset_beyond_length_returns_empty() {
        let df = test_df();
        let out = offset(&df, 10, false).unwrap();
        assert_eq!(out.count().unwrap(), 0);
    }

    #[test]
    fn drop_column() {
        let df = test_df();
        let out = drop(&df, vec!["v"], false).unwrap();
        let cols = out.columns().unwrap();
        assert!(!cols.contains(&"v".to_string()));
        assert_eq!(out.count().unwrap(), 3);
    }

    #[test]
    fn dropna_all_columns() {
        let df = test_df();
        let out = dropna(&df, None, "any", None, false).unwrap();
        assert_eq!(out.count().unwrap(), 3);
    }
}

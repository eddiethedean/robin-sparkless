//! DataFrame transformation operations: filter, select, with_column, order_by,
//! union, distinct, drop, dropna, fillna, limit, with_column_renamed,
//! replace, cross_join, describe, subtract, intersect,
//! sample, random_split, first, head, take, tail, is_empty, to_df.

use super::DataFrame;
use crate::column::expect_col;
use crate::functions::SortOrder;
use crate::type_coercion::{coerce_expr_pair, find_common_type, is_numeric_public};
use crate::udfs;
use polars::prelude::{
    DataType, Expr, Float64Chunked, IntoLazy, IntoSeries, NamedFrom, PlSmallStr, PolarsError,
    SchemaNamesAndDtypes, Selector, Series, UniqueKeepStrategy, col, len, lit, repeat,
};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};

/// Returns the set of column names referenced in the expression tree.
fn expr_referenced_columns(expr: &Expr) -> HashSet<String> {
    let refs = RefCell::new(HashSet::<String>::new());
    let _ = expr.clone().try_map_expr(|e| {
        if let Expr::Column(n) = &e {
            refs.borrow_mut().insert(n.as_str().to_string());
        }
        Ok(e)
    });
    refs.into_inner()
}

/// Returns true if the expression tree contains a reference to the given column name (so we must not drop it before adding).
fn expr_refs_column(expr: &Expr, column_name: &str) -> bool {
    expr_referenced_columns(expr).contains(column_name)
}

/// If the expression contains an Explode node, return its input and options (for posexplode detection).
fn find_explode_in_expr(expr: &Expr) -> Option<(Arc<Expr>, polars::prelude::ExplodeOptions)> {
    if let Expr::Explode { input, options } = expr {
        return Some((input.clone(), *options));
    }
    if let Expr::Alias(inner, _) = expr {
        return find_explode_in_expr(inner.as_ref());
    }
    // Recurse into common wrappers so we find Explode inside StructField etc.
    let out = RefCell::new(None);
    let _ = expr.clone().try_map_expr(|e| {
        if out.borrow().is_none() {
            if let Expr::Explode { input, options } = &e {
                out.borrow_mut().replace((input.clone(), *options));
            }
        }
        Ok(e)
    });
    out.into_inner()
}

/// Replace a pure literal expr with a column-referencing expr so Polars produces correct row count.
/// Polars lit() in select-only or empty-df contexts yields 1 row; we need N rows (or 0 for empty).
fn expand_pure_literal_to_rows(expr: Expr, first_col: Option<&str>) -> Result<Expr, PolarsError> {
    let (inner, alias): (Expr, Option<PlSmallStr>) = match &expr {
        Expr::Alias(e, name) => (e.as_ref().clone(), Some(name.clone())),
        _ => (expr.clone(), None),
    };
    let (lit_val, out_dtype): (String, DataType) = match &inner {
        Expr::Literal(lv) if lv.get_datatype() == DataType::String => {
            let s = lv.extract_str().unwrap_or("");
            (s.to_string(), DataType::String)
        }
        _ => return Ok(expr),
    };
    let expanded = if let Some(fc) = first_col {
        let fc = fc.to_string();
        use polars::datatypes::Field;
        col(PlSmallStr::from(fc.as_str())).map(
            move |c| expect_col(udfs::apply_literal_string_repeat(c, &lit_val)),
            move |_schema, _field| Ok(Field::new("literal".into(), out_dtype.clone())),
        )
    } else {
        // No columns (e.g. empty schema): use repeat(lit(""), len()) so empty df yields 0 rows
        repeat(lit(lit_val), len().cast(DataType::UInt32))
    };
    Ok(if let Some(name) = alias {
        expanded.alias(name.as_str())
    } else {
        expanded
    })
}

fn series_as_f64_ca(s: &Series, context: &str) -> Result<Float64Chunked, PolarsError> {
    let s_f64 = s.cast(&DataType::Float64)?;
    let ca = s_f64.f64().map_err(|_| {
        PolarsError::ComputeError(format!("{}: need numeric/f64 column", context).into())
    })?;
    Ok(ca.clone())
}
use std::sync::Arc;

/// Select columns (returns a new DataFrame). Preserves case_sensitive on result.
pub fn select(
    df: &DataFrame,
    cols: Vec<&str>,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    let resolved: Vec<String> = cols
        .iter()
        .map(|c| df.resolve_column_name(c))
        .collect::<Result<Vec<_>, _>>()?;
    let exprs: Vec<Expr> = resolved.iter().map(|s| col(s.as_str())).collect();
    let lf = df.lazy_frame().select(&exprs);
    Ok(super::DataFrame::from_lazy_with_options(lf, case_sensitive))
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
    let exprs: Vec<Expr> = exprs
        .into_iter()
        .map(|e| df.coerce_string_numeric_comparisons(e))
        .collect::<Result<Vec<_>, _>>()?;
    let df_columns: HashSet<String> = df
        .columns()
        .ok()
        .map(|c| c.into_iter().map(|s| s.as_str().to_string()).collect())
        .unwrap_or_default();
    let first_col = df_columns.iter().next().map(String::as_str);
    let exprs: Vec<Expr> = exprs
        .into_iter()
        .map(|e| expand_pure_literal_to_rows(e.clone(), first_col).unwrap_or(e))
        .collect();

    // Detect posexplode: two exprs that each contain Explode(list.eval(...)) with same single list column (#1243).
    type PosexplodeTarget = (
        PlSmallStr,
        polars::prelude::ExplodeOptions,
        [(PlSmallStr, usize); 2], // (alias_pos, idx_pos), (alias_col, idx_col)
    );
    let posexplode_target: Option<PosexplodeTarget> = {
        let mut by_col: HashMap<
            String,
            Vec<(polars::prelude::ExplodeOptions, String, usize)>,
        > = HashMap::new();
        for (i, e) in exprs.iter().enumerate() {
            let (inner, alias_name) = match e {
                Expr::Alias(inner, name) => (inner.as_ref(), name.as_str().to_string()),
                _ => continue,
            };
            if let Some((input, options)) = find_explode_in_expr(inner) {
                let refs = expr_referenced_columns(input.as_ref());
                // List.eval uses col("") for element context; only consider columns that exist in the frame.
                let frame_refs: Vec<String> = refs
                    .intersection(&df_columns)
                    .cloned()
                    .collect();
                if frame_refs.len() == 1 {
                    let list_col = frame_refs.into_iter().next().unwrap();
                    by_col
                        .entry(list_col.clone())
                        .or_default()
                        .push((options, alias_name, i));
                }
            }
        }
        by_col
            .into_iter()
            .find(|(_, v)| v.len() == 2)
            .map(|(list_col, mut v)| {
                v.sort_by_key(|(_, _, i)| *i);
                let list_col = PlSmallStr::from(list_col.as_str());
                let options = v[0].0;
                (
                    list_col,
                    options,
                    [
                        (PlSmallStr::from(v[0].1.as_str()), v[0].2),
                        (PlSmallStr::from(v[1].1.as_str()), v[1].2),
                    ],
                )
            })
    };

    // Detect simple explode expressions (e.g. F.explode(col(\"scores\")).alias(\"score\"))
    // and rewrite to use LazyFrame.explode. When alias != source column, add a copy then explode
    // so the original list column is preserved (PySpark select(Name, Value, explode(Value).alias("ExplodedValue"))).
    // Python Column.into_expr() does expr.alias(name), so explode(col).alias("x") becomes Alias(Alias(Explode(...), "x"), "x"); peel one more Alias.
    let posexplode_pe_col = posexplode_target
        .as_ref()
        .map(|(lc, _, _)| format!("__pe_{}", lc.as_str()));

    type ExplodeTarget = (
        PlSmallStr,
        Option<PlSmallStr>,
        polars::prelude::ExplodeOptions,
    );
    let mut explode_target: Option<ExplodeTarget> = None;
    let exprs: Vec<Expr> = exprs
        .into_iter()
        .enumerate()
        .map(|(i, e)| {
            if let (Some(pt), Some(pe_col)) = (&posexplode_target, &posexplode_pe_col) {
                if i == pt.2[0].1 {
                    return col(pe_col.as_str())
                        .struct_()
                        .field_by_name("pos")
                        .alias(pt.2[0].0.as_str());
                }
                if i == pt.2[1].1 {
                    return col(pe_col.as_str())
                        .struct_()
                        .field_by_name("col")
                        .alias(pt.2[1].0.as_str());
                }
            }
            match e {
            Expr::Alias(inner, name) => {
                let inner_ref = inner.as_ref();
                let (explode_input, options): (Option<Arc<Expr>>, polars::prelude::ExplodeOptions) =
                    if let Expr::Explode { input, options } = inner_ref {
                        (Some(input.clone()), *options)
                    } else if let Expr::Alias(inner2, _) = inner_ref {
                        if let Expr::Explode { input, options } = inner2.as_ref() {
                            (Some(input.clone()), *options)
                        } else {
                            (
                                None,
                                polars::prelude::ExplodeOptions {
                                    empty_as_null: false,
                                    keep_nulls: false,
                                },
                            )
                        }
                    } else {
                        (
                            None,
                            polars::prelude::ExplodeOptions {
                                empty_as_null: false,
                                keep_nulls: false,
                            },
                        )
                    };
                if let (Some(input), options) = (explode_input, options) {
                    if let Expr::Column(col_name) = input.as_ref() {
                        if explode_target.is_none() {
                            let alias_name = if col_name.as_str() != name.as_str() {
                                Some(name.clone())
                            } else {
                                None
                            };
                            explode_target = Some((col_name.clone(), alias_name, options));
                        }
                        let out_col = explode_target
                            .as_ref()
                            .and_then(|(_, a, _)| a.clone())
                            .unwrap_or_else(|| col_name.clone());
                        Expr::Alias(Arc::new(Expr::Column(out_col)), name)
                    } else {
                        Expr::Alias(inner, name)
                    }
                } else {
                    Expr::Alias(inner, name)
                }
            }
            Expr::Explode { input, options } => {
                if let Expr::Column(col_name) = input.as_ref() {
                    if explode_target.is_none() {
                        explode_target = Some((col_name.clone(), None, options));
                    }
                    let (_, alias_name, _) = explode_target.as_ref().unwrap();
                    let out_col = alias_name.clone().unwrap_or_else(|| col_name.clone());
                    Expr::Column(out_col)
                } else {
                    Expr::Explode { input, options }
                }
            }
            other => other,
        }
        })
        .collect();
    let mut name_count: HashMap<String, u32> = HashMap::new();
    let mut output_names: Vec<String> = Vec::new();
    let exprs: Vec<Expr> = exprs
        .into_iter()
        .map(|e| {
            let base_name = polars_plan::utils::expr_output_name(&e)
                .map(|s| s.to_string())
                .unwrap_or_else(|_| "_".to_string());
            let count = name_count.entry(base_name.clone()).or_insert(0);
            *count += 1;
            let final_name = if *count == 1 {
                base_name.clone()
            } else {
                format!("{}_{}", base_name, *count - 1)
            };
            output_names.push(final_name.clone());
            if *count == 1 {
                e
            } else {
                e.alias(final_name.as_str())
            }
        })
        .collect();

    // When every expression references no column from the frame, Polars select yields 1 row.
    // Cross-join with a single key column to get N rows (PySpark parity).
    let mut lf = df.lazy_frame();
    let had_explode = explode_target.is_some() || posexplode_target.is_some();

    // If we saw an explode expression on a single column, apply frame-level explode first so
    // non-exploded columns are replicated correctly (PySpark explode parity).
    // When alias != source (e.g. select(Name, Value, explode(Value).alias("ExplodedValue"))),
    // add a copy column then explode it so the original list column is preserved.
    if let Some((explode_col, alias_name, options)) = explode_target {
        if let Some(alias) = &alias_name {
            lf = lf.with_column(col(explode_col.as_str()).alias(alias.as_str()));
            let selector = Selector::ByName {
                names: Arc::from([alias.clone()]),
                strict: true,
            };
            lf = lf.explode(selector, options);
        } else {
            let selector = Selector::ByName {
                names: Arc::from([explode_col]),
                strict: true,
            };
            lf = lf.explode(selector, options);
        }
    }
    // Posexplode: add list-of-structs column and explode so non-exploded columns replicate (#1243).
    if let Some((list_col, options, _)) = &posexplode_target {
        let pe_col = format!("__pe_{}", list_col.as_str());
        use polars::prelude::as_struct;
        let pos_inner = (col("").cum_count(false) - lit(1i64)).alias("pos");
        let val_inner = col("").alias("col");
        let list_struct = col(list_col.as_str())
            .list()
            .eval(as_struct(vec![pos_inner, val_inner]));
        lf = lf.with_column(list_struct.alias(pe_col.as_str()));
        let selector = Selector::ByName {
            names: Arc::from([PlSmallStr::from(pe_col.as_str())]),
            strict: true,
        };
        lf = lf.explode(selector, *options);
    }
    // When we applied explode, the frame was already expanded; rewritten exprs may reference only
    // the new column (e.g. "x"), so we must not use the no-col-refs cross_join path (it would
    // wrongly cross-join and multiply rows).
    let no_col_refs = !had_explode
        && first_col.is_some()
        && exprs.iter().all(|e| {
            expr_referenced_columns(e)
                .intersection(&df_columns)
                .next()
                .is_none()
        });
    let lf = if no_col_refs {
        let first = first_col.unwrap();
        let lf_key = lf.clone().select([col(first)]);
        let lf_vals = lf.select(&exprs);
        let joined = lf_key.cross_join(lf_vals, None);
        let right_exprs: Vec<Expr> = output_names.iter().map(|n| col(n.as_str())).collect();
        joined.select(right_exprs)
    } else {
        lf.select(&exprs)
    };
    // When input is Eager (e.g. createDataFrame), return Eager result so collect() uses
    // schema order that matches columns (#1267). List-of-dicts createDataFrame first column
    // null is fixed in Python binding (python_row_to_json).
    if df.is_eager() {
        let pl_df = lf.collect()?;
        Ok(super::DataFrame::from_eager_with_options(
            pl_df,
            case_sensitive,
        ))
    } else {
        Ok(super::DataFrame::from_lazy_with_options(lf, case_sensitive))
    }
}

/// Select item: either a column name (str) or an expression (PySpark parity: select("a", col("b").alias("x"))).
/// Fixes #645: select expects Column or str.
#[derive(Clone)]
pub enum SelectItem<'a> {
    /// Column name; resolved per DataFrame case sensitivity.
    ColumnName(&'a str),
    /// Expression (e.g. from col("x").cast(...).alias("y")).
    Expr(Expr),
}

/// Safe output name for struct field select: Polars can resolve dotted aliases as column lookups, so use a placeholder (no dot) and rename after select to the desired output name (e.g. "Person.name").
fn struct_field_safe_alias(dotted_name: &str) -> String {
    format!("__sf_{}", dotted_name.replace('.', "_"))
}

/// Select using a mix of column names and expressions. Preserves case_sensitive on result.
pub fn select_items(
    df: &DataFrame,
    items: Vec<SelectItem<'_>>,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    let mut exprs = Vec::with_capacity(items.len());
    let mut rename_after: Vec<(String, String)> = Vec::new();
    for item in items {
        match item {
            SelectItem::ColumnName(name) => {
                // When multiple physical columns match by lowercase name (e.g. "name" and "NAME"
                // after a join), we need PySpark-like behavior for ambiguous selects such as
                // select("NaMe") (#297). We coalesce all matching physical columns and expose
                // the result under the requested name so that:
                // - rows that exist only on one side (e.g. right-join-only rows) still have a value, and
                // - original physical columns remain accessible by their own names.
                if let Ok(cols) = df.columns() {
                    let name_lower = name.to_lowercase();
                    let matches: Vec<String> = cols
                        .iter()
                        .filter(|c| c.to_lowercase() == name_lower)
                        .cloned()
                        .collect();
                    if matches.len() > 1 {
                        use polars::prelude::coalesce as pl_coalesce;
                        let coalesce_exprs: Vec<Expr> =
                            matches.iter().map(|m| col(m.as_str())).collect();
                        let coalesced = pl_coalesce(&coalesce_exprs);
                        exprs.push(coalesced.alias(name));
                        continue;
                    }
                }
                // #1055, #1076: Dot notation (e.g. "StructValue.e1") is struct field access.
                // PySpark: select("StructValue.e1") yields output column "e1" (last segment), not full dotted name.
                if name.contains('.') {
                    let e = col(name);
                    let resolved = df.resolve_expr_column_names(e)?;
                    let coerced = df.coerce_string_numeric_comparisons(resolved)?;
                    let safe = struct_field_safe_alias(name);
                    let last_segment = name.split('.').next_back().unwrap_or(name);
                    rename_after.push((safe.clone(), last_segment.to_string()));
                    exprs.push(coerced.alias(safe));
                } else {
                    let resolved = df.resolve_column_name(name)?;
                    // Explicit alias so output name is stable when mixed with window exprs (#1267).
                    exprs.push(col(resolved).alias(name));
                }
            }
            SelectItem::Expr(e) => {
                let name_for_alias = if let polars::prelude::Expr::Column(n) = &e {
                    let s = n.as_str();
                    if s.contains('.') {
                        Some(s.to_string())
                    } else {
                        None
                    }
                } else if let polars::prelude::Expr::Alias(_, n) = &e {
                    let s = n.as_str();
                    if s.contains('.') {
                        Some(s.to_string())
                    } else {
                        None
                    }
                } else {
                    None
                };
                let resolved = df.resolve_expr_column_names(e)?;
                let coerced = df.coerce_string_numeric_comparisons(resolved)?;
                if let Some(name) = name_for_alias {
                    let safe = struct_field_safe_alias(&name);
                    let last_segment = name.split('.').next_back().unwrap_or(&name).to_string();
                    rename_after.push((safe.clone(), last_segment));
                    exprs.push(coerced.alias(safe));
                } else {
                    exprs.push(coerced);
                }
            }
        }
    }
    let mut result = select_with_exprs(df, exprs, case_sensitive)?;
    for (from, to) in rename_after {
        result = result.with_column_renamed(&from, &to)?;
    }
    Ok(result)
}

/// Filter rows using a Polars expression. Preserves case_sensitive on result.
/// Column names in the condition are resolved per df's case sensitivity (PySpark parity).
/// #646: Coerce predicate to Boolean so Polars never receives a non-Boolean filter (e.g. string-involving predicates).
/// Uses expr_coerce_to_boolean so string columns cast via "true"/"false"/"1"/"0" (PySpark parity).
/// Filter rows using a Polars expression. Preserves case_sensitive on result.
/// Column names in the condition are resolved per df's case sensitivity (PySpark parity).
/// #646: Coerce predicate to Boolean so Polars never receives a non-Boolean filter (e.g. string-involving predicates).
/// Uses expr_coerce_to_boolean so string columns cast via "true"/"false"/"1"/"0" (PySpark parity).
pub fn filter(
    df: &DataFrame,
    condition: Expr,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    let condition = df.resolve_expr_column_names(condition)?;
    let condition = df.coerce_string_numeric_comparisons(condition)?;
    // #972: expr_coerce_to_boolean already yields Boolean (and handles string via apply_string_to_boolean).
    // Do not call .cast(DataType::Boolean) here — Polars does not support casting Utf8View to Boolean.
    let condition = crate::functions::expr_coerce_to_boolean(condition);
    let lf = df.lazy_frame().filter(condition);
    Ok(super::DataFrame::from_lazy_with_options(lf, case_sensitive))
}

/// Add or replace a column. Handles deferred rand/randn and Python UDF (UdfCall).
pub fn with_column(
    df: &DataFrame,
    column_name: &str,
    column: &crate::column::Column,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    // Python UDF: eager execution at UDF boundary
    if let Some(deferred) = column.deferred {
        match deferred {
            crate::column::DeferredRandom::Rand(seed) => {
                let pl_df = df.collect_inner()?;
                let mut pl_df = pl_df.as_ref().clone();
                let n = pl_df.height();
                let series = crate::udfs::series_rand_n(column_name, n, seed);
                pl_df.with_column(series.into())?;
                return Ok(super::DataFrame::from_polars_with_options(
                    pl_df,
                    case_sensitive,
                ));
            }
            crate::column::DeferredRandom::Randn(seed) => {
                let pl_df = df.collect_inner()?;
                let mut pl_df = pl_df.as_ref().clone();
                let n = pl_df.height();
                let series = crate::udfs::series_randn_n(column_name, n, seed);
                pl_df.with_column(series.into())?;
                return Ok(super::DataFrame::from_polars_with_options(
                    pl_df,
                    case_sensitive,
                ));
            }
        }
    }
    let mut expr = df.resolve_expr_column_names(column.expr().clone())?;
    expr = df.coerce_string_numeric_comparisons(expr)?;
    if let Ok(cols) = df.columns() {
        let first_col = cols.into_iter().next();
        if let Ok(expanded) = expand_pure_literal_to_rows(expr.clone(), first_col.as_deref()) {
            expr = expanded;
        }
    }
    // PySpark withColumn replaces if column exists; avoid duplicate output name (e.g. CTE self-join).
    // If the new expr references the column being replaced, replace in one select so the column stays in scope.
    // When replacing with explode(col(name)), use LazyFrame.explode() so other columns are replicated (PySpark parity).
    let lf = df.lazy_frame();
    let lf = if let Ok(existing) = df.resolve_column_name(column_name) {
        let all = df.columns()?;
        let existing_str = existing.as_str();
        if expr_refs_column(&expr, existing_str) {
            let inner = match &expr {
                Expr::Alias(e, _) => e.as_ref(),
                e => e,
            };
            // Use LazyFrame.explode() when replacing a column with explode(that column) so other columns replicate.
            let refs = expr_referenced_columns(inner);
            let use_frame_explode = refs.len() == 1
                && refs.contains(existing_str)
                && matches!(inner, Expr::Explode { .. });
            if use_frame_explode {
                let options = match inner {
                    Expr::Explode { options, .. } => *options,
                    _ => unreachable!(),
                };
                let selector = Selector::ByName {
                    names: Arc::from([PlSmallStr::from(existing_str)]),
                    strict: true,
                };
                lf.explode(selector, options)
            } else {
                // Replace in one shot: select all columns but use expr for the replaced name.
                let select_exprs: Vec<Expr> = all
                    .iter()
                    .map(|n| {
                        if n.as_str() == existing_str {
                            expr.clone().alias(column_name)
                        } else {
                            col(n.as_str())
                        }
                    })
                    .collect();
                lf.select(select_exprs)
            }
        } else {
            let to_keep: Vec<Expr> = all
                .iter()
                .filter(|n| n.as_str() != existing_str)
                .map(|n| col(n.as_str()))
                .collect();
            lf.select(&to_keep).with_column(expr.alias(column_name))
        }
    } else {
        // Adding a new column. If expr is explode(some_column), use frame-level explode so row count matches (PySpark parity).
        // Preserve the original list column: add a copy with column_name, then explode it (so Name/Value stay, ExplodedValue expands).
        let inner = match &expr {
            Expr::Alias(e, _) => e.as_ref(),
            e => e,
        };
        if let Expr::Explode {
            input,
            options: explode_opts,
        } = inner
        {
            if let Expr::Column(explode_col) = input.as_ref() {
                let refs = expr_referenced_columns(inner);
                if refs.len() == 1 {
                    let explode_col_str = explode_col.as_str();
                    if df.resolve_column_name(explode_col_str).is_ok() {
                        // Add new column as copy of list, then explode it so original column is preserved.
                        let lf_with_copy =
                            lf.with_column(col(explode_col.as_str()).alias(column_name));
                        let selector = Selector::ByName {
                            names: Arc::from([PlSmallStr::from(column_name)]),
                            strict: true,
                        };
                        lf_with_copy.explode(selector, *explode_opts)
                    } else {
                        lf.with_column(expr.alias(column_name))
                    }
                } else {
                    lf.with_column(expr.alias(column_name))
                }
            } else {
                lf.with_column(expr.alias(column_name))
            }
        } else {
            lf.with_column(expr.alias(column_name))
        }
    };
    Ok(super::DataFrame::from_lazy_with_options(lf, case_sensitive))
}

/// Order by columns (sort). Preserves case_sensitive on result.
/// Note: We do not coerce string sort columns to Float64 here (unlike filter/join) so that
/// genuine string sort (e.g. by name) is preserved. A future improvement could apply
/// coercion only when the column is numeric-looking (e.g. try_parse to number).
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
    let resolved: Vec<String> = column_names
        .iter()
        .map(|c| df.resolve_column_name(c))
        .collect::<Result<Vec<_>, _>>()?;
    let exprs: Vec<Expr> = resolved.iter().map(|s| col(s.as_str())).collect();
    let descending: Vec<bool> = asc.iter().map(|&a| !a).collect();
    // PySpark default: nulls last for both ASC and DESC (issue #1052 / #327 test expectation).
    let nulls_last: Vec<bool> = vec![true; column_names.len()];
    let lf = df.lazy_frame().sort_by_exprs(
        exprs,
        SortMultipleOptions::new()
            .with_order_descending_multi(descending)
            .with_nulls_last_multi(nulls_last),
    );
    Ok(super::DataFrame::from_lazy_with_options(lf, case_sensitive))
}

/// Order by sort expressions (asc/desc with nulls_first/last). Preserves case_sensitive on result.
/// Column names in sort expressions are resolved per df's case sensitivity (PySpark parity).
/// #1261: Coerce string–numeric in sort expressions (e.g. orderBy(col("s") / 10)) so string columns
/// used in arithmetic are cast to numeric for sorting.
pub fn order_by_exprs(
    df: &DataFrame,
    sort_orders: Vec<SortOrder>,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    use polars::prelude::*;
    if sort_orders.is_empty() {
        return Ok(super::DataFrame::from_lazy_with_options(
            df.lazy_frame(),
            case_sensitive,
        ));
    }
    let exprs: Vec<Expr> = sort_orders
        .iter()
        .map(|s| {
            let e = df.resolve_expr_column_names(s.expr().clone())?;
            df.coerce_string_numeric_comparisons(e)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let descending: Vec<bool> = sort_orders.iter().map(|s| s.descending).collect();
    let nulls_last: Vec<bool> = sort_orders.iter().map(|s| s.nulls_last).collect();
    let opts = SortMultipleOptions::new()
        .with_order_descending_multi(descending)
        .with_nulls_last_multi(nulls_last);
    let lf = df.lazy_frame().sort_by_exprs(exprs, opts);
    Ok(super::DataFrame::from_lazy_with_options(lf, case_sensitive))
}

/// Union (unionAll): stack another DataFrame vertically.
/// When column names match (set equality, order may differ): use left's order, reorder right by name (#551).
/// When column count matches but names differ: union by position (PySpark/createDataFrame parity #1018):
/// align by index, result uses left's column names.
/// When column types differ, both sides are coerced to a common type.
pub fn union(
    left: &DataFrame,
    right: &DataFrame,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    let left_names = left.columns()?;
    let right_names = right.columns()?;
    if left_names.len() != right_names.len() {
        return Err(PolarsError::InvalidOperation(
            format!(
                "union: column count must match. Left: {:?}, Right: {:?}",
                left_names, right_names
            )
            .into(),
        ));
    }
    let right_names_set: std::collections::HashSet<_> = if case_sensitive {
        right_names.iter().cloned().collect()
    } else {
        right_names.iter().map(|s| s.to_lowercase()).collect()
    };
    let names_match = left_names.iter().all(|n| {
        let key = if case_sensitive {
            n.clone()
        } else {
            n.to_lowercase()
        };
        right_names_set.contains(&key)
    });

    let debug_union = std::env::var("SPARKLESS_DEBUG_UNION").as_deref() == Ok("1");
    let (left_exprs, right_exprs) = if names_match {
        // Same set of names: use left's order for both sides (reorder right by name).
        let mut left_exprs = Vec::with_capacity(left_names.len());
        let mut right_exprs = Vec::with_capacity(right_names.len());
        for name in &left_names {
            let resolved_left = left.resolve_column_name(name)?;
            let resolved_right = right.resolve_column_name(name)?;
            let left_dtype = left.get_column_dtype(name).unwrap_or(DataType::Null);
            let right_dtype = right.get_column_dtype(name).unwrap_or(DataType::Null);
            let mut target = if left_dtype == DataType::Null {
                right_dtype.clone()
            } else if right_dtype == DataType::Null || left_dtype == right_dtype {
                left_dtype.clone()
            } else {
                find_common_type(&left_dtype, &right_dtype)?
            };
            // Issue #1262: when one side is String and the other numeric, coerce to String
            // (PySpark union behavior); ensure we cast even if get_column_dtype disagrees.
            if (left_dtype == DataType::String && is_numeric_public(&right_dtype))
                || (right_dtype == DataType::String && is_numeric_public(&left_dtype))
            {
                target = DataType::String;
            }
            let need_coerce = left_dtype != target || right_dtype != target;
            if debug_union {
                eprintln!(
                    "[union #1262] name={:?} left_dtype={:?} right_dtype={:?} target={:?} need_coerce={}",
                    name, left_dtype, right_dtype, target, need_coerce
                );
            }
            let left_expr = if need_coerce {
                col(resolved_left.as_str()).cast(target.clone())
            } else {
                col(resolved_left.as_str())
            };
            let right_expr = if need_coerce {
                col(resolved_right.as_str()).cast(target)
            } else {
                col(resolved_right.as_str())
            };
            left_exprs.push(left_expr.alias(name.as_str()));
            right_exprs.push(right_expr.alias(name.as_str()));
        }
        (left_exprs, right_exprs)
    } else {
        // #1018: Union by position — same column count, different names; align by index, use left's names.
        let mut left_exprs = Vec::with_capacity(left_names.len());
        let mut right_exprs = Vec::with_capacity(right_names.len());
        for (i, left_name) in left_names.iter().enumerate() {
            let right_name = right_names.get(i).ok_or_else(|| {
                PolarsError::InvalidOperation("union by position: index out of range".into())
            })?;
            let resolved_left = left.resolve_column_name(left_name)?;
            let resolved_right = right.resolve_column_name(right_name)?;
            let left_dtype = left.get_column_dtype(left_name).unwrap_or(DataType::Null);
            let right_dtype = right.get_column_dtype(right_name).unwrap_or(DataType::Null);
            let mut target = if left_dtype == DataType::Null {
                right_dtype.clone()
            } else if right_dtype == DataType::Null || left_dtype == right_dtype {
                left_dtype.clone()
            } else {
                find_common_type(&left_dtype, &right_dtype)?
            };
            if (left_dtype == DataType::String && is_numeric_public(&right_dtype))
                || (right_dtype == DataType::String && is_numeric_public(&left_dtype))
            {
                target = DataType::String;
            }
            let need_coerce = left_dtype != target || right_dtype != target;
            if debug_union {
                eprintln!(
                    "[union #1262] left_name={:?} right_name={:?} left_dtype={:?} right_dtype={:?} target={:?} need_coerce={}",
                    left_name, right_name, left_dtype, right_dtype, target, need_coerce
                );
            }
            let left_expr = if need_coerce {
                col(resolved_left.as_str()).cast(target.clone())
            } else {
                col(resolved_left.as_str())
            };
            let right_expr = if need_coerce {
                col(resolved_right.as_str()).cast(target)
            } else {
                col(resolved_right.as_str())
            };
            left_exprs.push(left_expr.alias(left_name.as_str()));
            right_exprs.push(right_expr.alias(left_name.as_str()));
        }
        (left_exprs, right_exprs)
    };

    let lf1 = left.lazy_frame().select(&left_exprs);
    let lf2 = right.lazy_frame().select(&right_exprs);
    // Collect then vstack so result schema is the coerced schema (Issue #1262: LazyFrame concat
    // can yield wrong schema for collect_schema(); eager vstack preserves cast result).
    let mut out = lf1.collect()?;
    let df2 = lf2.collect()?;
    if debug_union {
        eprintln!(
            "[union #1262] after lf1.collect() schema: {:?}",
            out.schema().iter_names_and_dtypes().collect::<Vec<_>>()
        );
    }
    out.vstack_mut(&df2)?;
    if debug_union {
        eprintln!(
            "[union #1262] after vstack schema: {:?}",
            out.schema().iter_names_and_dtypes().collect::<Vec<_>>()
        );
    }
    Ok(super::DataFrame::from_eager_with_options(
        out,
        case_sensitive,
    ))
}

/// Union by name: stack vertically, aligning columns by name.
/// When allow_missing_columns is true: result has all columns from both sides (missing filled with null).
/// When false: result has only left columns; right must have all left columns.
/// When same-named columns have different types (e.g. String vs Int64), coerces to a common type (PySpark parity #603).
pub fn union_by_name(
    left: &DataFrame,
    right: &DataFrame,
    allow_missing_columns: bool,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    use crate::type_coercion::find_common_type;
    use polars::prelude::*;

    let left_names = left.columns()?;
    let right_names = right.columns()?;
    let contains = |names: &[String], name: &str| -> bool {
        if case_sensitive {
            names.iter().any(|n| n.as_str() == name)
        } else {
            let name_lower = name.to_lowercase();
            names
                .iter()
                .any(|n| n.as_str().to_lowercase() == name_lower)
        }
    };
    let resolve = |names: &[String], name: &str| -> Option<String> {
        if case_sensitive {
            names.iter().find(|n| n.as_str() == name).cloned()
        } else {
            let name_lower = name.to_lowercase();
            names
                .iter()
                .find(|n| n.as_str().to_lowercase() == name_lower)
                .cloned()
        }
    };
    let all_columns: Vec<String> = if allow_missing_columns {
        let mut out = left_names.clone();
        for r in &right_names {
            if !contains(&out, r.as_str()) {
                out.push(r.clone());
            }
        }
        out
    } else {
        left_names.clone()
    };
    // Per-column common type for coercion when left/right types differ (#603).
    let mut left_exprs: Vec<Expr> = Vec::with_capacity(all_columns.len());
    let mut right_exprs: Vec<Expr> = Vec::with_capacity(all_columns.len());
    for c in &all_columns {
        let left_has = resolve(&left_names, c.as_str());
        let right_has = resolve(&right_names, c.as_str());
        let left_dtype = left_has.as_ref().and_then(|r| left.get_column_dtype(r));
        let right_dtype = right_has.as_ref().and_then(|r| right.get_column_dtype(r));
        // When both sides have the column and types differ, use shared coercion helper.
        if let (Some(l), Some(r)) = (&left_has, &right_has) {
            if let (Some(lt), Some(rt)) = (&left_dtype, &right_dtype) {
                if lt != rt {
                    let (le, re) = coerce_expr_pair(l, r, lt, rt, c).map_err(|e| {
                        PolarsError::ComputeError(
                            format!("union_by_name: column '{}' type coercion: {}", c, e).into(),
                        )
                    })?;
                    left_exprs.push(le);
                    right_exprs.push(re);
                    continue;
                }
            }
        }
        // #613 / #603: When both sides have dtypes, use a common type helper. When only one
        // side has a dtype, keep that dtype so columns that exist on only one side (and are
        // null on the other) preserve their natural type (e.g. Int64 id, Double aggregates).
        let common_dtype = match (&left_dtype, &right_dtype) {
            (Some(lt), Some(rt)) if lt != rt => find_common_type(lt, rt).map_err(|e| {
                PolarsError::ComputeError(
                    format!("union_by_name: column '{}' type coercion: {}", c, e).into(),
                )
            })?,
            (Some(lt), Some(_)) => lt.clone(),
            // One side unknown: keep the known dtype. If the known side is String, we still
            // get String here; if it's numeric, we avoid upcasting to String and changing
            // semantics for columns that are null on the other side.
            (Some(lt), None) | (None, Some(lt)) => lt.clone(),
            (None, None) => polars::prelude::DataType::Null,
        };
        let left_expr = match &left_has {
            Some(r) => col(r.as_str()).cast(common_dtype.clone()).alias(c.as_str()),
            None => polars::prelude::lit(polars::prelude::NULL)
                .cast(common_dtype.clone())
                .alias(c.as_str()),
        };
        left_exprs.push(left_expr);
        let right_expr = match &right_has {
            Some(r) => col(r.as_str()).cast(common_dtype.clone()).alias(c.as_str()),
            None if allow_missing_columns => polars::prelude::lit(polars::prelude::NULL)
                .cast(common_dtype)
                .alias(c.as_str()),
            None => {
                return Err(PolarsError::InvalidOperation(
                    format!(
                        "union_by_name: column '{}' missing in right DataFrame (allow_missing_columns=False)",
                        c
                    )
                    .into(),
                ));
            }
        };
        right_exprs.push(right_expr);
    }
    let lf1 = left.lazy_frame().select(&left_exprs);
    let lf2 = right.lazy_frame().select(&right_exprs);
    let out = polars::prelude::concat([lf1, lf2], UnionArgs::default())?;
    Ok(super::DataFrame::from_lazy_with_options(
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
    let subset_names: Option<Vec<String>> = subset
        .map(|cols| {
            cols.iter()
                .map(|s| df.resolve_column_name(s))
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?;
    let subset_selector: Option<Selector> = subset_names.map(|names| Selector::ByName {
        names: Arc::from(names.into_iter().map(PlSmallStr::from).collect::<Vec<_>>()),
        strict: false,
    });
    let lf = df
        .lazy_frame()
        .unique(subset_selector, UniqueKeepStrategy::First);
    Ok(super::DataFrame::from_lazy_with_options(lf, case_sensitive))
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
    let all_names = df.columns()?;
    let to_keep: Vec<Expr> = all_names
        .iter()
        .filter(|n| !resolved.iter().any(|r| r == n.as_str()))
        .map(|n| col(n.as_str()))
        .collect();
    let lf = df.lazy_frame().select(&to_keep);
    Ok(super::DataFrame::from_lazy_with_options(lf, case_sensitive))
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
    let cols: Vec<String> = match &subset {
        Some(c) => c
            .iter()
            .map(|n| df.resolve_column_name(n))
            .collect::<Result<Vec<_>, _>>()?,
        None => df.columns()?,
    };
    let col_exprs: Vec<Expr> = cols.iter().map(|c| col(c.as_str())).collect();
    let base_lf = df.lazy_frame();
    let lf = if let Some(n) = thresh {
        // Keep row if number of non-null in subset >= n
        let count_expr: Expr = col_exprs
            .iter()
            .map(|e| e.clone().is_not_null().cast(DataType::Int32))
            .fold(lit(0i32), |a, b| a + b);
        base_lf.filter(count_expr.gt_eq(lit(n as i32)))
    } else if how.eq_ignore_ascii_case("all") {
        // Drop only when all subset columns are null → keep when any is not null
        let any_not_null: Expr = col_exprs
            .into_iter()
            .map(|e| e.is_not_null())
            .fold(lit(false), |a, b| a.or(b));
        base_lf.filter(any_not_null)
    } else {
        // how == "any" (default): drop if any null in subset
        let subset_selector = Selector::ByName {
            names: Arc::from(
                cols.iter()
                    .map(|s| PlSmallStr::from(s.as_str()))
                    .collect::<Vec<_>>(),
            ),
            strict: false,
        };
        base_lf.drop_nulls(Some(subset_selector))
    };
    Ok(super::DataFrame::from_lazy_with_options(lf, case_sensitive))
}

/// Fill nulls with a literal expression. If subset is Some, only those columns are filled; else all.
/// Casts fill value to each column's dtype to preserve type (e.g. fill 0 -> int 0, not string "0").
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
                let fill = match df.get_column_dtype(resolved.as_str()) {
                    Some(dt) => value_expr.clone().cast(dt),
                    None => value_expr.clone(),
                };
                Ok(col(resolved.as_str()).fill_null(fill))
            })
            .collect::<Result<Vec<_>, PolarsError>>()?,
        None => df
            .columns()?
            .iter()
            .map(|n| {
                let fill = match df.get_column_dtype(n) {
                    Some(dt) => value_expr.clone().cast(dt),
                    None => value_expr.clone(),
                };
                col(n.as_str()).fill_null(fill)
            })
            .collect(),
    };
    let lf = df.lazy_frame().with_columns(exprs);
    Ok(super::DataFrame::from_lazy_with_options(lf, case_sensitive))
}

/// Limit: return first n rows.
pub fn limit(df: &DataFrame, n: usize, case_sensitive: bool) -> Result<DataFrame, PolarsError> {
    // limit is a transformation: slice(0, n) on lazy
    let lf = df.lazy_frame().slice(0, n as u32);
    Ok(super::DataFrame::from_lazy_with_options(lf, case_sensitive))
}

/// Rename a column (old_name -> new_name).
pub fn with_column_renamed(
    df: &DataFrame,
    old_name: &str,
    new_name: &str,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    match df.resolve_column_name(old_name) {
        Ok(resolved) => {
            let lf = df
                .lazy_frame()
                .rename([resolved.as_str()], [new_name], true);
            Ok(super::DataFrame::from_lazy_with_options(lf, case_sensitive))
        }
        // PySpark parity: renaming a non-existent column is a no-op.
        Err(PolarsError::ColumnNotFound(_)) => Ok(df.clone()),
        Err(e) => Err(e),
    }
}

/// Replace values in a column: where column == old_value, use new_value. PySpark replace (single column).
/// Coerces the equality so string–numeric comparisons (e.g. int column vs string literal) work like PySpark.
pub fn replace(
    df: &DataFrame,
    column_name: &str,
    old_value: Expr,
    new_value: Expr,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    use polars::prelude::*;
    let resolved = df.resolve_column_name(column_name)?;
    let eq_expr = col(resolved.as_str()).eq(old_value);
    let coerced_eq = df.coerce_string_numeric_comparisons(eq_expr)?;
    let repl = when(coerced_eq)
        .then(new_value)
        .otherwise(col(resolved.as_str()));
    let lf = df.lazy_frame().with_column(repl.alias(resolved.as_str()));
    Ok(super::DataFrame::from_lazy_with_options(lf, case_sensitive))
}

/// Cross join: cartesian product of two DataFrames. PySpark crossJoin.
/// Reorders columns so common names (e.g. dept_id) come first on each side. Renames right-side
/// columns that duplicate left names with _right suffix so result has unique column names (#1049).
pub fn cross_join(
    left: &DataFrame,
    right: &DataFrame,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    use polars::prelude::col;
    let left_names = left.columns()?;
    let right_names = right.columns()?;
    let right_set: std::collections::HashSet<&str> =
        right_names.iter().map(|s| s.as_str()).collect();
    let left_set: std::collections::HashSet<&str> = left_names.iter().map(|s| s.as_str()).collect();
    // Put columns that exist on both sides first (e.g. dept_id), then rest (PySpark cross order).
    let left_ordered = order_columns_common_first(&left_names, &right_set);
    let right_ordered = order_columns_common_first(&right_names, &left_set);
    let exprs_left: Vec<_> = left_ordered.iter().map(|s| col(*s)).collect();
    // Suffix right columns that duplicate left so result has unique names (parity fixture comparison).
    let exprs_right: Vec<_> = right_ordered
        .iter()
        .map(|s| {
            if left_set.contains(*s) {
                col(*s).alias(format!("{}_right", s))
            } else {
                col(*s)
            }
        })
        .collect();
    let lf_left = left.lazy_frame().select(&exprs_left);
    let lf_right = right.lazy_frame().select(&exprs_right);
    let out = lf_left.cross_join(lf_right, None);
    Ok(super::DataFrame::from_lazy_with_options(
        out,
        case_sensitive,
    ))
}

fn order_columns_common_first<'a>(
    names: &'a [String],
    other: &std::collections::HashSet<&str>,
) -> Vec<&'a str> {
    let mut common = Vec::new();
    let mut rest = Vec::new();
    for n in names {
        let s = n.as_str();
        if other.contains(s) {
            common.push(s);
        } else {
            rest.push(s);
        }
    }
    common.into_iter().chain(rest).collect()
}

/// Summary statistics (count, mean, std, min, max). PySpark describe.
/// Builds a summary DataFrame with a "summary" column (PySpark name) and one column per numeric input column.
pub fn describe(df: &DataFrame, case_sensitive: bool) -> Result<DataFrame, PolarsError> {
    use polars::prelude::*;
    let pl_df = df.collect_inner()?.as_ref().clone();
    let mut stat_values: Vec<Column> = Vec::new();
    for col in pl_df.columns() {
        let s = col.as_materialized_series();
        let dtype = s.dtype();
        if dtype.is_numeric() {
            let name = s.name().clone();
            let count = s.len() as i64 - s.null_count() as i64;
            let mean_f = s.mean().unwrap_or(f64::NAN);
            let std_f = s.std(1).unwrap_or(f64::NAN);
            let ca = series_as_f64_ca(s, "describe")?;
            let min_f = ca.min().unwrap_or(f64::NAN);
            let max_f = ca.max().unwrap_or(f64::NAN);
            // PySpark describe/summary returns string type for value columns.
            // Use "null" for NaN so JSON/parity consumers get proper null (not string "None").
            let is_float = matches!(dtype, DataType::Float64 | DataType::Float32);
            let count_s = count.to_string();
            let mean_s = if mean_f.is_nan() {
                "null".to_string()
            } else {
                format!("{:.1}", mean_f)
            };
            let std_s = if std_f.is_nan() {
                "null".to_string()
            } else {
                format!("{:.1}", std_f)
            };
            let min_s = if min_f.is_nan() {
                "null".to_string()
            } else if min_f.fract() == 0.0 && is_float {
                format!("{:.1}", min_f)
            } else if min_f.fract() == 0.0 {
                format!("{:.0}", min_f)
            } else {
                format!("{min_f}")
            };
            let max_s = if max_f.is_nan() {
                "null".to_string()
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
        let out_pl = polars::prelude::DataFrame::new_infer_height(vec![stat_col, empty_series])?;
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
    let out_pl = polars::prelude::DataFrame::new_infer_height(cols)?;
    Ok(super::DataFrame::from_polars_with_options(
        out_pl,
        case_sensitive,
    ))
}

/// Set difference: rows in left that are not in right (by all columns). PySpark subtract / except.
/// Aligns right column names to left (case-insensitive) so subtract works when casing differs.
pub fn subtract(
    left: &DataFrame,
    right: &DataFrame,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    use polars::prelude::*;
    let left_names = left.columns()?;
    let right_names = right.columns()?;
    let right_on: Vec<Expr> = left_names
        .iter()
        .map(|ln| {
            let resolved = if case_sensitive {
                right_names
                    .iter()
                    .find(|rn| rn.as_str() == ln.as_str())
                    .cloned()
                    .ok_or_else(|| {
                        PolarsError::ColumnNotFound(
                            format!(
                                "cannot resolve: subtract: column '{}' not found on right",
                                ln
                            )
                            .into(),
                        )
                    })?
            } else {
                let ln_lower = ln.to_lowercase();
                right_names
                    .iter()
                    .find(|rn| rn.to_lowercase() == ln_lower)
                    .cloned()
                    .ok_or_else(|| {
                        PolarsError::ColumnNotFound(
                            format!(
                                "cannot resolve: subtract: column '{}' not found on right",
                                ln
                            )
                            .into(),
                        )
                    })?
            };
            Ok(col(resolved.as_str()))
        })
        .collect::<Result<Vec<_>, PolarsError>>()?;
    let left_on: Vec<Expr> = left_names.iter().map(|n| col(n.as_str())).collect();
    let right_lf = right.lazy_frame();
    let left_lf = left.lazy_frame();
    let anti = left_lf.join(right_lf, left_on, right_on, JoinArgs::new(JoinType::Anti));
    Ok(super::DataFrame::from_lazy_with_options(
        anti,
        case_sensitive,
    ))
}

/// Set intersection: rows that appear in both DataFrames (by all columns). PySpark intersect.
/// Aligns right column names to left (case-insensitive) so intersect works when casing differs.
pub fn intersect(
    left: &DataFrame,
    right: &DataFrame,
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    use polars::prelude::*;
    let left_names = left.columns()?;
    let right_names = right.columns()?;
    let right_on: Vec<Expr> = left_names
        .iter()
        .map(|ln| {
            let resolved = if case_sensitive {
                right_names
                    .iter()
                    .find(|rn| rn.as_str() == ln.as_str())
                    .cloned()
                    .ok_or_else(|| {
                        PolarsError::ColumnNotFound(
                            format!(
                                "cannot resolve: intersect: column '{}' not found on right",
                                ln
                            )
                            .into(),
                        )
                    })?
            } else {
                let ln_lower = ln.to_lowercase();
                right_names
                    .iter()
                    .find(|rn| rn.to_lowercase() == ln_lower)
                    .cloned()
                    .ok_or_else(|| {
                        PolarsError::ColumnNotFound(
                            format!(
                                "cannot resolve: intersect: column '{}' not found on right",
                                ln
                            )
                            .into(),
                        )
                    })?
            };
            Ok(col(resolved.as_str()))
        })
        .collect::<Result<Vec<_>, PolarsError>>()?;
    let left_on: Vec<Expr> = left_names.iter().map(|n| col(n.as_str())).collect();
    let left_lf = left.lazy_frame();
    let right_lf = right.lazy_frame();
    let semi = left_lf
        .join(right_lf, left_on, right_on, JoinArgs::new(JoinType::Semi))
        .unique(None, UniqueKeepStrategy::First);
    Ok(super::DataFrame::from_lazy_with_options(
        semi,
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
    let pl = df.collect_inner()?;
    let n = pl.height();
    if n == 0 {
        return Ok(super::DataFrame::from_lazy_with_options(
            polars::prelude::DataFrame::empty().lazy(),
            case_sensitive,
        ));
    }
    let take_n = (n as f64 * fraction).round() as usize;
    let take_n = take_n.min(n).max(0);
    if take_n == 0 {
        return Ok(super::DataFrame::from_lazy_with_options(
            pl.as_ref().head(Some(0)).lazy(),
            case_sensitive,
        ));
    }
    let idx_series = Series::new("idx".into(), (0..n).map(|i| i as u32).collect::<Vec<_>>());
    let sampled_idx = idx_series.sample_n(take_n, with_replacement, true, seed)?;
    let idx_ca = sampled_idx
        .u32()
        .map_err(|_| PolarsError::ComputeError("sample: expected u32 indices".into()))?;
    let pl_df = pl.as_ref().take(idx_ca)?;
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
    let pl = df.collect_inner()?;
    let n = pl.height();
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
        let r: f64 = rng.r#gen();
        let bucket = cum
            .iter()
            .position(|&c| r < c)
            .unwrap_or(weights.len().saturating_sub(1));
        bucket_indices[bucket].push(i as u32);
    }
    let pl = pl.as_ref();
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
        return Ok(super::DataFrame::from_lazy_with_options(
            df.lazy_frame().slice(0, 0),
            case_sensitive,
        ));
    }
    let resolved = df.resolve_column_name(col_name)?;
    let mut parts = Vec::with_capacity(fractions.len());
    for (value_expr, frac) in fractions {
        let cond = col(resolved.as_str()).eq(value_expr.clone());
        let filtered = df.lazy_frame().filter(cond).collect()?;
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
        parts.push(sampled.collect_inner()?.as_ref().clone());
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
/// Uses limit(1) then collect so that orderBy (and other plan steps) are applied before taking
/// the first row (issue #579: first() after orderBy must return first in sort order, not storage order).
pub fn first(df: &DataFrame, case_sensitive: bool) -> Result<DataFrame, PolarsError> {
    let limited = limit(df, 1, case_sensitive)?;
    let pl_df = limited.collect_inner()?.as_ref().clone();
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
    let pl = df.collect_inner()?;
    let total = pl.height();
    let skip = total.saturating_sub(n);
    let pl_df = pl.as_ref().clone().slice(skip as i64, n);
    Ok(super::DataFrame::from_polars_with_options(
        pl_df,
        case_sensitive,
    ))
}

/// Whether the DataFrame has zero rows. PySpark isEmpty.
pub fn is_empty(df: &DataFrame) -> bool {
    df.count().map(|n| n == 0).unwrap_or(true)
}

/// Rename columns. PySpark toDF(*colNames). Names must match length of columns.
pub fn to_df(
    df: &DataFrame,
    names: &[&str],
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    let cols = df.columns()?;
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
    let pl_df = df.collect_inner()?;
    let mut pl_df = pl_df.as_ref().clone();
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
        AnyValue::StringOwned(v) => serde_json::Value::String(v.to_string()),
        _ => serde_json::Value::String(format!("{av:?}")),
    }
}

/// Convert a literal expression value to JSON for Python UDF executor (literal args).
pub(crate) fn literal_value_to_serde_value(
    lv: &polars::prelude::LiteralValue,
) -> Option<serde_json::Value> {
    lv.to_any_value().as_ref().map(any_value_to_serde_value)
}

/// Collect rows as JSON strings (one JSON object per row). PySpark toJSON.
pub fn to_json(df: &DataFrame) -> Result<Vec<String>, PolarsError> {
    use polars::prelude::*;
    let collected = df.collect_inner()?;
    let pl = collected.as_ref();
    let names = pl.get_column_names();
    let mut out = Vec::with_capacity(pl.height());
    for r in 0..pl.height() {
        let mut row = serde_json::Map::new();
        for (i, name) in names.iter().enumerate() {
            let col = pl
                .columns()
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

/// Parse simple "col op literal" expression for selectExpr (e.g. "age * 2", "salary + 100").
fn parse_simple_expr(df: &DataFrame, s: &str) -> Result<Option<Expr>, PolarsError> {
    let s = s.trim();
    for (op, kind) in [
        (" * ", "mul"),
        ("*", "mul"),
        (" + ", "add"),
        ("+", "add"),
        (" - ", "sub"),
        (" / ", "div"),
        ("/", "div"),
    ] {
        if let Some((a, b)) = s.split_once(op) {
            let a = a.trim();
            let b = b.trim();
            let (col_part, num_part, col_on_left) =
                if df.resolve_column_name(a).is_ok() && b.parse::<f64>().is_ok() {
                    (a, b, true)
                } else if df.resolve_column_name(b).is_ok() && a.parse::<f64>().is_ok() {
                    (b, a, false)
                } else {
                    continue;
                };
            let resolved = df.resolve_column_name(col_part)?;
            let col_expr = col(resolved.as_str());
            let num: f64 = num_part.parse().map_err(|_| {
                PolarsError::ComputeError(
                    format!("selectExpr: could not parse literal {num_part:?}").into(),
                )
            })?;
            let lit_expr = lit(num);
            let expr = match kind {
                "mul" => col_expr * lit_expr,
                "add" => col_expr + lit_expr,
                "sub" => {
                    if col_on_left {
                        col_expr - lit_expr
                    } else {
                        lit_expr - col_expr
                    }
                }
                "div" => col_expr / lit_expr,
                _ => continue,
            };
            return Ok(Some(expr));
        }
    }
    Ok(None)
}

/// Select by expression strings. Supports column names, "col as alias", and simple "col op num as alias". PySpark selectExpr.
pub fn select_expr(
    df: &DataFrame,
    exprs: &[String],
    case_sensitive: bool,
) -> Result<DataFrame, PolarsError> {
    let mut select_exprs: Vec<Expr> = Vec::new();
    for e in exprs {
        let e = e.trim();
        if let Some((left, right)) = e.split_once(" as ") {
            let left = left.trim();
            let alias = right.trim();
            if let Some(expr) = parse_simple_expr(df, left)? {
                select_exprs.push(expr.alias(alias));
            } else {
                let resolved = df.resolve_column_name(left)?;
                select_exprs.push(col(resolved.as_str()).alias(alias));
            }
        } else {
            let resolved = df.resolve_column_name(e)?;
            select_exprs.push(col(resolved.as_str()));
        }
    }
    select_with_exprs(df, select_exprs, case_sensitive)
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
    let names = df.columns()?;
    let matched: Vec<&str> = names
        .iter()
        .filter(|n| re.is_match(n))
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
    let pl = df.collect_inner()?.as_ref().clone();
    let mut current = super::DataFrame::from_polars_with_options(pl, case_sensitive);
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
    // Apply renames one by one; skip columns that do not exist (no-op for missing),
    // matching PySpark withColumnsRenamed behavior for non-existent columns.
    let mut lf = df.lazy_frame();
    let mut applied_any = false;
    for (old_name, new_name) in renames {
        match df.resolve_column_name(old_name) {
            Ok(resolved) => {
                lf = lf.rename([resolved.as_str()], [new_name.as_str()], true);
                applied_any = true;
            }
            Err(PolarsError::ColumnNotFound(_)) => {
                // Non-existent column: leave DataFrame unchanged for this entry.
                continue;
            }
            Err(e) => return Err(e),
        }
    }
    if !applied_any {
        return Ok(df.clone());
    }
    Ok(super::DataFrame::from_lazy_with_options(lf, case_sensitive))
}

/// NA sub-API builder. PySpark df.na().fill(...) / .drop(...).
pub struct DataFrameNa<'a> {
    pub(crate) df: &'a DataFrame,
}

impl<'a> DataFrameNa<'a> {
    /// Create from a reference to a DataFrame (for root crate wrapper).
    pub fn new(df: &'a DataFrame) -> Self {
        DataFrameNa { df }
    }

    /// Fill nulls with the given value. PySpark na.fill(value, subset=...).
    pub fn fill(&self, value: Expr, subset: Option<Vec<&str>>) -> Result<DataFrame, PolarsError> {
        fillna(self.df, value, subset, self.df.case_sensitive)
    }

    /// Replace values in columns. PySpark na.replace(to_replace, value, subset=None).
    pub fn replace(
        &self,
        old_value: Expr,
        new_value: Expr,
        subset: Option<Vec<&str>>,
    ) -> Result<DataFrame, PolarsError> {
        let cols: Vec<String> = match &subset {
            Some(s) => s.iter().map(|x| (*x).to_string()).collect(),
            None => self.df.columns()?,
        };
        let mut result = self.df.clone();
        for col_name in &cols {
            result = replace(
                &result,
                col_name.as_str(),
                old_value.clone(),
                new_value.clone(),
                self.df.case_sensitive,
            )?;
        }
        Ok(result)
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
    let lf = df.lazy_frame().slice(n as i64, u32::MAX);
    Ok(super::DataFrame::from_lazy_with_options(lf, case_sensitive))
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
        return Ok(super::DataFrame::from_lazy_with_options(
            df.lazy_frame().slice(0, 0),
            case_sensitive,
        ));
    }
    let support = support.clamp(1e-4, 1.0);
    let collected = df.collect_inner()?;
    let pl_df = collected.as_ref();
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
            polars::prelude::DataFrame::new_infer_height(out)?,
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
    let out_df = polars::prelude::DataFrame::new_infer_height(out_series)?;
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
            polars::prelude::DataFrame::new_infer_height(vec![
                Series::new("quantile".into(), Vec::<f64>::new()).into(),
            ])?,
            case_sensitive,
        ));
    }
    let resolved = df.resolve_column_name(column)?;
    let collected = df.collect_inner()?;
    let s = collected
        .column(resolved.as_str())?
        .as_series()
        .ok_or_else(|| PolarsError::ComputeError("approx_quantile: column not a series".into()))?
        .clone();
    let ca = series_as_f64_ca(&s, "approx_quantile")?;
    let mut quantiles = Vec::with_capacity(probabilities.len());
    for &p in probabilities {
        let q = ca.quantile(p, QuantileMethod::Linear)?;
        quantiles.push(q.unwrap_or(f64::NAN));
    }
    let out_df = polars::prelude::DataFrame::new_infer_height(vec![
        Series::new("quantile".into(), quantiles).into(),
    ])?;
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
    let collected = df.collect_inner()?;
    let pl_df = collected.as_ref();
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
    let collected = df.collect_inner()?;
    let pl_df = collected.as_ref();
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
        part.with_column(var_series.into())?;
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
    use super::{
        SelectItem, distinct, drop, dropna, filter, first, head, limit, offset, order_by,
        select_items, union, union_by_name, with_column,
    };
    use crate::column::Column;
    use crate::functions;
    use crate::{DataFrame, SparkSession};
    use polars::prelude::{col, concat_str, lit};
    use serde_json::json;

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

    /// Issue #579: first() after orderBy must return first row in sort order, not storage order.
    #[test]
    fn first_after_order_by_returns_first_in_sort_order() {
        use polars::prelude::df;
        let spark = SparkSession::builder()
            .app_name("transform_tests")
            .get_or_create();
        let pl = df![
            "name" => ["Charlie", "Alice", "Bob"],
            "value" => [3i64, 1i64, 2i64],
        ]
        .unwrap();
        let df = spark.create_dataframe_from_polars(pl);
        let ordered = order_by(&df, vec!["value"], vec![true], false).unwrap();
        let one = first(&ordered, false).unwrap();
        let collected = one.collect_inner().unwrap();
        let name_series = collected.column("name").unwrap();
        let first_name = name_series.str().unwrap().get(0).unwrap();
        assert_eq!(
            first_name, "Alice",
            "first() after orderBy(value) must return row with min value (Alice=1), not first in storage (Charlie)"
        );
    }

    /// Issue #1253: to_timestamp accepts StringType, TimestampType, IntegerType, LongType, DateType, DoubleType (PySpark).
    #[test]
    fn with_column_to_timestamp_accepts_multiple_types() {
        let spark = SparkSession::builder()
            .app_name("to_timestamp_types_test")
            .get_or_create();

        // IntegerType (int) -> unix seconds
        let rows_int = vec![vec![json!(1672574400)]];
        let schema_int = vec![("unix_ts".to_string(), "int".to_string())];
        let df_int = spark
            .create_dataframe_from_rows(rows_int, schema_int, false, false)
            .unwrap();
        let col_ts = functions::to_timestamp(&df_int.column("unix_ts").unwrap(), None).unwrap();
        let out_int = with_column(&df_int, "parsed", &col_ts, false).unwrap();
        let rows_out = out_int.collect_as_json_rows().unwrap();
        assert_eq!(rows_out.len(), 1);
        assert!(rows_out[0].get("parsed").and_then(|v| v.as_str()).is_some());

        // LongType (long) -> unix seconds
        let rows_long = vec![vec![json!(1672574400)]];
        let schema_long = vec![("unix_ts".to_string(), "long".to_string())];
        let df_long = spark
            .create_dataframe_from_rows(rows_long, schema_long, false, false)
            .unwrap();
        let col_ts_long =
            functions::to_timestamp(&df_long.column("unix_ts").unwrap(), None).unwrap();
        let out_long = with_column(&df_long, "parsed", &col_ts_long, false).unwrap();
        assert_eq!(out_long.collect_as_json_rows().unwrap().len(), 1);

        // DateType (date) -> date to timestamp
        let rows_date = vec![vec![json!("2023-01-01")]];
        let schema_date = vec![("date_col".to_string(), "date".to_string())];
        let df_date = spark
            .create_dataframe_from_rows(rows_date, schema_date, false, false)
            .unwrap();
        let col_ts_date =
            functions::to_timestamp(&df_date.column("date_col").unwrap(), None).unwrap();
        let out_date = with_column(&df_date, "parsed", &col_ts_date, false).unwrap();
        assert_eq!(out_date.collect_as_json_rows().unwrap().len(), 1);

        // DoubleType (double) -> unix seconds with fraction
        let rows_double = vec![vec![json!(1672574400.5)]];
        let schema_double = vec![("unix_ts".to_string(), "double".to_string())];
        let df_double = spark
            .create_dataframe_from_rows(rows_double, schema_double, false, false)
            .unwrap();
        let col_ts_double =
            functions::to_timestamp(&df_double.column("unix_ts").unwrap(), None).unwrap();
        let out_double = with_column(&df_double, "parsed", &col_ts_double, false).unwrap();
        assert_eq!(out_double.collect_as_json_rows().unwrap().len(), 1);
    }

    /// Issue #168: to_timestamp(regexp_replace(col, r"\.\d+", "").cast("string"), "yyyy-MM-dd'T'HH:mm:ss")
    /// must yield null (PySpark: pattern in that context doesn't match fractional seconds).
    #[test]
    fn issue_168_to_timestamp_after_regexp_replace_cast_string_yields_nulls() {
        use polars::prelude::{NamedFrom, Series};
        let spark = SparkSession::builder()
            .app_name("issue_168_test")
            .get_or_create();
        // ISO strings with fractional seconds (like datetime.isoformat())
        let impression_id = Series::new("impression_id".into(), &["IMP-001", "IMP-002", "IMP-003"]);
        let impression_date = Series::new(
            "impression_date".into(),
            &[
                "2025-03-07T19:34:56.123456",
                "2025-03-07T18:00:00.0",
                "2025-03-06T12:00:00.999",
            ],
        );
        let pl = polars::prelude::DataFrame::new_infer_height(vec![
            impression_id.into(),
            impression_date.into(),
        ])
        .unwrap();
        let df = spark.create_dataframe_from_polars(pl);
        let c = df.column("impression_date").unwrap();
        let replaced = functions::regexp_replace(&c, r"\.\d+", "");
        let casted = replaced.cast_to("string").unwrap();
        let ts_col =
            functions::to_timestamp(&casted, Some("yyyy-MM-dd'T'HH:mm:ss")).unwrap();
        let silver = with_column(&df, "impression_date_parsed", &ts_col, false).unwrap();
        let selected = select_items(
            &silver,
            vec![
                SelectItem::ColumnName("impression_id"),
                SelectItem::ColumnName("impression_date_parsed"),
            ],
            false,
        )
        .unwrap();
        let cond = functions::col("impression_id")
            .is_not_null()
            .and_(&functions::col("impression_date_parsed").is_not_null());
        let valid = filter(&selected, cond.into_expr(), false).unwrap();
        let count = valid.count().unwrap();
        assert_eq!(count, 0, "issue #168: parsed values must be null so valid count is 0");
    }

    /// Issue #1054 / #293: with_column(explode(col)) must expand rows and preserve original list column.
    #[test]
    fn with_column_explode_adds_column_and_expands_rows() {
        use polars::chunked_array::builder::ListStringChunkedBuilder;
        use polars::prelude::{IntoSeries, ListBuilderTrait, NamedFrom, Series};
        let spark = SparkSession::builder()
            .app_name("with_column_explode_test")
            .get_or_create();
        let names = Series::new("Name".into(), &["Alice", "Bob", "Charlie"]);
        let mut list_builder = ListStringChunkedBuilder::new("Value".into(), 3, 16);
        list_builder.append_values_iter(["1", "2"].iter().copied());
        list_builder.append_values_iter(["2", "3"].iter().copied());
        list_builder.append_values_iter(["4", "5"].iter().copied());
        let value_series = list_builder.finish().into_series();
        let pl =
            polars::prelude::DataFrame::new_infer_height(vec![names.into(), value_series.into()])
                .unwrap();
        let df = spark.create_dataframe_from_polars(pl);
        let col_explode = functions::explode(&df.column("Value").unwrap());
        let out = with_column(&df, "ExplodedValue", &col_explode, false).unwrap();
        assert_eq!(
            out.count().unwrap(),
            6,
            "explode should produce 6 rows (2+2+2)"
        );
        let cols = out.columns().unwrap();
        assert!(cols.iter().any(|c| c == "Name"));
        assert!(cols.iter().any(|c| c == "Value"));
        assert!(cols.iter().any(|c| c == "ExplodedValue"));
    }

    /// Issue #1111: with_column(map_col[col("Value")]) must resolve "Value" so map lookup works.
    #[test]
    fn with_column_map_get_with_column_key_resolves_key() {
        use polars::prelude::{NamedFrom, Series};
        let spark = SparkSession::builder()
            .app_name("map_get_test")
            .get_or_create();
        let names = Series::new("Name".into(), &["Alice", "Bob"]);
        let values = Series::new("Value".into(), [1i64, 3i64]);
        let pl = polars::prelude::DataFrame::new_infer_height(vec![names.into(), values.into()])
            .unwrap();
        let df = spark.create_dataframe_from_polars(pl);
        let mapping = functions::create_map(&[
            &functions::lit_i64(1),
            &functions::lit_str("Small"),
            &functions::lit_i64(2),
            &functions::lit_str("Medium"),
            &functions::lit_i64(3),
            &functions::lit_str("Large"),
        ])
        .unwrap();
        let size_col = mapping.get(&functions::col("Value"));
        let out = with_column(&df, "Size", &size_col, false).unwrap();
        let rows = out.collect_as_json_rows().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get("Size").and_then(|v| v.as_str()), Some("Small"));
        assert_eq!(rows[1].get("Size").and_then(|v| v.as_str()), Some("Large"));
    }

    /// Issue #1054: select(Name, Value, explode(Value).alias("ExplodedValue")) must preserve list column and expand rows.
    #[test]
    fn select_with_explode_alias_preserves_list_column() {
        use polars::chunked_array::builder::ListStringChunkedBuilder;
        use polars::prelude::{ExplodeOptions, IntoSeries, ListBuilderTrait, NamedFrom, Series};
        let spark = SparkSession::builder()
            .app_name("select_explode_test")
            .get_or_create();
        let names = Series::new("Name".into(), &["Alice", "Bob"]);
        let mut list_builder = ListStringChunkedBuilder::new("Value".into(), 2, 8);
        list_builder.append_values_iter(["1", "2"].iter().copied());
        list_builder.append_values_iter(["2", "3"].iter().copied());
        let value_series = list_builder.finish().into_series();
        let pl =
            polars::prelude::DataFrame::new_infer_height(vec![names.into(), value_series.into()])
                .unwrap();
        let df = spark.create_dataframe_from_polars(pl);
        let explode_expr = polars::prelude::col("Value")
            .explode(ExplodeOptions {
                empty_as_null: false,
                keep_nulls: false,
            })
            .alias("ExplodedValue");
        let items = vec![
            SelectItem::ColumnName("Name"),
            SelectItem::ColumnName("Value"),
            SelectItem::Expr(explode_expr),
        ];
        let out = select_items(&df, items, false).unwrap();
        assert_eq!(
            out.count().unwrap(),
            4,
            "select with explode should produce 4 rows (2+2)"
        );
        let cols = out.columns().unwrap();
        assert!(cols.iter().any(|c| c == "Name"));
        assert!(cols.iter().any(|c| c == "Value"));
        assert!(cols.iter().any(|c| c == "ExplodedValue"));
    }

    /// Issue #297: selecting an ambiguous column name with different casing (e.g. "NaMe")
    /// after a join where both "name" and "NAME" exist should:
    /// - pick the first matching physical column (left side of the join), and
    /// - expose it under the requested spelling ("NaMe").
    ///   #1267: select("dept", "salary", row_number().over(w)) must preserve dept/salary values.
    #[test]
    fn select_items_with_window_preserves_column_values() {
        let spark = SparkSession::builder()
            .app_name("select_window_1267")
            .get_or_create();
        let rows = vec![vec![json!("A"), json!(100)], vec![json!("A"), json!(200)]];
        let schema = vec![
            ("dept".to_string(), "string".to_string()),
            ("salary".to_string(), "bigint".to_string()),
        ];
        let df = spark
            .create_dataframe_from_rows(rows, schema, false, false)
            .unwrap();
        let rank_col = Column::row_number_over(&["dept"], &["salary".to_string()]).unwrap();
        let rank_expr = rank_col.into_expr().alias("rn");
        let items = vec![
            SelectItem::ColumnName("dept"),
            SelectItem::ColumnName("salary"),
            SelectItem::Expr(rank_expr),
        ];
        let out = select_items(&df, items, false).unwrap();
        let rows_out = out.collect_as_json_rows().unwrap();
        assert_eq!(rows_out.len(), 2, "expected 2 rows");
        let first = &rows_out[0];
        assert_eq!(
            first.get("dept").and_then(|v| v.as_str()),
            Some("A"),
            "first row dept must be A (#1267)"
        );
        assert_eq!(
            first.get("salary").and_then(|v| v.as_i64()),
            Some(100),
            "first row salary must be 100"
        );
        assert_eq!(
            first.get("rn").and_then(|v| v.as_i64()),
            Some(1),
            "first row rn must be 1"
        );
    }

    #[test]
    fn select_items_ambiguous_case_prefers_first_match_and_uses_requested_name() {
        use polars::prelude::df;

        let spark = SparkSession::builder()
            .app_name("select_ambiguous_case")
            .get_or_create();
        let left_pl = df!("name" => &["Alice"], "value" => &[1i64]).unwrap();
        let right_pl = df!("NAME" => &["Bob"], "other" => &[2i64]).unwrap();
        let left = spark.create_dataframe_from_polars(left_pl);
        let right = spark.create_dataframe_from_polars(right_pl);
        // Join on "Name" so both "name" and "NAME" columns are present after the join.
        let joined = left
            .join(&right, vec!["Name"], crate::dataframe::JoinType::Left)
            .unwrap();

        let out = select_items(&joined, vec![SelectItem::ColumnName("NaMe")], false).unwrap();
        let cols = out.columns().unwrap();
        assert!(
            cols.contains(&"NaMe".to_string()),
            "ambiguous select must expose requested spelling"
        );

        let pl = out.collect().unwrap();
        let name_series = pl.column("NaMe").unwrap().str().unwrap();
        assert_eq!(name_series.get(0).unwrap(), "Alice");
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

    /// #681: union (same column order) with Int64 vs String in same position coerces to common type.
    #[test]
    fn union_coerces_int_str_same_position() {
        use polars::prelude::df;

        let spark = SparkSession::builder()
            .app_name("transform_tests")
            .get_or_create();
        let left_pl = df!("id" => &[1i64, 2i64], "name" => &["a", "b"]).unwrap();
        let right_pl = df!("id" => &["3", "4"], "name" => &["c", "d"]).unwrap();
        let left = spark.create_dataframe_from_polars(left_pl);
        let right = spark.create_dataframe_from_polars(right_pl);
        let out = union(&left, &right, false).expect("#681: union must coerce id Int64 vs String");
        assert_eq!(out.count().unwrap(), 4);
        let cols = out.columns().unwrap();
        assert_eq!(cols.len(), 2);
        assert!(cols.contains(&"id".to_string()));
        assert!(cols.contains(&"name".to_string()));
        // #1262: collected rows must have id as string (PySpark union coerces numeric+string to string).
        let (_names, rows, schema) = out.collect_as_json_rows_with_names().unwrap();
        let id_field = schema.fields().iter().find(|f| f.name == "id").unwrap();
        assert!(matches!(
            id_field.data_type,
            robin_sparkless_core::DataType::String
        ));
        for row in &rows {
            let id_val = row.get("id").unwrap();
            assert!(
                matches!(id_val, serde_json::Value::String(_)),
                "id should be string, got {id_val:?}"
            );
        }
    }

    /// Union with same column names in different order: right reordered to left's order (PR1).
    #[test]
    fn union_same_names_different_order() {
        use polars::prelude::df;

        let spark = SparkSession::builder()
            .app_name("transform_tests")
            .get_or_create();
        let left_pl = df!("a" => &[1i64, 2i64], "b" => &["x", "y"]).unwrap();
        let right_pl = df!("b" => &["p", "q"], "a" => &[3i64, 4i64]).unwrap();
        let left = spark.create_dataframe_from_polars(left_pl);
        let right = spark.create_dataframe_from_polars(right_pl);
        let out = union(&left, &right, false).expect("union by name set should reorder right");
        assert_eq!(out.count().unwrap(), 4);
        let cols = out.columns().unwrap();
        assert_eq!(cols[0], "a");
        assert_eq!(cols[1], "b");
    }

    /// Issue #603: unionByName with same-named columns of different types (e.g. id Int vs id String) must coerce and succeed.
    #[test]
    fn union_by_name_coerces_different_column_types() {
        use polars::prelude::df;

        let spark = SparkSession::builder()
            .app_name("transform_tests")
            .get_or_create();
        let left_pl = df!("id" => &[1i64], "name" => &["a"]).unwrap();
        let left = spark.create_dataframe_from_polars(left_pl);
        let schema = vec![
            ("id".to_string(), "string".to_string()),
            ("name".to_string(), "string".to_string()),
        ];
        let right = spark
            .create_dataframe_from_rows(vec![vec![json!("2"), json!("b")]], schema, false, false)
            .unwrap();
        let out = union_by_name(&left, &right, true, false)
            .expect("issue #603: union_by_name must coerce id Int64 vs String");
        assert_eq!(out.count().unwrap(), 2);
    }

    #[test]
    fn dropna_all_columns() {
        let df = test_df();
        let out = dropna(&df, None, "any", None, false).unwrap();
        assert_eq!(out.count().unwrap(), 3);
    }

    /// #722: na.drop(subset=["NonExistentColumn"]) must raise (column not found).
    #[test]
    fn dropna_invalid_subset_column_raises() {
        let df = test_df();
        let result = dropna(&df, Some(vec!["NonExistentColumn"]), "any", None, false);
        match &result {
            Err(e) => assert!(
                e.to_string().to_lowercase().contains("not found")
                    || e.to_string().to_lowercase().contains("column"),
                "expected column-not-found error, got: {}",
                e
            ),
            Ok(_) => panic!("expected error for dropna with non-existent subset column"),
        }
    }

    /// #1105: filter(col("full_id") == "rec1_cust1") after withColumn(full_id) must return 1 row.
    /// Condition is passed as Column.into_expr() (Alias(BinaryExpr(...))); expr_coerce_to_boolean must not coerce comparison operands.
    #[test]
    fn filter_string_equality_after_with_column() {
        let spark = SparkSession::builder()
            .app_name("filter_string_eq_test")
            .get_or_create();
        let pl = polars::prelude::df!["record_id" => &["rec1"], "cust_id" => &["cust1"]].unwrap();
        let df = spark.create_dataframe_from_polars(pl);
        let transformed = df
            .with_column_renamed("record_id", "id")
            .unwrap()
            .with_column_renamed("cust_id", "customer_id")
            .unwrap();
        let full_id_expr = concat_str(&[col("id"), col("customer_id")], "_", false);
        let transformed = with_column(
            &transformed,
            "full_id",
            &Column::from_expr(full_id_expr, None),
            false,
        )
        .unwrap();
        let transformed = select_items(
            &transformed,
            vec![
                SelectItem::Expr(col("id")),
                SelectItem::Expr(col("customer_id")),
                SelectItem::Expr(col("full_id")),
            ],
            false,
        )
        .unwrap();
        // Simulate Python: col("full_id") == "rec1_cust1" -> Column.into_expr() is Alias(BinaryExpr(...))
        let condition = Column::new("full_id".to_string())
            .eq(lit("rec1_cust1"))
            .into_expr();
        let result = filter(&transformed, condition, false).unwrap();
        assert_eq!(
            result.count().unwrap(),
            1,
            "#1105: filter on string column must return 1 row"
        );
    }
}

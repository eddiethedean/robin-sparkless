use polars::prelude::{Expr, LazyFrame};
use std::collections::HashSet;

/// Minimal logical plan representation used for higher-level optimizations (e.g. filter pushdown
/// across projections) before compiling to a Polars `LazyFrame`.
#[derive(Clone)]
pub enum LogicalPlan {
    /// Base node wrapping an existing Polars `LazyFrame`.
    Base(LazyFrame),
    /// Projection of a set of expressions (typically column references or aliases) on top of an input.
    Project {
        exprs: Vec<Expr>,
        input: Box<LogicalPlan>,
    },
    /// Filter with a predicate expression on top of an input.
    Filter {
        predicate: Expr,
        input: Box<LogicalPlan>,
    },
}

impl LogicalPlan {
    /// Convenience constructor for a base plan.
    pub fn from_lazy(lf: LazyFrame) -> Self {
        LogicalPlan::Base(lf)
    }

    /// Compile this logical plan into a Polars `LazyFrame` without changing semantics.
    pub fn to_lazy(&self) -> LazyFrame {
        match self {
            LogicalPlan::Base(lf) => lf.clone(),
            LogicalPlan::Project { exprs, input } => {
                let child = input.to_lazy();
                child.select(exprs)
            }
            LogicalPlan::Filter { predicate, input } => {
                let child = input.to_lazy();
                child.filter(predicate.clone())
            }
        }
    }

    /// Optimize this logical plan by applying simple rewrite rules, such as pushing
    /// filters below projections when safe.
    pub fn optimize(&self) -> LogicalPlan {
        match self {
            LogicalPlan::Base(lf) => LogicalPlan::Base(lf.clone()),
            LogicalPlan::Project { exprs, input } => LogicalPlan::Project {
                exprs: exprs.clone(),
                input: Box::new(input.optimize()),
            },
            LogicalPlan::Filter { predicate, input } => {
                let optimized_input = input.optimize();
                // Try filter–project reordering on the optimized child.
                if let LogicalPlan::Project {
                    exprs,
                    input: project_child,
                } = optimized_input
                {
                    if let Some(rewritten) =
                        try_rewrite_filter_over_project(predicate, &exprs, &project_child)
                    {
                        return rewritten;
                    }
                    LogicalPlan::Filter {
                        predicate: predicate.clone(),
                        input: Box::new(LogicalPlan::Project { exprs, input: project_child }),
                    }
                } else {
                    LogicalPlan::Filter {
                        predicate: predicate.clone(),
                        input: Box::new(optimized_input),
                    }
                }
            }
        }
    }
}

/// Attempt to rewrite Filter(Project(child)) into Project(Filter(child)) when:
/// - The projection is a simple list of column expressions (no aliases/complex exprs).
/// - The filter predicate references at least one column that is not in the projected list
///   (i.e. a \"dropped\" column).
fn try_rewrite_filter_over_project(
    predicate: &Expr,
    project_exprs: &[Expr],
    project_child: &LogicalPlan,
) -> Option<LogicalPlan> {
    // Extract simple projected column names.
    let mut projected_cols: Vec<String> = Vec::with_capacity(project_exprs.len());
    for e in project_exprs {
        match e {
            Expr::Column(name) => projected_cols.push(name.as_str().to_string()),
            // When projection contains aliases or non-column expressions, skip rewrite
            // to avoid changing semantics.
            _ => return None,
        }
    }
    let projected_set: HashSet<String> = projected_cols.iter().cloned().collect();

    // Collect referenced columns in the predicate.
    let referenced_cols = expr_referenced_columns(predicate);
    if referenced_cols.is_empty() {
        return None;
    }

    // Only rewrite when at least one referenced column is not in the projected set,
    // indicating a predicate on a \"dropped\" column (issue #1135).
    let has_dropped_ref = referenced_cols
        .iter()
        .any(|c| !projected_set.contains(c.as_str()));
    if !has_dropped_ref {
        return None;
    }

    let new_filter = LogicalPlan::Filter {
        predicate: predicate.clone(),
        input: Box::new(project_child.clone()),
    };
    Some(LogicalPlan::Project {
        exprs: project_exprs.to_vec(),
        input: Box::new(new_filter),
    })
}

/// Returns the set of column names referenced in an expression tree.
fn expr_referenced_columns(expr: &Expr) -> HashSet<String> {
    let mut refs = HashSet::new();
    let _ = expr.clone().try_map_expr(|e| {
        if let Expr::Column(name) = &e {
            refs.insert(name.as_str().to_string());
        }
        Ok(e)
    });
    refs
}


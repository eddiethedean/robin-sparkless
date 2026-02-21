//! Sort order specification for orderBy/sort.

use crate::column::Column;
use polars::prelude::Expr;

/// Sort order specification for use in orderBy/sort. Holds expr + direction + null placement.
#[derive(Debug, Clone)]
pub struct SortOrder {
    pub(crate) expr: Expr,
    pub descending: bool,
    pub nulls_last: bool,
}

impl SortOrder {
    pub fn expr(&self) -> &Expr {
        &self.expr
    }
}

/// Ascending sort, nulls first (Spark default for ASC).
pub fn asc(column: &Column) -> SortOrder {
    SortOrder {
        expr: column.expr().clone(),
        descending: false,
        nulls_last: false,
    }
}

/// Ascending sort, nulls first.
pub fn asc_nulls_first(column: &Column) -> SortOrder {
    SortOrder {
        expr: column.expr().clone(),
        descending: false,
        nulls_last: false,
    }
}

/// Ascending sort, nulls last.
pub fn asc_nulls_last(column: &Column) -> SortOrder {
    SortOrder {
        expr: column.expr().clone(),
        descending: false,
        nulls_last: true,
    }
}

/// Descending sort, nulls last (Spark default for DESC).
pub fn desc(column: &Column) -> SortOrder {
    SortOrder {
        expr: column.expr().clone(),
        descending: true,
        nulls_last: true,
    }
}

/// Descending sort, nulls first.
pub fn desc_nulls_first(column: &Column) -> SortOrder {
    SortOrder {
        expr: column.expr().clone(),
        descending: true,
        nulls_last: false,
    }
}

/// Descending sort, nulls last.
pub fn desc_nulls_last(column: &Column) -> SortOrder {
    SortOrder {
        expr: column.expr().clone(),
        descending: true,
        nulls_last: true,
    }
}

//! Portable logical operations, JSON decoding, and shared semantic helpers.

mod json;
mod model;
mod names;

pub use model::{
    BinaryInput, ExpressionKind, GroupInput, LogicalPlan, OperationKind, PlanExpression,
    PlanOperation, PlanPayload,
};
pub use names::{
    common_numeric_type, matching_indices, matching_indices_ascii_case_insensitive, resolve_name,
};

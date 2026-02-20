//! Join type for engine-agnostic join operations.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    LeftAnti,
    LeftSemi,
    Cross,
}

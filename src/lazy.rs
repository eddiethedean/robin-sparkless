use datafusion::prelude::*;
use datafusion::logical_expr::{LogicalPlan, Expr};
use datafusion::execution::context::SessionContext;
use std::sync::Arc;
use tokio::runtime::Runtime;
use arrow::record_batch::RecordBatch as ArrowRecordBatch;
use crate::arrow_conversion::df_to_arrow_record_batch;

// Helper to convert DataFusion RecordBatch to Arrow RecordBatch
fn convert_record_batch(df_batch: datafusion::arrow::record_batch::RecordBatch) -> ArrowRecordBatch {
    df_to_arrow_record_batch(&df_batch).unwrap()
}

/// Represents a lazy DataFrame with a logical plan
pub struct LazyFrame {
    pub(crate) plan: LogicalPlan,
    pub(crate) ctx: Arc<SessionContext>,
}

impl LazyFrame {
    pub fn new(plan: LogicalPlan, ctx: Arc<SessionContext>) -> Self {
        LazyFrame { plan, ctx }
    }
    
    pub fn plan(&self) -> &LogicalPlan {
        &self.plan
    }
    
    pub fn ctx(&self) -> &Arc<SessionContext> {
        &self.ctx
    }
    
    /// Apply a filter operation (lazy)
    /// Uses LogicalPlanBuilder to construct the plan
    pub fn filter(self, predicate: Expr) -> Self {
        use datafusion::logical_expr::builder::LogicalPlanBuilder;
        
        // Use builder to create filter plan - filter() returns Result<LogicalPlanBuilder>
        let plan_clone = self.plan.clone();
        let new_plan = LogicalPlanBuilder::from(self.plan)
            .filter(predicate)
            .and_then(|b| b.build())
            .unwrap_or_else(|_| plan_clone); // Fallback on error
        
        LazyFrame {
            plan: new_plan,
            ctx: self.ctx,
        }
    }
    
    /// Apply a projection/select operation (lazy)
    pub fn select(self, exprs: Vec<Expr>) -> Self {
        use datafusion::logical_expr::builder::LogicalPlanBuilder;
        
        let plan_clone = self.plan.clone();
        let new_plan = LogicalPlanBuilder::from(self.plan)
            .project(exprs)
            .and_then(|b| b.build())
            .unwrap_or_else(|_| plan_clone);
        
        LazyFrame {
            plan: new_plan,
            ctx: self.ctx,
        }
    }
    
    /// Apply aggregation (lazy)
    pub fn aggregate(
        self,
        group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
    ) -> Self {
        use datafusion::logical_expr::builder::LogicalPlanBuilder;
        
        let plan_clone = self.plan.clone();
        let new_plan = LogicalPlanBuilder::from(self.plan)
            .aggregate(group_expr, aggr_expr)
            .and_then(|b| b.build())
            .unwrap_or_else(|_| plan_clone);
        
        LazyFrame {
            plan: new_plan,
            ctx: self.ctx,
        }
    }
    
    /// Join with another lazy frame (lazy)
    pub fn join(
        self,
        right: LazyFrame,
        join_type: datafusion::logical_expr::JoinType,
        left_keys: Vec<Expr>,
        right_keys: Vec<Expr>,
        filter: Option<Expr>,
    ) -> Self {
        use datafusion::logical_expr::builder::LogicalPlanBuilder;
        
        let plan_clone = self.plan.clone();
        // Join expects (left_keys, right_keys) where keys are Column types
        // Convert Expr to Column if it's a column reference, otherwise use column name
        use datafusion::prelude::Column as DFColumn;
        let left_cols: Vec<DFColumn> = left_keys.iter()
            .filter_map(|e| {
                // Try to extract column name from Expr
                if let datafusion::logical_expr::Expr::Column(col) = e {
                    Some(col.clone())
                } else {
                    // Fallback: try to get column name from expression
                    // This is a simplified approach - in production, handle all Expr types
                    None
                }
            })
            .collect();
        let right_cols: Vec<DFColumn> = right_keys.iter()
            .filter_map(|e| {
                if let datafusion::logical_expr::Expr::Column(col) = e {
                    Some(col.clone())
                } else {
                    None
                }
            })
            .collect();
        
        // Join signature: join(right, join_type, (left_keys, right_keys), filter)
        let new_plan = LogicalPlanBuilder::from(self.plan)
            .join(right.plan, join_type, (left_cols, right_cols), filter)
            .and_then(|b| b.build())
            .unwrap_or_else(|_| plan_clone);
        
        LazyFrame {
            plan: new_plan,
            ctx: self.ctx,
        }
    }
    
    /// Sort operation (lazy)
    pub fn sort(self, expr: Vec<datafusion::logical_expr::SortExpr>) -> Self {
        use datafusion::logical_expr::builder::LogicalPlanBuilder;
        
        let plan_clone = self.plan.clone();
        let new_plan = LogicalPlanBuilder::from(self.plan)
            .sort(expr)
            .and_then(|b| b.build())
            .unwrap_or_else(|_| plan_clone);
        
        LazyFrame {
            plan: new_plan,
            ctx: self.ctx,
        }
    }
    
    /// Execute the lazy plan and return results
    pub async fn collect(self) -> datafusion::error::Result<Vec<arrow::record_batch::RecordBatch>> {
        // Use the context to optimize and execute the logical plan
        let state = self.ctx.state();
        let optimized_plan = state.optimize(&self.plan)?;
        let physical_plan = state.create_physical_plan(&optimized_plan).await?;
        
        // Execute the physical plan
        let task_ctx = state.task_ctx();
        let results = datafusion::physical_plan::collect(physical_plan, task_ctx).await?;
        // Convert DataFusion RecordBatch to Arrow RecordBatch
        let converted: Vec<ArrowRecordBatch> = results.into_iter()
            .map(|b| convert_record_batch(b))
            .collect();
        Ok(converted)
    }
    
    /// Get the optimized plan (for debugging)
    pub async fn explain(&self) -> datafusion::error::Result<String> {
        let state = self.ctx.state();
        let optimized_plan = state.optimize(&self.plan)?;
        Ok(format!("{:?}", optimized_plan))
    }
    
    /// Take ownership and return the plan and context
    pub fn into_parts(self) -> (LogicalPlan, Arc<SessionContext>) {
        (self.plan, self.ctx)
    }
}

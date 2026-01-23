use datafusion::prelude::*;
use datafusion::logical_expr::{LogicalPlan, Expr};
use datafusion::execution::context::SessionContext;
use std::sync::Arc;
use tokio::runtime::Runtime;

/// Represents a lazy DataFrame with a logical plan
pub struct LazyFrame {
    plan: LogicalPlan,
    ctx: Arc<SessionContext>,
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
    /// Uses DataFusion's DataFrame API to build the plan properly
    pub fn filter(self, predicate: Expr) -> Self {
        let runtime = Runtime::new().expect("Failed to create runtime");
        let result = runtime.block_on(async {
            let df = self.ctx.read_logical_plan(self.plan.clone())?;
            let filtered_df = df.filter(predicate)?;
            Ok::<LogicalPlan, datafusion::error::DataFusionError>(filtered_df.logical_plan().clone())
        });
        
        match result {
            Ok(new_plan) => LazyFrame {
                plan: new_plan,
                ctx: self.ctx,
            },
            Err(e) => {
                // Fallback: create filter plan manually (simplified)
                // In production, you'd handle this better
                panic!("Failed to create filter plan: {}", e);
            }
        }
    }
    
    /// Apply a projection/select operation (lazy)
    pub fn select(self, exprs: Vec<Expr>) -> Self {
        let runtime = Runtime::new().expect("Failed to create runtime");
        let result = runtime.block_on(async {
            let df = self.ctx.read_logical_plan(self.plan.clone())?;
            let selected_df = df.select(exprs)?;
            Ok::<LogicalPlan, datafusion::error::DataFusionError>(selected_df.logical_plan().clone())
        });
        
        match result {
            Ok(new_plan) => LazyFrame {
                plan: new_plan,
                ctx: self.ctx,
            },
            Err(e) => {
                panic!("Failed to create select plan: {}", e);
            }
        }
    }
    
    /// Apply aggregation (lazy)
    pub fn aggregate(
        self,
        group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
    ) -> Self {
        let runtime = Runtime::new().expect("Failed to create runtime");
        let result = runtime.block_on(async {
            let df = self.ctx.read_logical_plan(self.plan.clone())?;
            let aggregated_df = df.aggregate(group_expr, aggr_expr)?;
            Ok::<LogicalPlan, datafusion::error::DataFusionError>(aggregated_df.logical_plan().clone())
        });
        
        match result {
            Ok(new_plan) => LazyFrame {
                plan: new_plan,
                ctx: self.ctx,
            },
            Err(e) => {
                panic!("Failed to create aggregate plan: {}", e);
            }
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
        let runtime = Runtime::new().expect("Failed to create runtime");
        let result = runtime.block_on(async {
            let left_df = self.ctx.read_logical_plan(self.plan.clone())?;
            let right_df = self.ctx.read_logical_plan(right.plan.clone())?;
            
            // Build join on conditions
            let on: Vec<(Expr, Expr)> = left_keys.into_iter()
                .zip(right_keys.into_iter())
                .collect();
            
            let joined_df = left_df.join(right_df, join_type, &on, filter)?;
            Ok::<LogicalPlan, datafusion::error::DataFusionError>(joined_df.logical_plan().clone())
        });
        
        match result {
            Ok(new_plan) => LazyFrame {
                plan: new_plan,
                ctx: self.ctx,
            },
            Err(e) => {
                panic!("Failed to create join plan: {}", e);
            }
        }
    }
    
    /// Sort operation (lazy)
    pub fn sort(self, expr: Vec<Expr>) -> Self {
        let runtime = Runtime::new().expect("Failed to create runtime");
        let result = runtime.block_on(async {
            let df = self.ctx.read_logical_plan(self.plan.clone())?;
            let sorted_df = df.sort(expr)?;
            Ok::<LogicalPlan, datafusion::error::DataFusionError>(sorted_df.logical_plan().clone())
        });
        
        match result {
            Ok(new_plan) => LazyFrame {
                plan: new_plan,
                ctx: self.ctx,
            },
            Err(e) => {
                panic!("Failed to create sort plan: {}", e);
            }
        }
    }
    
    /// Execute the lazy plan and return results
    pub async fn collect(self) -> datafusion::error::Result<Vec<arrow::record_batch::RecordBatch>> {
        // Create DataFrame from logical plan using DataFusion
        // Use the context to create a DataFrame from the logical plan
        let df = self.ctx.read_logical_plan(self.plan)?;
        
        // Execute and collect - DataFusion optimizes automatically
        df.collect().await
    }
    
    /// Get the optimized plan (for debugging)
    pub async fn explain(&self) -> datafusion::error::Result<String> {
        let df = self.ctx.read_logical_plan(self.plan.clone())?;
        df.explain(false, false).await
    }
    
    /// Take ownership and return the plan and context
    pub fn into_parts(self) -> (LogicalPlan, Arc<SessionContext>) {
        (self.plan, self.ctx)
    }
}

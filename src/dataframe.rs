use pyo3::prelude::*;
use arrow::record_batch::RecordBatch;
use arrow::datatypes::Schema as ArrowSchema;
use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::logical_expr::{LogicalPlan, Expr, JoinType};
use datafusion::execution::context::SessionContext;
use crate::column::Column;
use crate::schema::StructType;
use crate::execution::record_batch_to_python_dicts;
use crate::lazy::LazyFrame;
use crate::expression::pyany_to_expr;
use serde_json::Value;
use std::collections::HashMap;
use tokio::runtime::Runtime;

#[pyclass]
pub struct DataFrame {
    lazy_frame: Option<LazyFrame>,
    materialized: Option<Arc<RecordBatch>>, // Cached materialized data
    schema: Arc<ArrowSchema>,
    ctx: Arc<SessionContext>,
    runtime: Arc<Runtime>, // For async execution
}

#[pymethods]
impl DataFrame {
    #[new]
    fn new(data: Vec<RecordBatch>, schema: ArrowSchema) -> PyResult<Self> {
        let batch = data.first()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("No data provided"))?
            .clone();
        
        let schema = Arc::new(schema);
        let ctx = Arc::new(SessionContext::new());
        let runtime = Arc::new(Runtime::new().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to create runtime: {}", e))
        })?);
        
        // Create a DataFrame from the RecordBatch using DataFusion
        // Use read_batch to create a DataFrame from a single RecordBatch
        let runtime = Runtime::new().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to create runtime: {}", e))
        })?;
        
        let df = runtime.block_on(async {
            ctx.read_batch(batch.clone()).await
        }).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            format!("Failed to read batch: {}", e)
        ))?;
        
        // Create logical plan from the DataFrame
        let plan = df.logical_plan().clone();
        let lazy_frame = LazyFrame::new(plan, ctx.clone());
        
        Ok(DataFrame {
            lazy_frame: Some(lazy_frame),
            materialized: Some(Arc::new(batch)),
            schema,
            ctx,
            runtime,
        })
    }
    
    fn schema(&self) -> StructType {
        StructType::from_arrow_schema(&self.schema)
    }
    
    fn columns(&self) -> Vec<String> {
        self.schema.fields().iter().map(|f| f.name().clone()).collect()
    }
    
    fn count(&self) -> PyResult<usize> {
        // This is an action - need to execute
        if let Some(batch) = &self.materialized {
            Ok(batch.num_rows())
        } else if let Some(lazy) = &self.lazy_frame {
            // Execute lazy plan to get count
            let result = self.runtime.block_on(async {
                let df = self.ctx.read_logical_plan(lazy.plan().clone())?;
                df.count().await
            });
            match result {
                Ok(count) => Ok(count as usize),
                Err(e) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    format!("Failed to count: {}", e)
                )),
            }
        } else {
            Ok(0)
        }
    }
    
    fn show(&self, n: Option<usize>, truncate: Option<bool>) -> PyResult<()> {
        let n = n.unwrap_or(20);
        let truncate = truncate.unwrap_or(true);
        
        // Materialize if needed
        let batch = self.materialize_if_needed()?;
        
        let rows = record_batch_to_python_dicts(&batch);
        let display_rows = rows.iter().take(n);
        
        // Print header
        let cols: Vec<String> = batch.schema().fields().iter().map(|f| f.name().clone()).collect();
        println!("{}", cols.join(" | "));
        println!("{}", "-".repeat(cols.join(" | ").len()));
        
        // Print rows
        for row in display_rows {
            if let Value::Object(map) = row {
                let values: Vec<String> = cols.iter()
                    .map(|col| {
                        let val = map.get(col)
                            .map(|v| format!("{}", v))
                            .unwrap_or_else(|| "null".to_string());
                        if truncate && val.len() > 20 {
                            format!("{}...", &val[..20])
                        } else {
                            val
                        }
                    })
                    .collect();
                println!("{}", values.join(" | "));
            }
        }
        
        if rows.len() > n {
            println!("only showing top {} row(s)", n);
        }
        
        Ok(())
    }
    
    fn collect(&self) -> PyResult<Vec<HashMap<String, Value>>> {
        // Materialize if needed
        let batch = self.materialize_if_needed()?;
        
        let rows = record_batch_to_python_dicts(&batch);
        let result: Vec<HashMap<String, Value>> = rows.into_iter()
            .filter_map(|v| {
                if let Value::Object(map) = v {
                    Some(map.into_iter().collect())
                } else {
                    None
                }
            })
            .collect();
        Ok(result)
    }
    
    fn select(&self, cols: Vec<&str>) -> PyResult<DataFrame> {
        // Build lazy plan - this is a transformation
        if let Some(lazy) = &self.lazy_frame {
            let exprs: Vec<Expr> = cols.iter()
                .map(|name| col(name))
                .collect();
            
            let new_lazy = lazy.clone().select(exprs);
            
            // Infer new schema
            let new_fields: Vec<_> = cols.iter()
                .filter_map(|name| {
                    self.schema.fields().iter().find(|f| f.name() == *name)
                })
                .cloned()
                .collect();
            let new_schema = Arc::new(ArrowSchema::new(new_fields));
            
            Ok(DataFrame {
                lazy_frame: Some(new_lazy),
                materialized: None, // Clear materialized cache
                schema: new_schema,
                ctx: self.ctx.clone(),
                runtime: self.runtime.clone(),
            })
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("No lazy frame available"))
        }
    }
    
    fn filter(&self, condition: &PyAny) -> PyResult<DataFrame> {
        // Build lazy plan - this is a transformation
        if let Some(lazy) = &self.lazy_frame {
            let expr = pyany_to_expr(condition, Some(&self.schema))?;
            
            let new_lazy = lazy.clone().filter(expr);
            
            Ok(DataFrame {
                lazy_frame: Some(new_lazy),
                materialized: None, // Clear materialized cache
                schema: self.schema.clone(),
                ctx: self.ctx.clone(),
                runtime: self.runtime.clone(),
            })
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("No lazy frame available"))
        }
    }
    
    fn groupBy(&self, cols: Vec<&str>) -> PyResult<GroupedData> {
        Python::with_gil(|py| {
            Ok(GroupedData {
                df: Py::new(py, self.clone())?,
                grouping_cols: cols.iter().map(|s| s.to_string()).collect(),
            })
        })
    }
    
    fn join(&self, other: &DataFrame, on: &str, how: Option<&str>) -> PyResult<DataFrame> {
        let join_type = match how.unwrap_or("inner") {
            "inner" => JoinType::Inner,
            "left" => JoinType::Left,
            "right" => JoinType::Right,
            "outer" | "full" => JoinType::Full,
            _ => JoinType::Inner,
        };
        
        if let (Some(left_lazy), Some(right_lazy)) = (&self.lazy_frame, &other.lazy_frame) {
            let left_key = col(on);
            let right_key = col(on);
            
            let new_lazy = left_lazy.clone().join(
                right_lazy.clone(),
                join_type,
                vec![left_key],
                vec![right_key],
                None,
            );
            
            // Merge schemas for join result
            let mut new_fields = self.schema.fields().to_vec();
            new_fields.extend_from_slice(other.schema.fields());
            let new_schema = Arc::new(ArrowSchema::new(new_fields));
            
            Ok(DataFrame {
                lazy_frame: Some(new_lazy),
                materialized: None,
                schema: new_schema,
                ctx: self.ctx.clone(),
                runtime: self.runtime.clone(),
            })
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Both DataFrames must have lazy frames"))
        }
    }
    
    fn sort(&self, cols: Vec<&str>, ascending: Option<bool>) -> PyResult<DataFrame> {
        let asc = ascending.unwrap_or(true);
        
        if let Some(lazy) = &self.lazy_frame {
            let exprs: Vec<Expr> = cols.iter()
                .map(|name| {
                    let expr = col(name);
                    if asc {
                        expr
                    } else {
                        expr.sort(false, false) // descending
                    }
                })
                .collect();
            
            let new_lazy = lazy.clone().sort(exprs);
            
            Ok(DataFrame {
                lazy_frame: Some(new_lazy),
                materialized: None,
                schema: self.schema.clone(),
                ctx: self.ctx.clone(),
                runtime: self.runtime.clone(),
            })
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("No lazy frame available"))
        }
    }
    
    fn alias(&self, name: &str) -> DataFrame {
        DataFrame {
            lazy_frame: self.lazy_frame.clone(),
            materialized: self.materialized.clone(),
            schema: self.schema.clone(),
            ctx: self.ctx.clone(),
            runtime: self.runtime.clone(),
        }
    }
    
    fn __getitem__(&self, key: &str) -> PyResult<Column> {
        if self.schema.fields().iter().any(|f| f.name() == key) {
            Ok(Column::new(key.to_string()))
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(
                format!("Column '{}' not found", key)
            ))
        }
    }
}

impl DataFrame {
    /// Create DataFrame from a logical plan (for internal use)
    pub fn from_logical_plan(plan: LogicalPlan, ctx: Arc<SessionContext>) -> DataFrame {
        let runtime = Arc::new(Runtime::new().expect("Failed to create runtime"));
        
        // Get schema from plan
        let schema = plan.schema();
        
        DataFrame {
            lazy_frame: Some(LazyFrame::new(plan, ctx.clone())),
            materialized: None,
            schema: Arc::new(schema.as_ref().to_owned()),
            ctx,
            runtime,
        }
    }
    
    /// Materialize the lazy frame if needed
    fn materialize_if_needed(&self) -> PyResult<Arc<RecordBatch>> {
        if let Some(batch) = &self.materialized {
            return Ok(batch.clone());
        }
        
        if let Some(lazy) = &self.lazy_frame {
            let result = self.runtime.block_on(async {
                lazy.clone().collect().await
            });
            
            match result {
                Ok(batches) => {
                    if let Some(first_batch) = batches.first() {
                        Ok(Arc::new(first_batch.clone()))
                    } else {
                        Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("No data returned"))
                    }
                }
                Err(e) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    format!("Failed to materialize: {}", e)
                )),
            }
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("No lazy frame to materialize"))
        }
    }
}

#[pyclass]
pub struct GroupedData {
    df: Py<DataFrame>,
    grouping_cols: Vec<String>,
}

#[pymethods]
impl GroupedData {
    fn count(&self, py: Python) -> PyResult<DataFrame> {
        let df = self.df.borrow(py);
        
        if let Some(lazy) = &df.lazy_frame {
            let group_expr: Vec<Expr> = self.grouping_cols.iter()
                .map(|name| col(name))
                .collect();
            let aggr_expr: Vec<Expr> = vec![count(lit(1))];
            
            let new_lazy = lazy.clone().aggregate(group_expr, aggr_expr);
            
            Ok(DataFrame {
                lazy_frame: Some(new_lazy),
                materialized: None,
                schema: df.schema.clone(),
                ctx: df.ctx.clone(),
                runtime: df.runtime.clone(),
            })
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("No lazy frame available"))
        }
    }
    
    fn agg(&self, py: Python, exprs: &PyDict) -> PyResult<DataFrame> {
        let df = self.df.borrow(py);
        
        if let Some(lazy) = &df.lazy_frame {
            let group_expr: Vec<Expr> = self.grouping_cols.iter()
                .map(|name| col(name))
                .collect();
            
            // Parse aggregation expressions from dict
            let mut aggr_expr: Vec<Expr> = Vec::new();
            for (key, value) in exprs.iter() {
                if let Ok(col_name) = key.extract::<String>() {
                    if let Ok(col_ref) = value.extract::<PyRef<Column>>() {
                        aggr_expr.push(col_ref.expr().clone());
                    }
                }
            }
            
            let new_lazy = lazy.clone().aggregate(group_expr, aggr_expr);
            
            Ok(DataFrame {
                lazy_frame: Some(new_lazy),
                materialized: None,
                schema: df.schema.clone(),
                ctx: df.ctx.clone(),
                runtime: df.runtime.clone(),
            })
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("No lazy frame available"))
        }
    }
}

impl Clone for DataFrame {
    fn clone(&self) -> Self {
        DataFrame {
            lazy_frame: self.lazy_frame.clone(),
            materialized: self.materialized.clone(),
            schema: self.schema.clone(),
            ctx: self.ctx.clone(),
            runtime: self.runtime.clone(),
        }
    }
}

impl Clone for LazyFrame {
    fn clone(&self) -> Self {
        LazyFrame {
            plan: self.plan.clone(),
            ctx: self.ctx.clone(),
        }
    }
}

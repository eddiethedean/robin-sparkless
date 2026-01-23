# Refactoring Plan: Path to Polars Competitiveness

## Executive Summary

**Current State**: ❌ Not competitive - eager evaluation, no optimization
**Target State**: ✅ Competitive with Polars - lazy evaluation, query optimization

## Critical Changes Required

### 1. Replace String Expressions with DataFusion Expr

**Current Problem:**
```rust
// column.rs - Using strings
pub struct Column {
    expr: String, // ❌ Just a string
}
```

**Solution:**
```rust
// column.rs - Use DataFusion Expr
use datafusion::logical_expr::Expr;

pub struct Column {
    expr: Expr, // ✅ Real expression tree
}
```

### 2. Replace Eager Evaluation with Lazy Logical Plans

**Current Problem:**
```rust
// dataframe.rs - Immediate execution
fn filter(&self, condition: &PyAny) -> PyResult<DataFrame> {
    // ❌ Creates new DataFrame with cloned data immediately
    Ok(DataFrame {
        data: self.data.clone(), // Expensive copy!
        ...
    })
}
```

**Solution:**
```rust
// dataframe.rs - Lazy evaluation
#[pyclass]
pub struct DataFrame {
    lazy_frame: Option<LazyFrame>, // ✅ Store logical plan
    materialized: Option<Arc<RecordBatch>>, // Only when needed
    schema: Arc<ArrowSchema>,
    ctx: Arc<SessionContext>,
}

fn filter(&self, condition: &PyAny) -> PyResult<DataFrame> {
    // ✅ Build logical plan, don't execute
    let expr = column_to_expr(condition)?;
    let new_lazy = self.lazy_frame.filter(expr);
    Ok(DataFrame::from_lazy(new_lazy))
}
```

### 3. Implement Proper Column-to-Expr Conversion

**Needed:**
```rust
// expression.rs
pub fn column_to_expr(col: &Column) -> PyResult<Expr> {
    match col.expr {
        Expr::Column(name) => Ok(Expr::Column(name)),
        Expr::BinaryExpr { left, op, right } => {
            // Handle operators
        }
        // ... etc
    }
}

pub fn parse_string_expr(s: &str, schema: &Schema) -> PyResult<Expr> {
    // Parse string expressions to DataFusion Expr
    // Use DataFusion's expression parser
}
```

### 4. Defer Execution Until Actions

**Actions that trigger execution:**
- `.collect()`
- `.show()`
- `.count()`
- `.take()`
- `.head()`

**Transformations that should be lazy:**
- `.filter()`
- `.select()`
- `.groupBy()`
- `.join()`
- `.sort()`

### 5. Leverage DataFusion Optimizer

**Current:**
```rust
// execution.rs - No optimization
pub async fn execute_query(&self, sql: &str) -> Result<Vec<RecordBatch>> {
    let df = self.ctx.sql(sql).await?;
    df.collect().await? // ❌ No explicit optimization
}
```

**Solution:**
```rust
// execution.rs - With optimization
pub async fn execute_plan(&self, plan: LogicalPlan) -> Result<Vec<RecordBatch>> {
    // DataFusion optimizes automatically, but we can configure it
    let df = DataFrame::from_logical_plan(plan, self.ctx.clone());
    
    // Enable all optimizations
    let config = SessionConfig::new()
        .with_target_partitions(num_cpus::get())
        .with_repartition_joins(true)
        .with_repartition_aggregations(true);
    
    df.collect().await?
}
```

## Implementation Steps

### Phase 1: Expression System (Week 1)

1. Create `src/expression.rs` with Column-to-Expr conversion
2. Refactor `Column` to use `Expr` instead of `String`
3. Update all Column operations to build `Expr` trees
4. Add expression parsing for string inputs

### Phase 2: Lazy DataFrame (Week 2)

1. Create `LazyFrame` wrapper around `LogicalPlan`
2. Refactor `DataFrame` to store `LazyFrame` instead of data
3. Update all transformations to build logical plans
4. Implement lazy-to-materialized conversion

### Phase 3: Execution Engine (Week 3)

1. Refactor `ExecutionEngine` to execute `LogicalPlan`
2. Ensure DataFusion optimizer is enabled
3. Add query plan explanation/debugging
4. Implement efficient materialization

### Phase 4: Performance (Week 4)

1. Benchmark against Polars
2. Profile hot paths
3. Optimize expression building
4. Add caching where beneficial

## Code Structure After Refactoring

```
src/
├── lib.rs
├── session.rs          # SparkSession
├── dataframe.rs        # DataFrame (lazy wrapper)
├── lazy.rs             # LazyFrame (LogicalPlan wrapper)
├── column.rs           # Column (Expr wrapper)
├── expression.rs       # NEW: Expr conversion utilities
├── schema.rs
├── execution.rs        # Execution with optimization
└── functions.rs        # Functions that return Expr
```

## Key Design Decisions

### Decision 1: Use DataFusion or Polars Backend?

**Recommendation: DataFusion**
- Already integrated
- Competitive performance
- Better SQL support
- More control

### Decision 2: How to Handle PySpark API?

**Recommendation: Thin wrapper**
- Keep PySpark API on Python side
- Convert to DataFusion internally
- Maintain compatibility

### Decision 3: When to Materialize?

**Recommendation: Only on actions**
- All transformations = lazy
- Only materialize for `.collect()`, `.show()`, etc.
- Cache materialized results if accessed multiple times

## Performance Targets

After refactoring, we should achieve:
- **Simple filter**: < 2x Polars
- **Complex query**: < 3x Polars  
- **Memory**: Similar or better
- **Large data**: Streaming support

## Testing Strategy

1. **Correctness**: PySpark compatibility tests
2. **Performance**: Benchmark suite vs Polars
3. **Optimization**: Verify query plans are optimized
4. **Memory**: Profile memory usage

## Migration Path

1. Implement new lazy system alongside old
2. Add feature flag to switch between them
3. Test thoroughly
4. Switch default to lazy
5. Remove old eager code

## Estimated Effort

- **Expression system**: 3-5 days
- **Lazy DataFrame**: 5-7 days
- **Execution engine**: 3-5 days
- **Testing & optimization**: 5-7 days
- **Total**: ~3-4 weeks for full implementation

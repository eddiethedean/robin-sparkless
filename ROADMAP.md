# Roadmap: Becoming Competitive with Polars While Maintaining PySpark Parity

## Core Principle: PySpark Parity First, Performance Second

**Primary Goal**: Maintain 100% PySpark API compatibility while achieving Polars-level performance.

**Constraint**: All optimizations and refactorings must preserve PySpark API behavior. PySpark code should work without modification.

### PySpark Parity Requirements

1. **API Compatibility**: All PySpark DataFrame, Column, and SparkSession methods must work identically
2. **Behavior Matching**: Lazy evaluation, error messages, edge cases must match PySpark
3. **Result Correctness**: Same inputs must produce same outputs as PySpark
4. **Type Handling**: Data types, null handling, type coercion must match PySpark
5. **Schema Inference**: Schema inference from data must match PySpark behavior

### Testing Strategy for Parity

- **Unit Tests**: Each operation tested against PySpark
- **Integration Tests**: Common PySpark workflows validated
- **Compatibility Suite**: Port PySpark test suite
- **Real-World Validation**: Run existing PySpark code without modification

## Current Status: ⚠️ Foundation Built, But Not Competitive Yet

We have the right building blocks (Arrow, DataFusion) and PySpark API structure, but need to implement proper lazy evaluation and query optimization **without breaking PySpark compatibility**.

## Immediate Priorities (To Be Competitive)

### 🔴 Critical (Must Have)

1. **True Lazy Evaluation** (2-3 weeks)
   - Replace string-based expressions with DataFusion `Expr`
   - Build `LogicalPlan` instead of executing immediately
   - Defer all execution until actions (`.collect()`, `.show()`, etc.)
   - **PySpark Parity**: Ensure lazy behavior matches PySpark's lazy evaluation semantics
   - **Testing**: Validate all transformations are lazy and match PySpark behavior

2. **Query Optimization** (1 week)
   - Ensure DataFusion optimizer is enabled
   - Verify predicate/projection pushdown works
   - Add query plan inspection
   - **PySpark Parity**: Optimizations must not change query results (only performance)
   - **Testing**: Compare query results before/after optimization to ensure correctness

3. **Efficient Execution** (1 week)
   - Remove unnecessary data cloning
   - Keep data in Arrow format
   - Optimize materialization
   - **PySpark Parity**: Execution results must match PySpark exactly
   - **Testing**: Run PySpark test suite to validate output correctness

### 🟡 Important (Should Have)

4. **Expression System** (1 week)
   - Proper Column-to-Expr conversion
   - Support complex expressions
   - Expression optimization
   - **PySpark Parity**: All Column operations must match PySpark behavior
   - **Testing**: Test all Column operators, methods, and functions for PySpark compatibility

5. **Data Source Readers** (2 weeks)
   - Efficient CSV reading with PySpark-compatible options
   - Parquet support matching PySpark schema handling
   - JSON support with PySpark parsing behavior
   - Streaming for large files
   - **PySpark Parity**: Reader options, schema inference, and data types must match PySpark
   - **Testing**: Compare output with PySpark for same input files

6. **PySpark Compatibility Testing** (Ongoing)
   - Run existing PySpark test suites
   - Create compatibility test suite covering:
     - DataFrame operations
     - Column expressions
     - SQL functions
     - Data types and schemas
     - Edge cases and error handling
   - **PySpark Parity**: Maintain test suite that validates API compatibility

7. **Performance Benchmarking** (Ongoing)
   - Compare with Polars on same operations
   - Compare with PySpark on same operations (ensure correctness)
   - Identify bottlenecks
   - Iterate on performance while maintaining correctness

### 🟢 Nice to Have

8. **Advanced PySpark Features**
   - Window functions (PySpark API compatible)
   - UDF support (Python UDFs matching PySpark behavior)
   - More SQL functions (complete PySpark functions module)
   - Caching (PySpark-compatible cache levels)
   - Broadcast variables
   - Accumulators
   - **PySpark Parity**: All features must match PySpark API and behavior

## Timeline to Competitiveness

### Month 1: Core Lazy Evaluation + PySpark Parity
- Week 1-2: Expression system + LazyFrame
  - Build Expr-based Column system
  - Implement LazyFrame with LogicalPlan
  - **PySpark Parity**: Test each operation against PySpark
- Week 3: Execution engine refactoring
  - Integrate DataFusion execution
  - **PySpark Parity**: Validate execution results match PySpark
- Week 4: PySpark compatibility testing
  - Run PySpark test suite
  - Fix any API incompatibilities
  - Document differences (if any)

### Month 2: Optimization & Performance + PySpark Validation
- Week 1: Query optimization tuning
  - Enable DataFusion optimizations
  - **PySpark Parity**: Ensure optimizations don't change results
- Week 2: Data source readers
  - Implement CSV/Parquet/JSON readers
  - **PySpark Parity**: Match PySpark reader options and behavior
- Week 3: Performance profiling
  - Profile hot paths
  - **PySpark Parity**: Verify correctness after optimizations
- Week 4: Benchmarking vs Polars & PySpark
  - Performance vs Polars
  - Correctness vs PySpark
  - Document performance/parity trade-offs

### Month 3: Polish & Production Ready
- Week 1-2: Remaining PySpark features
  - Window functions
  - UDF support
  - Additional SQL functions
- Week 3: Documentation
  - PySpark compatibility guide
  - Migration guide from PySpark
  - Performance tuning guide
- Week 4: Production hardening
  - Final PySpark compatibility validation
  - Performance benchmarks
  - Production readiness checklist

## Success Metrics

We'll know we're competitive when:

### PySpark Parity (Primary Requirement)
1. **API Compatibility**: 100% of core PySpark DataFrame API works identically
2. **Result Correctness**: All operations produce same results as PySpark
3. **Behavior Matching**: Lazy evaluation, error handling, edge cases match PySpark
4. **Test Suite**: Pass PySpark compatibility test suite (target: >95% pass rate)

### Performance (Secondary Requirement)
5. **Performance**: Within 2-3x of Polars on common operations
6. **Memory**: Similar or better memory usage than Polars
7. **Features**: Core PySpark operations work efficiently
8. **Stability**: No crashes, handles edge cases

### Validation Process
- Run PySpark test suite on our implementation
- Compare output for same inputs
- Benchmark performance vs Polars
- Document any intentional differences

## Alternative: Use Polars Backend

If speed to market is critical, consider:

**Option**: Use Polars as the execution engine, maintain PySpark API on top

**Pros:**
- Get Polars performance immediately
- Proven optimization engine
- Faster development

**Cons:**
- Less control
- Need comprehensive PySpark API compatibility layer
- Polars API is different from PySpark (more work to map)
- Must ensure all PySpark behaviors are preserved

**Implementation**: ~2-3 weeks to build robust PySpark compatibility layer
- Map all PySpark DataFrame operations to Polars
- Handle PySpark-specific behaviors (null handling, type coercion, etc.)
- Extensive testing to ensure PySpark parity

## Recommendation

**For fastest path to competitiveness**: Use Polars backend with PySpark API wrapper
- **Timeline**: 2-3 weeks for compatibility layer
- **Risk**: Must ensure PySpark parity is maintained
- **Benefit**: Immediate Polars-level performance

**For long-term control**: Implement proper DataFusion lazy evaluation (3-4 months)
- **Timeline**: 3-4 months for full implementation
- **Risk**: More complex, but full control
- **Benefit**: Custom optimizations, better PySpark API fit

**Key Consideration**: Both paths require extensive PySpark compatibility testing. The Polars path may have more API mapping challenges, while the DataFusion path requires more implementation work but may fit PySpark semantics better.

## PySpark Parity Testing Strategy

### Phase 1: Unit Tests
- Test each DataFrame method individually
- Compare results with PySpark
- Test edge cases (nulls, empty DataFrames, etc.)

### Phase 2: Integration Tests
- Test common PySpark workflows
- Test complex query chains
- Test data type handling

### Phase 3: Compatibility Suite
- Port PySpark test suite
- Run existing PySpark code
- Validate real-world use cases

### Phase 4: Continuous Validation
- Maintain compatibility test suite
- Run on every change
- Document any intentional differences

## Decision Framework

When making implementation decisions, prioritize:
1. **PySpark Parity** - Does it match PySpark behavior?
2. **Performance** - Is it competitive with Polars?
3. **Maintainability** - Can we maintain it long-term?

If PySpark parity conflicts with performance, document the trade-off and provide migration path.

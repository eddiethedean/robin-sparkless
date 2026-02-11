Research doc: fixing string–numeric comparison parity (issue #235)
1. Problem statement
Goal: Make robin‑sparkless match PySpark’s behavior for comparisons between string columns and numeric values, especially in filter:
df.filter(col("str_col") == lit(123))
df.filter(col("str_col") > lit(200))
and symmetric forms like df.filter(lit(123) == col("str_col"))
Expected (PySpark):
String values that look like numbers (e.g. "123", " 45.6 ") are coerced to numbers and compared numerically.
Non‑numeric strings (e.g. "abc") are treated as null in numeric contexts; comparisons in WHERE behave as non‑matches.
Comparisons do not raise type errors; they simply include or exclude rows.
Actual (current robin‑sparkless behavior):
When a string column is compared to a numeric literal, Polars sees a direct Utf8 == Int32 (or similar) comparison and raises:
> RuntimeError: cannot compare string with numeric type (i32)
This shows up in tests/python/test_issue_235_type_strictness_comparisons.py for all four tests.
2. Key components in the codebase
This is the plumbing we have to work with:
2.1 Expression and comparison construction
Rust Column type: src/column.rs
Wraps a Polars Expr.
Provides methods like gt, lt, eq, neq, and PySpark‑style *_pyspark variants for null semantics.
Also has cast_to / try_cast_to which call into src/functions.rs.
Python Column bindings: src/python/column.rs
gt/ge/lt/le/eq/ne accept “other” as a Python object, convert it to RsColumn via py_any_to_column, and then call the corresponding Rust Column methods.
Python __gt__, __eq__, etc. just delegate to those.
DataFrame operations: src/dataframe/mod.rs, src/dataframe/transformations.rs
DataFrame.filter(&self, condition: Expr):
resolve_expr_column_names rewrites Expr::Column("name") according to schema/case sensitivity.
transformations::filter applies lazy().filter(condition) and collects.
There is a helper coerce_string_numeric_comparisons(&self, expr: Expr) that walks the Expr tree and tries to wrap string columns with cast(Float64) when compared to numeric literals.
Plan interpreter: src/plan/expr.rs
Converts JSON expressions from Sparkless into Polars Expr:
"op": "eq" | "ne" | "gt" | "ge" | "lt" | "le" → direct l.eq(r), l.gt(r), etc.
Currently does no type-based coercion; it assumes types are already compatible.
2.2 Type coercion utilities
src/type_coercion.rs:
find_common_type(left_type, right_type) with simple precedence (Int, Long, Float, Double, String).
coerce_for_comparison(left_expr, right_expr, left_type, right_type):
For numeric–numeric, casts both to a common numeric type (typically Float64).
For equal types, leaves as-is.
Otherwise, errors.
src/functions.rs:
cast(column, type_name):
Special‑cases string→boolean (apply_string_to_boolean), string→date (apply_string_to_date), string→int (apply_string_to_int).
For others, uses strict_cast.
try_cast(column, type_name):
Same idea but returns nulls instead of error for invalid conversions, where supported.
to_number / try_to_number:
Wrap cast("double") / try_cast("double").
String→X UDFs in src/udfs.rs:
apply_string_to_int:
Parses strings to i64, handles strict vs non‑strict modes, maps to Int32/Int64, invalid strings → error (strict) or null (non‑strict).
apply_string_to_date:
Parses date/datetime strings into Date, again with strict vs non‑strict behavior.
Newly added: apply_string_to_double:
Similar in structure to apply_string_to_int, but producing Float64.
2.3 Tests and current status
Issue #235 tests: tests/python/test_issue_235_type_strictness_comparisons.py:
df.filter(F.col("str_col") == F.lit(123)).collect() → expect [{ "str_col": "123" }].
df.filter(F.col("str_col") > F.lit(200)).collect() → expect [{ "str_col": "456" }].
Edge cases added for invalid strings and symmetric lit(123) == col("str_col").
These still fail with RuntimeError: cannot compare string with numeric type (i32).
Other tests:
Issue #201 (test_issue_201_type_strictness.py) verifies string→numeric coercion for arithmetic; those pass, using separate arithmetic‑specific logic (add_pyspark, etc.).
3. What’s been attempted so far (and why it’s insufficient)
String→double UDF and wiring:
apply_string_to_double added and used in cast("double") / try_cast("double").
That means to_number / try_to_number now have a proper string→double path.
This is necessary, but not sufficient: no comparison code path is actually building Exprs that invoke to_number/try_to_number on the string side.
Column-level comparison helpers:
eq_with_numeric_literal, gt_with_numeric_literal, etc. added to Column and invoked from Python Column bindings.
These try to run both sides through try_to_number when a numeric literal is present.
However, the final Expr tree that reaches DataFrame.filter and Polars is still effectively col("str_col") == 123, because:
The helpers return new Column wrappers, but when those Columns are converted back into Exprs and passed through the current coerce_string_numeric_comparisons, the string side is not being re‑expressed as try_to_number(...)—just cast to Float64 or left as Utf8.
DataFrame-level coerce_string_numeric_comparisons:
Current implementation:
Detects Expr::BinaryExpr with Expr::Column on one side and numeric literal on the other.
Wraps the column Expr in a Polars cast(DataType::Float64).
This does not parse string content; it just casts the series type, which Polars will reject for invalid string→float conversions in comparisons.
Debug logging shows the Expr going into Polars is still col("str_col") == 123, not try_to_number(col("str_col")) == 123.0.
Plan interpreter remains strict:
expr_from_value in src/plan/expr.rs still builds comparisons via l.eq(r), l.gt(r) directly, with no type-aware coercion.
Net effect: even with the new UDF and helpers, every path that matters for #235 still hands Polars a direct string–numeric comparison, so the type error persists.
4. High-level design for a real fix
The core idea is to create one central comparison-coercion function in Rust and make everything go through it:
coerce_for_pyspark_comparison (new, in src/type_coercion.rs) becomes the single source of truth for how to compare two Exprs of given types.
It knows how to:
Call the string→double UDF (apply_string_to_double via to_number/try_to_number).
Choose a common numeric type (Float64) for string–numeric combinations.
Defer to coerce_for_comparison for plain numeric–numeric cases.
All comparison-building code (Python Column ops, DataFrame.filter/select/with_column, and the plan interpreter) constructs “naive” Exprs and then calls this coercion helper before execution.
This removes the need for scattered, partially overlapping logic and ensures that any comparison with a string + numeric combination is handled consistently.
5. Concrete steps and tasks
5.1 Solidify the string→double primitive
Task: Finish and verify apply_string_to_double in src/udfs.rs:
Follow the same structure as apply_string_to_int/apply_string_to_date.
Ensure strict vs non‑strict behavior is correct and error messages are clear (include column name and offending value for strict errors).
Task: Wire into cast/try_cast and to_number/try_to_number:
In cast(column, type_name):
When dtype == DataType::Float64, use Expr::map with apply_string_to_double(col, true), GetOutput::from_type(DataType::Float64).
In try_cast(column, type_name):
Same, but apply_string_to_double(col, false).
Task: Add Rust tests:
Test cast("double") and try_cast("double") on:
"123", " 45.6 ", "abc", "", and numeric inputs.
Confirm to_number/try_to_number behave as expected.
5.2 Implement coerce_for_pyspark_comparison in src/type_coercion.rs
Signature:
  pub fn coerce_for_pyspark_comparison(      left: Expr,      right: Expr,      left_type: &DataType,      right_type: &DataType,      op: &Operator,  ) -> Result<(Expr, Expr), PolarsError>
Logic:
If left_type and right_type are both numeric:
Call existing coerce_for_comparison.
If one side is DataType::String and the other is numeric:
For the string side:
Build an Expr that applies to_number/try_to_number to the column:
Typically via a Column helper (Column::from_expr(…) → to_number → back to Expr), or directly via Expr::map and apply_string_to_double.
For the numeric side:
Cast to DataType::Float64 if necessary.
If both sides are String (or equal non‑numeric type):
Return the original (left, right) unchanged.
Otherwise:
Option A: fallback to coerce_for_comparison and propagate its result.
Option B: return a PolarsError::ComputeError saying comparison of those types is unsupported.
Task: Add unit tests:
Use synthetic Expr::Column / Expr::Literal and DataTypes to verify:
String–int gets rewritten to (Float64Expr, Float64Expr) with string side going through numeric coercion.
Numeric–numeric behavior is unchanged.
Unsupported combos behave as defined.
5.3 Rework DataFrame expression rewriting
Task: Replace coerce_string_numeric_comparisons in src/dataframe/mod.rs:
New version:
Accepts Expr, returns Result<Expr, PolarsError>.
Runs expr.try_map_expr(|e| match e { Expr::BinaryExpr { left, op, right } if op is comparison => { … } … }).
For each comparison:
Determine left_type / right_type:
For Expr::Column(name): lookup in self.df.schema().
For Expr::Literal: infer from LiteralValue.
For other node types: either skip or handle only when you can determine a type.
Call coerce_for_pyspark_comparison and rebuild Expr::BinaryExpr from the returned Exprs.
Task: Use this function in src/dataframe/transformations.rs:
In filter:
After resolve_expr_column_names, call the new coerce_* before building the lazy filter.
Optionally apply similar treatment in other expression‑heavy APIs if needed.
This ensures that any comparison Expr hitting Polars from the DataFrame API has already had string–numeric combinations coerced via the central helper.
5.4 Simplify Python Column comparisons
Task: In src/python/column.rs:
Adjust gt/ge/lt/le/eq/ne to only:
Convert other to RsColumn via py_any_to_column.
Call the basic Rust Column methods that build simple Expr comparisons (gt, lt, eq, neq, etc.).
Remove the current reliance on *_with_numeric_literal or any Python-side coercion logic.
Task: In src/column.rs:
Either:
Make *_with_numeric_literal thin wrappers that call coerce_for_pyspark_comparison in a type‑aware way, or
Remove them entirely once the DataFrame/plan paths reliably rewrite comparisons.
The objective is: no divergent comparison semantics between Python Column and DataFrame/plan—all type reasoning happens in one place.
5.5 Integrate into the plan interpreter
Task: In src/plan/expr.rs:
For each comparison op ("eq" | "ne" | "gt" | "ge" | "lt" | "le"):
Instead of immediately doing l.eq(r):
Identify whether operands are Expr::Column vs Expr::Literal.
If you have schema/type metadata for columns (from the plan’s context), call coerce_for_pyspark_comparison with the right types.
Build the final comparison Expr from the coerced pair.
If schema isn’t available:
Handle at least the literal–column case with a heuristic (e.g. treat col("str_col") as String if known from context), and rely on the DataFrame rewrite as a second pass.
5.6 Testing and validation
Python tests:
Keep and extend tests/python/test_issue_235_type_strictness_comparisons.py as the canonical parity spec for #235.
Ensure it covers:
equality and ordering,
mixed valid/invalid strings,
symmetric literal–column forms.
Rust tests:
Add tests for coerce_for_pyspark_comparison to confirm:
string–numeric combinations are rewritten as expected,
numeric–numeric still use the existing path,
errors or fallbacks are deliberate.
Full suite:
Run:
python -m pytest tests/python/test_issue_235_type_strictness_comparisons.py tests/python/test_issue_201_type_strictness.py -v
make check-full
Investigate and adjust any regressions, particularly where prior behavior depended on strict comparison semantics.
5.7 Documentation and cleanup
Update:
CHANGELOG.md (#235 entry) to describe the new semantics precisely.
Optionally docs/PYSPARK_DIFFERENCES.md to note remaining edge‑case differences.
Remove:
Temporary debug env vars (ROBIN_DEBUG_235) and scattered ad hoc comparison hacks, now superseded by coerce_for_pyspark_comparison.

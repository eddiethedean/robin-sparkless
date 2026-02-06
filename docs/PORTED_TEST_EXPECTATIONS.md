# Ported Test Expectations

Expected outputs for the ported Sparkless parity tests in `tests/python/test_dataframe_parity.py`. Ground truth is PySpark; these were derived from Sparkless expected_outputs / PySpark behavior.

---

## test_filter_salary_gt_60000

**Input:** `INPUT_EMPLOYEES` (4 rows: id, name, age, salary, department).  
**Op:** `filter(col("salary").gt(lit(60000)))`.

**Expected rows (order irrelevant):**
```json
[
  {"age": 35, "department": "IT", "id": 3, "name": "Charlie", "salary": 70000},
  {"age": 40, "department": "Finance", "id": 4, "name": "David", "salary": 80000}
]
```

---

## test_filter_and_operator

**Input:** `[{"a":1,"b":2}, {"a":2,"b":3}, {"a":3,"b":1}]`.  
**Op:** `filter((col("a") > 1) & (col("b") > 1))`.

**Expected:** 1 row: `{"a": 2, "b": 3}`.

---

## test_filter_or_operator

**Input:** `[{"a":1,"b":2}, {"a":2,"b":3}, {"a":3,"b":1}]`.  
**Op:** `filter((col("a") > 1) | (col("b") > 1))`.

**Expected:** 3 rows (all rows).

---

## test_basic_select

**Input:** `INPUT_EMPLOYEES`.  
**Op:** `select(["id", "name", "age"])`.

**Expected rows (order matters):**
```json
[
  {"id": 1, "name": "Alice", "age": 25},
  {"id": 2, "name": "Bob", "age": 30},
  {"id": 3, "name": "Charlie", "age": 35},
  {"id": 4, "name": "David", "age": 40}
]
```

---

## test_select_with_alias

**Input:** `INPUT_EMPLOYEES`.  
**Op:** `with_column("user_id", col("id")).with_column("full_name", col("name")).select(["user_id", "full_name"])`.

**Expected rows (order matters):**
```json
[
  {"user_id": 1, "full_name": "Alice"},
  {"user_id": 2, "full_name": "Bob"},
  {"user_id": 3, "full_name": "Charlie"},
  {"user_id": 4, "full_name": "David"}
]
```

---

## test_aggregation_avg_count

**Input:** `INPUT_EMPLOYEES`.  
**Op:** `group_by(["department"]).agg([avg(salary).alias("avg_salary"), count(id).alias("count")])`.

**Expected rows (order irrelevant):**
```json
[
  {"department": "Finance", "avg_salary": 80000.0, "count": 1},
  {"department": "HR", "avg_salary": 60000.0, "count": 1},
  {"department": "IT", "avg_salary": 60000.0, "count": 2}
]
```

---

## test_inner_join

**Input:** Employees (id, name, dept_id, salary) and Departments (dept_id, name, location).  
**Op:** `emp_df.join(dept_df, ["dept_id"], "inner")`.

**Expected:** 3 rows (dept_id 10 × 2, dept_id 20 × 1). Key checks:
- `id` 1: dept_id 10, salary 50000
- `id` 2: dept_id 20
- `id` 3: dept_id 10, salary 70000

---

## Shared input: INPUT_EMPLOYEES

```json
[
  {"id": 1, "name": "Alice", "age": 25, "salary": 50000, "department": "IT"},
  {"id": 2, "name": "Bob", "age": 30, "salary": 60000, "department": "HR"},
  {"id": 3, "name": "Charlie", "age": 35, "salary": 70000, "department": "IT"},
  {"id": 4, "name": "David", "age": 40, "salary": 80000, "department": "Finance"}
]
```

**Schema:** `[("id","bigint"), ("name","string"), ("age","bigint"), ("salary","double"), ("department","string")]`.

---

See [SPARKLESS_PYTHON_TEST_PORT.md](SPARKLESS_PYTHON_TEST_PORT.md) for port tracker and [test_dataframe_parity.py](../tests/python/test_dataframe_parity.py) for the actual tests.

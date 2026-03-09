# Parity test expectations vs PySpark behavior

## Summary

**The fixture expectations are correct and match PySpark.** The failures are due to our join implementation producing fewer columns than PySpark when there are duplicate non-key column names or when the right side’s column is expected to appear as `*_right`.

---

## 1. PySpark parity fixtures (inner_join, left_join, right_join)

**Source:** `tests/fixtures/inner_join.json` (and left_join.json, right_join.json)  
**Expected schema:** 6 columns – `dept_id`, `id`, `name`, `salary`, `location`, `name`  
**Actual:** 5 columns  
**Error:** `schema length mismatch: actual 5 columns, expected 6`

**PySpark behavior:**

- Join uses **column-name syntax** `join(right_df, on=["dept_id"])`, so the join key is **deduplicated** (one `dept_id`).
- Left: `dept_id`, `id`, `name`, `salary`  
  Right: `dept_id`, `location`, `name`
- After join, PySpark can keep **two columns with the same name** `"name"` (one from left, one from right). So the result has 6 columns, with `"name"` appearing twice.
- Fixture expected schema: `dept_id`, `id`, `name`, `salary`, `location`, `name` → 6 columns, duplicate `"name"` is intentional.

**Conclusion:** Expectation is correct. Our join ends up with 5 columns because we either suffix the right’s `name` to `name_right` and then don’t include it when building the select list, or we deduplicate by name and drop the second column. PySpark keeps both.

---

## 2. Plan parity fixtures (join_inner_dept_issue510, join_on_string_issue513)

**Source:** `tests/fixtures/plans/join_inner_dept_issue510.json`, `join_on_string_issue513.json`  
**Expected schema:** 4 columns – `Name`, `Id`, `Dept`, `Name_right`  
**Actual:** 3 columns  
**Error:** `schema length mismatch: actual 3 columns, expected 4`

**Fixture setup:**

- Left: `Name`, `Id`, `Dept`  
  Right: `Dept`, `Name`  
  Join on: `Dept`
- Expected: left columns + right non-key column as `Name_right` → `Name`, `Id`, `Dept`, `Name_right`.

**PySpark behavior:**

- With `join(on="Dept")`, the key `Dept` appears once. The right’s `Name` is a different column from the left’s `Name`; PySpark typically exposes it as a separate column (often `Name` or with a suffix like `Name_right` in tests). The fixture explicitly expects `Name_right`.

**Conclusion:** Expectation is correct. We should produce 4 columns with the right’s `Name` as `Name_right`. We currently produce 3, so the right’s `Name` is missing (coalesced or dropped).

---

## References

- PySpark: `join(df, on="col")` deduplicates the join key; non-key columns with the same name can both appear (duplicate column names in the result).
- Fixtures:  
  - `tests/fixtures/inner_join.json` (and left/right_join) – expected from PySpark via `gen_pyspark_cases.py` or equivalent.  
  - `tests/expected_outputs/joins/inner_join.json` – 7 columns when using condition join (both key columns kept: `dept_id`, `dept_id_right`), consistent with condition-join semantics.
- Our join: `tests/parity.rs` uses `apply_operations` → `df.join(&right_df, on_refs, join_type)` (column-name join). Post-join we build a column list and, when Polars returns duplicate names or we use `_right` naming, we end up with fewer columns than the fixtures expect.

**Verdict:** Do **not** change the fixture expectations. They match PySpark. The join implementation should be updated so that:

1. For column-name joins with duplicate non-key names (e.g. two `"name"`), we preserve both columns (e.g. 6 columns for inner_join).
2. For plan joins, we include the right’s non-key columns with the expected naming (e.g. `Name_right`), so we get 4 columns for join_inner_dept_issue510 / join_on_string_issue513.

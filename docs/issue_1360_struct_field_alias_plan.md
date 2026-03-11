# Issue #1360: Struct field alias after getField (PySpark parity)

**Goal:** `col("struct_col").getField("field_name").alias("y")` should produce an output column named `y` with the field value. Struct field resolution (names/case) and alias propagation should match PySpark.

**Current state:** See `tests/dataframe/test_issue_330_struct_field_alias.py` and `test_struct_field_alias_parity.py`. Failures may include:
- Alias not applied so output column keeps struct field name
- getField / struct subscript resolution (case, nested)

**Implementation directions:**
1. Ensure expression plan preserves alias when the root is a struct field access (getField or struct.field).
2. Align struct field name resolution (case-sensitive vs case-insensitive) with PySpark.
3. When resolving select items, propagate `.alias("name")` to the final output schema.

**Reference:** [PySpark Column.getField](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.getField.html), [Column.alias](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.alias.html).

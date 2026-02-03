# Test Creation Guide: PySpark Parity via Fixtures

This document explains how to create and maintain **behavioral parity tests** between PySpark and `robin-sparkless`.

**Sparkless integration**: Robin-sparkless is designed to replace the backend of [Sparkless](https://github.com/eddiethedean/sparkless). Sparkless has 270+ expected_outputs that can be converted to robin-sparkless fixtures. See [SPARKLESS_INTEGRATION_ANALYSIS.md](SPARKLESS_INTEGRATION_ANALYSIS.md) §4 for fixture format comparison and conversion strategy.

The goal is to:
- Use **PySpark as the oracle** for correct behavior.
- Generate **JSON fixtures** from PySpark runs.
- Consume those fixtures in **Rust tests** to ensure `robin-sparkless` matches PySpark.

---

## 1. Overview

We separate the flow into three layers:

1. **Scenario definition (Python + PySpark)**  
   Small scripts that build PySpark `DataFrame`s, apply operations, and record the expected results.

2. **Fixtures (JSON)**  
   A stable, machine-readable description of:
   - Input schema and rows  
   - The sequence of operations  
   - Expected schema and rows

3. **Rust test harness**  
   `tests/parity.rs` reads fixtures, reconstructs the input as a `robin-sparkless` `DataFrame`, applies the same operations, and compares results.

---

## 2. Fixture Format

Fixtures live under `tests/fixtures/` and are plain JSON files.

### Minimal schema

```jsonc
{
  "name": "filter_age_gt_30",
  "pyspark_version": "3.5.0",

  "input": {
    "schema": [
      { "name": "id",   "type": "int" },
      { "name": "age",  "type": "int" },
      { "name": "name", "type": "string" }
    ],
    "rows": [
      [1, 25, "Alice"],
      [2, 30, "Bob"],
      [3, 35, "Charlie"]
    ]
  },

  "operations": [
    { "op": "filter",  "expr": "col('age') > 30" },
    { "op": "select",  "columns": ["name", "age"] },
    { "op": "orderBy", "columns": ["name"], "ascending": [true] }
  ],

  "expected": {
    "schema": [
      { "name": "name", "type": "string" },
      { "name": "age",  "type": "int" }
    ],
    "rows": [
      ["Charlie", 35]
    ]
  }
}
```

**Notes:**
- `type` uses PySpark’s simple type names (`int`, `bigint`, `string`, `double`, `boolean`, etc.).
- `rows` are lists of JSON values; `null` is allowed.
- `operations` are **descriptive**, not executable Python strings; we only embed simple PySpark-style expressions for `filter` to keep parity obvious.

### Date and timestamp columns

The parity harness supports **date** and **timestamp** (or **datetime**) column types so that datetime functions (e.g. `date_add`, `datediff`, `current_date`) can be tested with real date/datetime inputs.

- **Schema**: Use `"type": "date"` or `"type": "timestamp"` (or `"datetime"`, `"timestamp_ntz"`) in `input.schema` and `expected.schema`.
- **Row values**: Use JSON strings in ISO format:
  - **Date**: `"YYYY-MM-DD"` (e.g. `"2024-01-15"`).
  - **Timestamp**: `"YYYY-MM-DDTHH:MM:SS"` or `"YYYY-MM-DDTHH:MM:SS.ffffff"` (e.g. `"2024-01-15T12:00:00"`, `"2024-01-15T12:00:00.123456"`).
- Timestamp can also be given as a numeric (epoch microseconds) in the JSON when building the column.

---

## 3. PySpark Generator Script

Create a Python script, e.g. `tests/gen_pyspark_cases.py`, that:

1. Starts a PySpark `SparkSession` (pinned PySpark version).
2. Defines **scenario functions** that:
   - Build a PySpark `DataFrame` from literal data.
   - Apply a sequence of transformations/actions.
   - Collect both **input** and **expected** sections.
3. Writes JSON fixtures into `tests/fixtures/`.

### Example skeleton

```python
from pyspark.sql import SparkSession
import json
from pathlib import Path

spark = SparkSession.builder.appName("rs_parity_gen").getOrCreate()


def case_filter_age_gt_30():
    data = [(1, 25, "Alice"), (2, 30, "Bob"), (3, 35, "Charlie")]
    df = spark.createDataFrame(data, ["id", "age", "name"])

    out_df = df.filter("age > 30").select("name", "age").orderBy("name")

    def schema_to_json(schema):
        return [{"name": f.name, "type": f.dataType.simpleString()}
                for f in schema.fields]

    input_schema = schema_to_json(df.schema)
    input_rows = [list(r) for r in df.collect()]

    expected_schema = schema_to_json(out_df.schema)
    expected_rows = [list(r) for r in out_df.collect()]

    return {
        "name": "filter_age_gt_30",
        "pyspark_version": spark.version,
        "input": {"schema": input_schema, "rows": input_rows},
        "operations": [
            {"op": "filter", "expr": "col('age') > 30"},
            {"op": "select", "columns": ["name", "age"]},
            {"op": "orderBy", "columns": ["name"], "ascending": [True]}
        ],
        "expected": {"schema": expected_schema, "rows": expected_rows},
    }


def main():
    out_dir = Path("tests/fixtures")
    out_dir.mkdir(parents=True, exist_ok=True)

    fixtures = [
        case_filter_age_gt_30(),
        # add more scenarios here
    ]

    for fx in fixtures:
        path = out_dir / f"{fx['name']}.json"
        path.write_text(json.dumps(fx, indent=2))


if __name__ == "__main__":
    main()
```

Run this script manually (or via a Makefile target) whenever you add or update scenarios.

---

## 4. Rust Test Harness (`tests/parity.rs`)

The file `tests/parity.rs` already contains a **skeleton**:

- It defines `Fixture`, `InputSection`, `ColumnSpec`, `ExpectedSection`, and `Operation` to mirror the JSON format.
- It has a `#[test] #[ignore] fn pyspark_parity_fixtures()` that:
  - Scans `tests/fixtures/`.
  - Deserializes each JSON fixture.
  - Runs a small `run_fixture_smoke` check (currently just sanity-checks the fixture).

### Evolving the harness

As the Rust API stabilizes, replace the `run_fixture_smoke` body with:

1. **Input reconstruction**
   - Convert `input.schema` + `input.rows` into a `DataFrame`:
     - Initially via a helper that uses `DataFrame::from_polars` on a Polars-built frame.
     - Later via a proper `SparkSession::createDataFrame`.

2. **Operation dispatcher**
   - For each `Operation`:
     - `filter`: parse the simple `expr` and build an equivalent `Column` / `Expr` in Rust.
     - `select`: call your `DataFrame::select`.
     - `orderBy`: translate to sort calls.
     - `groupBy` / `agg`: group by columns, apply single or multiple aggregations.
     - `join`: join with right DataFrame (requires `right_input` in fixture).
     - `withColumn`: add computed columns (when/coalesce, arithmetic, window expressions).
     - `window`: add window function columns (row_number, rank, dense_rank, lag, lead).

3. **Comparison**
   - Collect the final `DataFrame` to a simple schema + rows representation.
   - Compare against `expected`:
     - Exact equality for now.
     - Introduce tolerances and more nuanced checks later (e.g. for floats, order-insensitive cases).

4. **Un-ignore the test**
   - Once at least one end-to-end parity path works reliably, remove `#[ignore]` so CI runs it.

---

## 5. Workflow for Adding a New Parity Test

1. **Pick a PySpark behavior to cover**
   - Example: `groupBy("name").count()` with nulls in the grouping column.

2. **Add a scenario function** in `gen_pyspark_cases.py`
   - Build the PySpark input `DataFrame`.
   - Apply the desired operations.
   - Emit a fixture JSON.

3. **Regenerate fixtures**
   - Run `python tests/gen_pyspark_cases.py`.

4. **Update Rust harness if needed**
   - If the new scenario uses an operation not yet supported by the dispatcher, update `Operation` and `apply_op` logic.

5. **Run tests**
   - `cargo test -- --ignored pyspark_parity_fixtures` (while the test is still `#[ignore]`).
   - Once stable, remove `#[ignore]` to run in normal CI.

---

## 6. Tracking Coverage

Maintain a simple matrix (e.g. in `PARITY_STATUS.md` or `ROADMAP.md`) with:

- Rows: operations / behaviors (e.g. `filter >`, `filter == null`, `groupBy+count`, `inner join`, etc.).
- Columns: data-type combinations, null presence, edge cases.
- Cell values: “covered by fixture X”, “not yet covered”, or “intentionally diverges from PySpark”.

This makes it clear where Robin Sparkless truly emulates PySpark today and where work remains.

---

## 7. Sparkless Fixture Conversion

Sparkless uses a different fixture format (`input_data` as dict rows, `expected_output` with `schema`/`data`). A converter can:

1. Read Sparkless `tests/expected_outputs/*.json`
2. Map `input_data` → `input.schema` + `input.rows`
3. Infer or annotate `operations` from the test name/category
4. Map `expected_output` → `expected.schema` + `expected.rows`
5. Write robin-sparkless fixtures to `tests/fixtures/`

This lets both projects validate against the same logical test cases. See [SPARKLESS_INTEGRATION_ANALYSIS.md](SPARKLESS_INTEGRATION_ANALYSIS.md) §4.1–4.4 for details.


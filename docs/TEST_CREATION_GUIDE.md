# Test Creation Guide: PySpark Parity via Fixtures

This document explains how to create and maintain **behavioral parity tests** between PySpark and `robin-sparkless`.

**Sparkless integration**: Robin-sparkless is designed to replace the backend of [Sparkless](https://github.com/eddiethedean/sparkless). Sparkless has 270+ expected_outputs that can be converted to robin-sparkless fixtures. See [SPARKLESS_INTEGRATION_ANALYSIS.md](SPARKLESS_INTEGRATION_ANALYSIS.md) §4 for fixture format comparison and conversion strategy.

The goal is to:
- Use **PySpark as the oracle** for correct behavior.
- Generate **JSON fixtures** from PySpark runs.
- Consume those fixtures in **Rust tests** to ensure `robin-sparkless` matches PySpark.

**Note:** Running the Rust test suite (`make test`, `make check-full`) does
**not** require PySpark or Java. PySpark/Java 17+ are only required when
**generating or refreshing** fixtures (e.g. via `tests/convert_sparkless_fixtures.py`
and `tests/regenerate_expected_from_pyspark.py`, or when running
`make sparkless-parity` with regeneration enabled).

---

## 1. Overview

We separate the flow into three layers:

1. **Scenario definition (Python + PySpark, optional)**  
   Small scripts that build PySpark `DataFrame`s, apply operations, and record the expected results when you need new fixtures or want to refresh existing ones.

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

**Requirements:** PySpark (`pip install pyspark`) and **Java 17 or newer**. Set `JAVA_HOME` to a JDK 17+ installation if you see JVM or `UnsupportedClassVersionError` when starting PySpark.

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

The file `tests/parity.rs` implements the full parity flow:

- It defines `Fixture`, `InputSection`, `ColumnSpec`, `ExpectedSection`, and `Operation` to mirror the JSON format.
- The test `pyspark_parity_fixtures`:
  - Scans `tests/fixtures/`, `tests/fixtures/converted/`, and `tests/fixtures/pyspark_extracted/`.
  - Deserializes each JSON fixture, skips any with `"skip": true`.
  - For each fixture, calls `run_fixture(&fixture)`, which:
    1. **Input reconstruction**: Builds a `DataFrame` from `input.schema` and `input.rows` (or from `input.file_source` for read_csv/read_parquet/read_json).
    2. **Operation dispatch**: Applies each `Operation` in sequence (filter, select, orderBy, groupBy, agg, join, withColumn, window, union, distinct, drop, dropna, fillna, limit, withColumnRenamed, replace, crossJoin, describe, subtract, intersect, first, head, offset, summary, etc.).
    3. **Comparison**: Collects the result to schema + rows and compares to `expected` (schema equality and row equality with optional order handling).
  - Collects all failures and reports them at the end with fixture names (so CI shows every failing fixture, not just the first).

**Running a single fixture**: Set the `PARITY_FIXTURE` environment variable to the fixture `name` (e.g. `PARITY_FIXTURE=groupby_count`) to run only that fixture. Example:

```bash
PARITY_FIXTURE=groupby_count cargo test pyspark_parity_fixtures
```

**Running by phase**: Set the `PARITY_PHASE` environment variable to run only fixtures in that phase's manifest. Phases are `a`–`g`; the mapping lives in `tests/fixtures/phase_manifest.json`. Example:

```bash
PARITY_PHASE=a cargo test pyspark_parity_fixtures
make test-parity-phase-a   # same as above
make test-parity-phases    # runs all phases (a through g)
```

Phase A = signature alignment; B = high-value functions; C = Reader/Writer; D = DataFrame methods; E = SparkSession/Catalog (no fixtures, Python-only); F = behavioral; G = fixture expansion. See [PARITY_STATUS.md](PARITY_STATUS.md) for the phase-to-fixture mapping.

### Extending the harness

When adding new operations or expression forms:

1. **New operation type**: Add a variant to the `Operation` enum in `parity.rs`, deserialize it from the fixture JSON, and handle it in `apply_operations`.
2. **New expression in filter/withColumn**: Extend `parse_with_column_expr` (and any related helpers) so the fixture `expr` string is parsed into the correct Rust `Expr`.
3. **Comparison**: Schema and row comparison already support nulls, numbers, strings, arrays; extend `values_equal` / `compare_values` if new types need special handling.

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

5. **Update phase manifest (optional)**  
   - Add the new fixture name to the appropriate phase in `tests/fixtures/phase_manifest.json` so it is included in phase-specific tests (`make test-parity-phase-X`).

6. **Run tests**
   - `cargo test pyspark_parity_fixtures` (runs all fixtures).
   - To run a single fixture: `PARITY_FIXTURE=<name> cargo test pyspark_parity_fixtures`.
   - To run by phase: `PARITY_PHASE=a cargo test pyspark_parity_fixtures` or `make test-parity-phase-a`.

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


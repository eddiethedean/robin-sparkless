# Sparkless → Robin-Sparkless Fixture Converter

The script `tests/convert_sparkless_fixtures.py` converts Sparkless `expected_outputs` JSON to robin-sparkless fixture format. Use it when you have the [Sparkless](https://github.com/eddiethedean/sparkless) repo or a copy of its `tests/expected_outputs/` directory.

## Usage

```bash
# Convert a single Sparkless JSON file
python tests/convert_sparkless_fixtures.py /path/to/sparkless/expected_outputs/some_test.json tests/fixtures

# Convert all JSON files in a Sparkless expected_outputs directory (output to tests/fixtures/converted/)
python tests/convert_sparkless_fixtures.py --batch /path/to/sparkless/tests/expected_outputs tests/fixtures --output-subdir converted
```

**Phase 5 – Run parity on converted fixtures:**

```bash
# Optional: set path to Sparkless expected_outputs; converter runs first, then parity test
export SPARKLESS_EXPECTED_OUTPUTS=/path/to/sparkless/tests/expected_outputs
make sparkless-parity

# Or run parity only (hand-written + tests/fixtures/converted/*.json)
cargo test pyspark_parity_fixtures
```

To run Phase 5 conversion, clone [Sparkless](https://github.com/eddiethedean/sparkless) and set `SPARKLESS_EXPECTED_OUTPUTS` to `path/to/sparkless/tests/expected_outputs`.

## Format mapping

| Sparkless | Robin-sparkless |
|-----------|-----------------|
| `input_data` (list of dicts) | `input.schema` + `input.rows` (column-order arrays) |
| `right_input_data` / `second_input_data` | `right_input` (schema + rows) for join/union |
| `expected_output.schema` (field_names/field_types or fields) | `expected.schema` (array of `{name, type}`) |
| `expected_output.data` (list of dicts) | `expected.rows` (column-order arrays) |
| `operation` (e.g. filter_operations, groupby, join) | `operations` (array of `{op, ...}`) |

## Operation mapping (supported)

- **filter_operations** / **filter** → `{ "op": "filter", "expr": "..." }`. Use `filter_expr` in Sparkless JSON if present.
- **select** → `{ "op": "select", "columns": [...] }`
- **groupby** / **group_by** → `{ "op": "groupBy", "columns": [...] }` + `{ "op": "agg", "aggregations": [...] }`
- **orderBy** / **order_by** → `{ "op": "orderBy", "columns": [...], "ascending": [...] }`
- **join** → `right_input` from `right_input_data` / `second_input_data`; `{ "op": "join", "on": [...], "how": "inner"|"left"|"right"|"outer" }`. Use `join_on` / `on`, `join_how` / `how` in Sparkless JSON.
- **window** → `{ "op": "window", "column": "...", "func": "row_number"|"rank"|"dense_rank"|"lag"|"lead", "partition_by": [...], "order_by": [...], "value_column": "..." }`. Use `partition_by`/`partition_cols`, `order_by`/`order_cols`, `window_func`/`func`, `value_column`, `output_column`.
- **withColumn** / **transformations** → `{ "op": "withColumn", "column": "...", "expr": "..." }`. Use `with_column_name`/`column_name`, `with_column_expr`/`expr`.
- **union** / **union_all** → `{ "op": "union" }`. For union by name, use **union_by_name** → `{ "op": "unionByName" }`. Second DataFrame from `right_input_data` when present.
- **distinct** / **drop_duplicates** → `{ "op": "distinct", "subset": [...] }`
- **drop** → `{ "op": "drop", "columns": [...] }`. Use `columns` / `drop_columns`.
- **dropna** / **drop_null** → `{ "op": "dropna", "subset": [...] }`
- **fillna** / **fill_null** → `{ "op": "fillna", "value": ... }`. Use `value` / `fill_value`.
- **limit** / **head** → `{ "op": "limit", "n": N }`. Use `n` / `limit`.
- **withColumnRenamed** / **rename** → `{ "op": "withColumnRenamed", "existing": "...", "new": "..." }`. Use `existing`/`old_name`, `new`/`new_name`.

## Parity discovery and skip

- **Fixtures**: Parity test runs all `tests/fixtures/*.json` and `tests/fixtures/converted/*.json` (if present). Converted fixtures are written with `--output-subdir converted` so they do not overwrite hand-written fixtures.
- **Skip**: Add `"skip": true` and optional `"skip_reason": "..."` in a fixture JSON to skip it in the parity test (e.g. known unsupported function or semantic difference). See [SPARKLESS_PARITY_STATUS.md](SPARKLESS_PARITY_STATUS.md) for recording pass/fail and failure reasons.

## Converter status

- **Script**: Implemented in `tests/convert_sparkless_fixtures.py`. Schema and row conversion, `right_input` for join/union, and operation mapping for filter, select, groupBy, agg, orderBy, join, window, withColumn, union, unionByName, distinct, drop, dropna, fillna, limit, withColumnRenamed are in place.
- **Target**: Convert Sparkless `expected_outputs` with `--batch` and `--output-subdir converted`; run `make sparkless-parity` (set `SPARKLESS_EXPECTED_OUTPUTS` when Sparkless repo is available). Goal: 50+ tests passing (hand-written + converted). Current: 80 hand-written fixtures passing.
- **Tests that may fail after conversion**: Unsupported expressions, missing functions, or semantic differences. Fix converter where possible; add `skip: true` and document in [SPARKLESS_PARITY_STATUS.md](SPARKLESS_PARITY_STATUS.md).

## Related

- [SPARKLESS_PARITY_STATUS.md](SPARKLESS_PARITY_STATUS.md) – pass/fail counts and failure reasons for converted fixtures
- [SPARKLESS_INTEGRATION_ANALYSIS.md](SPARKLESS_INTEGRATION_ANALYSIS.md) §4 – fixture format comparison and conversion strategy
- [TEST_CREATION_GUIDE.md](TEST_CREATION_GUIDE.md) – robin-sparkless fixture format and test flow

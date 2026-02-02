# Sparkless → Robin-Sparkless Fixture Converter

The script `tests/convert_sparkless_fixtures.py` converts Sparkless `expected_outputs` JSON to robin-sparkless fixture format. Use it when you have the [Sparkless](https://github.com/eddiethedean/sparkless) repo or a copy of its `tests/expected_outputs/` directory.

## Usage

```bash
# Convert a single Sparkless JSON file
python tests/convert_sparkless_fixtures.py /path/to/sparkless/expected_outputs/some_test.json tests/fixtures

# Convert all JSON files in a Sparkless expected_outputs directory
python tests/convert_sparkless_fixtures.py --batch /path/to/sparkless/tests/expected_outputs tests/fixtures
```

## Format mapping

| Sparkless | Robin-sparkless |
|-----------|-----------------|
| `input_data` (list of dicts) | `input.schema` + `input.rows` (column-order arrays) |
| `expected_output.schema` (field_names/field_types or fields) | `expected.schema` (array of `{name, type}`) |
| `expected_output.data` (list of dicts) | `expected.rows` (column-order arrays) |
| `operation` (e.g. filter_operations, groupby) | `operations` (array of `{op, ...}`) |

## Operation mapping (supported)

- **filter_operations** → `{ "op": "filter", "expr": "..." }`. If Sparkless JSON does not include `filter_expr`, the script uses a default; for accurate conversion, add `filter_expr` in the source or edit the generated fixture.
- **select** → `{ "op": "select", "columns": [...] }`
- **groupby** / **group_by** → `{ "op": "groupBy", "columns": [...] }` + `{ "op": "agg", "aggregations": [...] }`
- **orderBy** / **order_by** → `{ "op": "orderBy", "columns": [...], "ascending": [...] }`

## Not yet mapped

- **join**: Sparkless join tests use a different structure (two inputs); the converter does not yet emit `right_input` and `join` op. Hand-convert or extend the script.
- **window**: Window operations need `partition_by`, `order_by`, `func`; map from Sparkless window test format.
- **withColumn** / **transformations**: Map from Sparkless transformation tests; may need `withColumn` expr strings.

## Converter status

- **Script**: Implemented in `tests/convert_sparkless_fixtures.py`. Schema and row conversion (dicts ↔ column arrays) and basic operation mapping (filter, select, groupBy, agg, orderBy) are in place.
- **Target**: Convert 10–20 high-value Sparkless tests once Sparkless `expected_outputs` are available (e.g. clone Sparkless and run with `--batch`).
- **Tests that cannot be converted yet**: Any test that uses join (needs `right_input`), window (needs full window spec), or unsupported functions. After conversion, run `cargo test pyspark_parity_fixtures` and fix or skip failing fixtures.

## Related

- [SPARKLESS_INTEGRATION_ANALYSIS.md](SPARKLESS_INTEGRATION_ANALYSIS.md) §4 – fixture format comparison and conversion strategy
- [TEST_CREATION_GUIDE.md](TEST_CREATION_GUIDE.md) – robin-sparkless fixture format and test flow

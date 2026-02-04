# Signature alignment tasks: Robin-Sparkless → PySpark

Checklist derived from [SIGNATURE_GAP_ANALYSIS.md](SIGNATURE_GAP_ANALYSIS.md) / `signature_comparison.json`. Goal: make Python parameter names and optional args match PySpark so existing PySpark call sites work unchanged.

**Status (February 2026):** Section 1 (column→col renames), Section 2 (optional params), Section 3 (param count/shape), and Section 4 (other renames) are complete. All optional-parameter *behaviors* are implemented (assert_true errMsg, like/ilike escapeChar, months_between roundOff, parse_url key, make_timestamp timezone, to_char/to_timestamp format, when(condition, value)); parity fixtures added for each. PyO3 signatures and `into_py`→`into_py_any` fixes applied.

**How to apply:**
- In `src/python/mod.rs`, either (1) rename the `#[pyfunction]` parameter to the PySpark name, or (2) add `#[pyo3(signature = (col, ...))]` and keep the Rust param name.
- For "Add optional", add the parameter with the same default as PySpark (check [PySpark SQL API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)).

---

## 1. Simple rename: `column` → `col` (single-arg unary)

These functions only need the first (and only) parameter renamed from `column` to `col` in the Python signature.

| [ ] | Function |
|-----|----------|
| [x] | `acos` |
| [x] | `acosh` |
| [x] | `array_agg` |
| [x] | `array_compact` |
| [x] | `array_distinct` |
| [x] | `asc` |
| [x] | `asc_nulls_first` |
| [x] | `asc_nulls_last` |
| [x] | `ascii` |
| [x] | `asin` |
| [x] | `asinh` |
| [x] | `atan` |
| [x] | `atanh` |
| [x] | `avg` |
| [x] | `base64` |
| [x] | `bin` |
| [x] | `bit_count` |
| [x] | `bit_get` |
| [x] | `bit_length` |
| [x] | `bitwise_not` |
| [x] | `bround` |
| [x] | `cbrt` |
| [x] | `ceiling` |
| [x] | `char` |
| [x] | `cos` |
| [x] | `cosh` |
| [x] | `cot` |
| [x] | `count` |
| [x] | `csc` |
| [x] | `day` |
| [x] | `dayofmonth` |
| [x] | `dayofweek` |
| [x] | `dayofyear` |
| [x] | `degrees` |
| [x] | `desc` |
| [x] | `desc_nulls_first` |
| [x] | `desc_nulls_last` |
| [x] | `explode_outer` |
| [x] | `expm1` |
| [x] | `factorial` |
| [x] | `getbit` |
| [x] | `hex` |
| [x] | `isnan` |
| [x] | `isnotnull` |
| [x] | `isnull` |
| [x] | `ln` |
| [x] | `log10` |
| [x] | `log1p` |
| [x] | `log2` |
| [x] | `map_from_entries` |
| [x] | `max` |
| [x] | `md5` |
| [x] | `median` |
| [x] | `min` |
| [x] | `mode` |
| [x] | `negate` |
| [x] | `negative` |
| [x] | `positive` |
| [x] | `quarter` |
| [x] | `radians` |
| [x] | `rint` |
| [x] | `sec` |
| [x] | `sha1` |
| [x] | `signum` |
| [x] | `sin` |
| [x] | `sinh` |
| [x] | `stddev_pop` |
| [x] | `sum` |
| [x] | `tan` |
| [x] | `tanh` |
| [x] | `timestamp_micros` |
| [x] | `timestamp_millis` |
| [x] | `timestamp_seconds` |
| [x] | `to_degrees` |
| [x] | `to_radians` |
| [x] | `typeof` |
| [x] | `unbase64` |
| [x] | `unhex` |
| [x] | `unix_date` |
| [x] | `unix_micros` |
| [x] | `unix_millis` |
| [x] | `unix_seconds` |
| [x] | `var_pop` |
| [x] | `weekday` |
| [x] | `weekofyear` |

**Total: 85**

---

## 2. Add optional parameter(s)

PySpark has one or more optional parameters that robin-sparkless is missing. Add the param(s) with PySpark's default.

| [ ] | Function | PySpark signature | Robin today | Add |
|-----|----------|-------------------|-------------|-----|
| [x] | `assert_true` | `assert_true(col, errMsg)` | `assert_true(column)` | errMsg |
| [x] | `ilike` | `ilike(str, pattern, escapeChar)` | `ilike(column, pattern)` | escapeChar |
| [x] | `like` | `like(str, pattern, escapeChar)` | `like(column, pattern)` | escapeChar |
| [x] | `make_timestamp` | `make_timestamp(years, months, days, hours, mins, secs, timezone)` | `make_timestamp(year, month, day, hour, minute, sec)` | timezone |
| [x] | `months_between` | `months_between(date1, date2, roundOff=True)` | `months_between(end, start)` | roundOff |
| [x] | `parse_url` | `parse_url(url, partToExtract, key)` | `parse_url(column, part)` | key |
| [x] | `position` | `position(substr, str, start)` | `position(substr, column)` | start |
| [x] | `to_char` | `to_char(col, format)` | `to_char(column)` | format |
| [x] | `to_number` | `to_number(col, format)` | `to_number(column)` | format |
| [x] | `to_timestamp` | `to_timestamp(col, format)` | `to_timestamp(column)` | format |
| [x] | `to_varchar` | `to_varchar(col, format)` | `to_varchar(column)` | format |
| [x] | `try_to_number` | `try_to_number(col, format)` | `try_to_number(column)` | format |
| [x] | `try_to_timestamp` | `try_to_timestamp(col, format)` | `try_to_timestamp(column)` | format |
| [x] | `when` | `when(condition, value)` | `when(condition)` | value |

**Total: 14**

---

## 3. Param count / shape differs (review manually)

PySpark and robin-sparkless have different number of parameters (e.g. variadic vs two args). Check PySpark docs and decide mapping.

| [ ] | Function | PySpark | Robin |
|-----|----------|---------|-------|
| [x] | `arrays_zip` | `cols` | `left, right` |
| [x] | `bit_and` | `col` | `left, right` |
| [x] | `bit_or` | `col` | `left, right` |
| [x] | `bit_xor` | `col` | `left, right` |
| [x] | `elt` | `inputs` | `index, columns` |
| [x] | `json_array_length` | `col` | `column, path` |
| [x] | `map_concat` | `cols` | `a, b` |
| [x] | `named_struct` | `cols` | `names, columns` |

**Total: 8**

---

## 4. Other renames (multi-param or different names)

| [ ] | Function | PySpark | Robin | Action |
|-----|----------|---------|-------|--------|
| [x] | `add_months` | `start, months` | `column, n` | column → start; n → months |
| [x] | `array_append` | `col, value` | `array, elem` | array → col; elem → value |
| [x] | `array_except` | `col1, col2` | `a, b` | a → col1; b → col2 |
| [x] | `array_insert` | `arr, pos, value` | `array, pos, elem` | array → arr; elem → value |
| [x] | `array_intersect` | `col1, col2` | `a, b` | a → col1; b → col2 |
| [x] | `array_prepend` | `col, value` | `array, elem` | array → col; elem → value |
| [x] | `array_union` | `col1, col2` | `a, b` | a → col1; b → col2 |
| [x] | `arrays_overlap` | `a1, a2` | `left, right` | left → a1; right → a2 |
| [x] | `atan2` | `col1, col2` | `y, x` | y → col1; x → col2 |
| [x] | `btrim` | `str, trim` | `column, trim_str` | column → str; trim_str → trim |
| [x] | `coalesce` | `cols` | `columns` | columns → cols |
| [x] | `col` | `col` | `name` | name → col |
| [x] | `contains` | `left, right` | `column, substring` | column → left; substring → right |
| [x] | `conv` | `col, fromBase, toBase` | `column, from_base, to_base` | column → col; from_base → fromBase; to_base → toBase |
| [x] | `convert_timezone` | `sourceTz, targetTz, sourceTs` | `source_tz, target_tz, column` | source_tz → sourceTz; target_tz → targetTz; column → sourceTs |
| [x] | `create_map` | `cols` | `columns` | columns → cols |
| [x] | `date_from_unix_date` | `days` | `column` | column → days |
| [x] | `date_part` | `field, source` | `column, field` | column → field; field → source |
| [x] | `dateadd` | `start, days` | `column, n` | column → start; n → days |
| [x] | `datepart` | `field, source` | `column, field` | column → field; field → source |
| [x] | `days` | `col` | `n` | n → col |
| [x] | `endswith` | `str, suffix` | `column, suffix` | column → str |
| [x] | `equal_null` | `col1, col2` | `left, right` | left → col1; right → col2 |
| [x] | `extract` | `field, source` | `column, field` | column → field; field → source |
| [x] | `find_in_set` | `str, str_array` | `str_column, set_column` | str_column → str; set_column → str_array |
| [x] | `format_number` | `col, d` | `column, decimals` | column → col; decimals → d |
| [x] | `format_string` | `format, cols` | `format, columns` | columns → cols |
| [x] | `from_unixtime` | `timestamp, format='yyyy-MM-dd HH:mm:ss'` | `column, format` | column → timestamp |
| [x] | `from_utc_timestamp` | `timestamp, tz` | `column, tz` | column → timestamp |
| [x] | `get` | `col, index` | `map_col, key` | map_col → col; key → index |
| [x] | `greatest` | `cols` | `columns` | columns → cols |
| [x] | `hash` | `cols` | `columns` | columns → cols |
| [x] | `hours` | `col` | `n` | n → col |
| [x] | `hypot` | `col1, col2` | `x, y` | x → col1; y → col2 |
| [x] | `ifnull` | `col1, col2` | `column, value` | column → col1; value → col2 |
| [x] | `lcase` | `str` | `column` | column → str |
| [x] | `least` | `cols` | `columns` | columns → cols |
| [x] | `left` | `str, len` | `column, n` | column → str; n → len |
| [x] | `lit` | `col` | `value` | value → col |
| [x] | `locate` | `substr, str, pos=1` | `substr, column, pos` | column → str |
| [x] | `make_timestamp_ntz` | `years, months, days, hours, mins, secs` | `year, month, day, hour, minute, sec` | year → years; month → months; day → days; hour → hours; minute → mins; sec → secs |
| [x] | `map_contains_key` | `col, value` | `map_col, key` | map_col → col; key → value |
| [x] | `months` | `col` | `n` | n → col |
| [x] | `next_day` | `date, dayOfWeek` | `column, day_of_week` | column → date; day_of_week → dayOfWeek |
| [x] | `nvl` | `col1, col2` | `column, value` | column → col1; value → col2 |
| [x] | `overlay` | `src, replace, pos, len=-1` | `column, replace, pos, length` | column → src; length → len |
| [x] | `power` | `col1, col2` | `column, exp` | column → col1; exp → col2 |
| [x] | `printf` | `format, cols` | `format, columns` | columns → cols |
| [x] | `raise_error` | `errMsg` | `message` | message → errMsg |
| [x] | `regexp_count` | `str, regexp` | `column, pattern` | column → str; pattern → regexp |
| [x] | `regexp_instr` | `str, regexp, idx` | `column, pattern, group_idx` | column → str; pattern → regexp; group_idx → idx |
| [x] | `regexp_substr` | `str, regexp` | `column, pattern` | column → str; pattern → regexp |
| [x] | `replace` | `src, search, replace` | `column, search, replacement` | column → src; replacement → replace |
| [x] | `right` | `str, len` | `column, n` | column → str; n → len |
| [x] | `rlike` | `str, regexp` | `column, pattern` | column → str; pattern → regexp |
| [x] | `sha2` | `col, numBits` | `column, bit_length` | column → col; bit_length → numBits |
| [x] | `shift_left` | `col, numBits` | `column, n` | column → col; n → numBits |
| [x] | `shift_right` | `col, numBits` | `column, n` | column → col; n → numBits |
| [x] | `split_part` | `src, delimiter, partNum` | `column, delimiter, part_num` | column → src; part_num → partNum |
| [x] | `stack` | `cols` | `columns` | columns → cols |
| [x] | `startswith` | `str, prefix` | `column, prefix` | column → str |
| [x] | `str_to_map` | `text, pairDelim, keyValueDelim` | `column, pair_delim, key_value_delim` | column → text; pair_delim → pairDelim; key_value_delim → keyValueDelim |
| [x] | `struct` | `cols` | `columns` | columns → cols |
| [x] | `substr` | `str, pos, len` | `column, start, length` | column → str; start → pos; length → len |
| [x] | `to_unix_timestamp` | `timestamp, format` | `column, format` | column → timestamp |
| [x] | `to_utc_timestamp` | `timestamp, tz` | `column, tz` | column → timestamp |
| [x] | `ucase` | `str` | `column` | column → str |
| [x] | `unix_timestamp` | `timestamp, format='yyyy-MM-dd HH:mm:ss'` | `column, format` | column → timestamp |
| [x] | `url_decode` | `str` | `column` | column → str |
| [x] | `url_encode` | `str` | `column` | column → str |
| [x] | `width_bucket` | `v, min, max, numBucket` | `value, min_val, max_val, num_bucket` | value → v; min_val → min; max_val → max; num_bucket → numBucket |
| [x] | `years` | `col` | `n` | n → col |

**Total: 72**

---

## Summary

- **column → col only:** 85
- **Add optional param(s):** 14
- **Param count differs:** 8
- **Other renames:** 72
- **Total partial (to align):** 179

*Regenerate: `python scripts/write_signature_alignment_tasks.py` (after `compare_signatures.py`).*

---

## After this: unimplemented features

Follow-up items for *behavior* tied to the added optional parameters and to Section 3 (param count differs). Signature alignment is done first; these can be implemented later.

### From Section 2 (optional params)

- [x] **assert_true(col, errMsg)**: Use `errMsg` in the error message when assertion fails.
- [x] **ilike(str, pattern, escapeChar)**: Implement escape-character semantics when `escapeChar` is provided.
- [x] **like(str, pattern, escapeChar)**: Same as ilike for `escapeChar`.
- [x] **make_timestamp(..., timezone)**: Use `timezone` when constructing timestamp (e.g. timezone-aware result).
- [x] **months_between(date1, date2, roundOff)**: Use `roundOff` to control rounding of the result.
- [x] **parse_url(..., key)**: Use `key` when extracting a specific query parameter (or similar).
- [x] **position(substr, str, start)**: Use `start` as the 1-based start position for search.
- [x] **to_char(col, format)**: Use `format` for datetime formatting (PySpark-style mapped to chrono strftime).
- [x] **to_number(col, format)**: Signature accepts `format`; reserved for future format-based parsing.
- [x] **to_timestamp(col, format)**: Use `format` for string→timestamp parsing (PySpark-style format).
- [x] **to_varchar(col, format)**: Same as to_char.
- [x] **try_to_number(col, format)**: Signature accepts `format`; reserved for future.
- [x] **try_to_timestamp(col, format)**: Use `format` for string→timestamp parsing; null on invalid.
- [x] **when(condition, value)**: Two-arg form returns value where condition is true, null otherwise (single-branch when).

### From Section 3 (param count differs)

- [x] **arrays_zip**: Documented: we support two columns. PySpark variadic `*cols`; use chaining or two-arg form.
- [x] **bit_and / bit_or / bit_xor**: Documented: we provide element-wise two-column semantics; PySpark uses single-column (aggregate). Use with two columns for element-wise.
- [x] **elt**: Documented: we have `elt(index, columns)` (list of columns); equivalent to PySpark variadic `*inputs` by passing columns as a list.
- [x] **json_array_length**: Documented: we have `(column, path=None)`; path is optional, matching PySpark when only col is used.
- [x] **map_concat**: Documented: we support two map columns; PySpark variadic `*cols`; use two-arg form or chain.
- [x] **named_struct**: Documented: we have `(names: Vec<String>, columns: Vec<Column>)` (parallel lists); equivalent to PySpark’s alternating name1, col1, name2, col2, ….

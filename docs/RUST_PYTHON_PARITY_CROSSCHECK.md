# Rust ↔ Python API parity cross-check

This document reports the cross-check between Rust functionality and Python bindings (PyColumn methods and module-level `F.xxx` functions). It identifies **gaps**: Rust has the feature but Python does not expose it (or the `.pyi` declares it but the implementation is missing).

**Scope:** Column expression API (Rust `Column` / `functions::*`) vs Python `robin_sparkless` (PyColumn + module-level functions). DataFrame and SparkSession are largely covered by existing parity docs.

---

## Summary

- **Covered:** Most Column methods and expression functions are exposed either as `col.xxx()` (PyColumn) or as `F.xxx(col)` (module-level). Sort order, window functions, aggregations, and most string/math/date functions are present.
- **Previously missing, now implemented (as of parity fix):** The 14 bindings below (string: `length`, `trim`, `ltrim`, `rtrim`, `repeat`, `reverse`, `initcap`; regex: `regexp_extract`, `regexp_replace`; math: `floor`, `round`, `exp`; hash/other: `levenshtein`, `crc32`, `xxhash64`) are now exposed as both module-level functions and PyColumn methods, with matching stubs in `robin_sparkless.pyi`. See `tests/python/test_missing_bindings_parity.py`.

---

## 1. Former gaps (now implemented)

The following were implemented in Rust but not exposed in Python; they are now fully wired and type-stubbed.

- **String:** `length`, `trim`, `ltrim`, `rtrim`, `repeat`, `reverse`, `initcap`
- **Regex:** `regexp_extract`, `regexp_replace`
- **Math:** `floor`, `round`, `exp`
- **Hash/other:** `levenshtein`, `crc32`, `xxhash64`

---

## 2. Exposed correctly (sample)

- **Column methods:** `alias`, `asc`/`desc` (and nulls variants), `between`, `eq_null_safe`, comparisons, arithmetic, `cast`/`try_cast`, `substr`, `length`, `trim`, `ltrim`, `rtrim`, `repeat`, `reverse`, `initcap`, `regexp_extract`, `regexp_replace`, `floor`, `round`, `exp`, `levenshtein`, `crc32`, `xxhash64`, `upper`/`lower`, `like`/`ilike`/`rlike`, `split`, `split_part`, `get_json_object`, `json_tuple`, window (`over`, `rank`, `dense_rank`, `lag`, `lead`, etc.), array/map helpers (e.g. `array_append`, `map_concat`), many math/trig functions, date/time extractors, etc.
- **Module-level:** `col`, `lit`, `when`, `coalesce`, `sum`/`avg`/`min`/`max`/`count`, `asc`/`desc` (sort), `bround`, `negate`, `floor`, `round`, `exp`, `length`, `trim`, `ltrim`, `rtrim`, `repeat`, `reverse`, `initcap`, `regexp_extract`, `regexp_replace`, `levenshtein`, `crc32`, `xxhash64`, `cot`/`csc`/`sec`, `e`/`pi`, `median`/`mode`, `stddev_pop`/`var_pop`, `btrim`, `locate`, `conv`, `hex`/`unhex`/`bin`/`getbit`, `to_char`/`to_varchar`/`to_number`/`try_to_number`/`try_to_timestamp`, `str_to_map`, `arrays_overlap`/`arrays_zip`, `explode_outer`, `inline`/`inline_outer`, `sequence`, `shuffle`, `array_agg`, date/time builders and extractors, `isin`, `url_decode`/`url_encode`, `shift_left`/`shift_right`, bit ops, `version`, `equal_null`, `parse_url`, `hash`, `rand`/`randn`, and many others.

---

## 3. How this was produced

- **Rust:** `src/column.rs` (Column methods) and `src/functions.rs` (standalone `pub fn` taking `&Column` or returning `Column`).
- **Python:** `src/python/column.rs` (PyColumn methods) and `src/python/mod.rs` (`m.add("name", wrap_pyfunction!(py_xxx, m)?)` and `fn py_xxx(...)`).
- **Stub:** `robin_sparkless.pyi` (module-level `def xxx(...)` and `class Column` methods).

Comparison: for each Rust Column/function that is part of the public expression API, check for a corresponding PyColumn method or a `py_xxx` + `m.add` entry; also check that `.pyi` does not declare symbols that have no implementation.

---

*Generated as a one-time cross-check of Rust vs Python exposure. Update this doc when adding or removing APIs.*

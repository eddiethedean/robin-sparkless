# Spark DDL Parser (Rust) – Integrated

**Status: Done.** Robin-sparkless now uses the [spark-ddl-parser](https://crates.io/crates/spark-ddl-parser) crate from crates.io for DDL schema parsing in `createDataFrame(data, schema="...")`. The hand-rolled `split_ddl_top_level` logic has been replaced.

## Original motivation

Port the Python [spark-ddl-parser](https://github.com/eddiethedean/spark-ddl-parser) to a Rust crate so robin-sparkless (and others) can parse PySpark DDL schema strings in Rust without calling Python.

## Reference

- **Python package**: [spark-ddl-parser on PyPI](https://pypi.org/project/spark-ddl-parser/)
- **Repo**: https://github.com/eddiethedean/spark-ddl-parser
- **API**: `parse_ddl_schema(ddl_string: str) -> StructType`; returns structured types (StructType, StructField, DataType variants).

---

## Scope

### Types to support (match Python)

- [ ] **Simple types**: `string`, `int`, `integer`, `long`, `bigint`, `double`, `float`, `short`, `smallint`, `byte`, `tinyint`, `boolean`, `bool`, `date`, `timestamp`, `binary`
- [ ] **Arrays**: `array<element_type>`, e.g. `array<string>`, `array<long>`
- [ ] **Maps**: `map<key_type,value_type>`, e.g. `map<string,int>`
- [ ] **Structs**: `struct<field:type,...>`, including nested structs
- [ ] **Decimal**: `decimal(precision,scale)` e.g. `decimal(10,2)`

### Parsing behavior

- [ ] **Separators**: Both `name type` (space) and `name:type` (colon)
- [ ] **Top-level**: Comma-separated list of fields; commas inside `<...>` or `(...)` must not split (bracket-aware)
- [ ] **Struct fields**: Same format inside `struct<...>` (name/type with space or colon)
- [ ] **Whitespace**: Trim and allow newlines between fields
- [ ] **Errors**: Clear messages for invalid DDL (missing type, unbalanced brackets, etc.)

### Deliverables

- [ ] New crate (e.g. `spark-ddl-parser` or `spark_ddl_parser`) with `Cargo.toml`, README, LICENSE
- [ ] Public API: `parse_ddl_schema(ddl: &str) -> Result<StructType, ParseError>`
- [ ] Type model: `StructType`, `StructField`, `DataType` enum (Simple, Array, Map, Struct, Decimal)
- [ ] Optional: `StructField` has `nullable: bool` (default true) if we want full parity
- [ ] Unit tests covering: flat schema, nested struct, array, map, decimal, colon vs space, error cases
- [ ] Integration: Use the crate in robin-sparkless `parse_schema_param()` for DDL strings (replace current simple split-on-comma logic)

---

## Tasks (ordered)

1. [ ] **Create crate layout**  
   Add `crates/spark-ddl-parser/` (or top-level `spark-ddl-parser/`) with `Cargo.toml`, `src/lib.rs`, `README.md`. Add to workspace if using a workspace.

2. [ ] **Define type model**  
   - `DataType` enum: `Simple(String)`, `Array(Box<DataType>)`, `Map(Box<DataType>, Box<DataType>)`, `Struct(StructType)`, `Decimal(u32, u32)`  
   - `StructField { name: String, data_type: DataType, nullable: bool }`  
   - `StructType { fields: Vec<StructField> }`  
   - Implement `Display` / `Debug` and optionally `Serialize` for debugging/serialization.

3. [ ] **Implement lexer/tokenizer**  
   - Tokenize: identifiers, `<`, `>`, `,`, `:`, `(`, `)`, whitespace (or skip).  
   - Bracket-aware splitting at top level: scan for commas at depth 0 (count `<` and `>` and `(` `)`).

4. [ ] **Implement parser**  
   - Parse top-level: `field_def ("," field_def)*`  
   - Field: `identifier (":"|" ") data_type`  
   - Data type: simple word | `array "<" data_type ">"` | `map "<" data_type "," data_type ">"` | `struct "<" field_def_list ">"` | `decimal "(" number "," number ")"`  
   - Recursive for nested structs/array/map.

5. [ ] **Error handling**  
   - Custom `ParseError` (or use a crate like `thiserror`).  
   - Report position or snippet where parsing failed when possible.

6. [ ] **Tests**  
   - From Python package: copy or port representative tests (simple, nested struct, array, map, decimal, colon format, invalid DDL).  
   - Add tests for edge cases: empty string, single field, deeply nested.

7. [ ] **Documentation**  
   - README with examples matching Python quick start.  
   - Doc comments on public types and `parse_ddl_schema`.  
   - Note compatibility with PySpark DDL / spark-ddl-parser (Python).

8. [ ] **Integrate into robin-sparkless**  
   - Add dependency on `spark-ddl-parser` (path or crates.io if published).  
   - In `src/python/session.rs` (or shared Rust code): when schema is a string, call `spark_ddl_parser::parse_ddl_schema(ddl)` and convert `StructType` to our `Vec<(String, String)>` (flatten or pass type strings as needed for `create_dataframe_from_rows`).  
   - Remove or narrow the current hand-rolled DDL split logic.  
   - Run existing createDataFrame tests (including DDL tests) and fix any behavior differences.

9. [ ] **Publish (optional)**  
   - Publish crate to crates.io.  
   - Add repository link (e.g. under same org as robin-sparkless or spark-ddl-parser Python).

---

## Notes

- **No external parser dependency required**: A recursive-descent or hand-written parser is enough; no need for a full SQL parser.
- **Output format for robin-sparkless**: Our `create_dataframe_from_rows` uses `Vec<(String, String)>` (name, dtype_str). The Rust DDL parser’s `StructType` can be converted to that by turning each `DataType` into a string (e.g. `long`, `string`, `array<bigint>`, `struct<...>`) so we don’t need to change the rest of the pipeline.
- **Python parity**: Aim for same accepted DDL and same structure; type name normalization (e.g. `integer` → `int`) can match Python for compatibility.

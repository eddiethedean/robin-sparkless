//! Strings expression builders.

use super::imports::*;

/// Convert string column to uppercase (PySpark upper)
pub fn upper(column: &Column) -> Column {
    column.clone().upper()
}

/// Convert string column to lowercase (PySpark lower)
pub fn lower(column: &Column) -> Column {
    column.clone().lower()
}

/// Substring with 1-based start (PySpark substring semantics)
pub fn substring(column: &Column, start: i64, length: Option<i64>) -> Column {
    column.clone().substr(start, length)
}

/// Substring with Column start/length expressions (#1649).
pub fn substring_with(column: &Column, start: &Column, length: Option<&Column>) -> Column {
    column.clone().substr_with(start, length)
}

/// String length in characters (PySpark length)
pub fn length(column: &Column) -> Column {
    column.clone().length()
}

/// Trim leading and trailing whitespace (PySpark trim)
pub fn trim(column: &Column) -> Column {
    column.clone().trim()
}

/// Trim leading whitespace (PySpark ltrim)
pub fn ltrim(column: &Column) -> Column {
    column.clone().ltrim()
}

/// Trim trailing whitespace (PySpark rtrim)
pub fn rtrim(column: &Column) -> Column {
    column.clone().rtrim()
}

/// Trim leading and trailing chars (PySpark btrim). trim_str defaults to whitespace.
pub fn btrim(column: &Column, trim_str: Option<&str>) -> Column {
    column.clone().btrim(trim_str)
}

/// Find substring position 1-based, starting at pos (PySpark locate). 0 if not found.
pub fn locate(substr: &str, column: &Column, pos: i64) -> Column {
    column.clone().locate(substr, pos)
}

/// Base conversion (PySpark conv). num from from_base to to_base.
pub fn conv(column: &Column, from_base: i32, to_base: i32) -> Column {
    column.clone().conv(from_base, to_base)
}

/// Convert to hex string (PySpark hex).
pub fn hex(column: &Column) -> Column {
    column.clone().hex()
}

/// Convert hex string to binary/string (PySpark unhex).
pub fn unhex(column: &Column) -> Column {
    column.clone().unhex()
}

/// Encode string to binary (PySpark encode). Charset: UTF-8. Returns hex string.
pub fn encode(column: &Column, charset: &str) -> Column {
    column.clone().encode(charset)
}

/// Decode binary (hex string) to string (PySpark decode). Charset: UTF-8.
pub fn decode(column: &Column, charset: &str) -> Column {
    column.clone().decode(charset)
}

/// Convert to binary (PySpark to_binary). fmt: 'utf-8', 'hex'.
pub fn to_binary(column: &Column, fmt: &str) -> Column {
    column.clone().to_binary(fmt)
}

/// Try convert to binary; null on failure (PySpark try_to_binary).
pub fn try_to_binary(column: &Column, fmt: &str) -> Column {
    column.clone().try_to_binary(fmt)
}

/// AES encrypt (PySpark aes_encrypt). Key as string; AES-128-GCM.
pub fn aes_encrypt(column: &Column, key: &str) -> Column {
    column.clone().aes_encrypt(key)
}

/// AES decrypt (PySpark aes_decrypt). Input hex(nonce||ciphertext).
pub fn aes_decrypt(column: &Column, key: &str) -> Column {
    column.clone().aes_decrypt(key)
}

/// Try AES decrypt (PySpark try_aes_decrypt). Returns null on failure.
pub fn try_aes_decrypt(column: &Column, key: &str) -> Column {
    column.clone().try_aes_decrypt(key)
}

/// Convert integer to binary string (PySpark bin).
pub fn bin(column: &Column) -> Column {
    column.clone().bin()
}

/// Get bit at 0-based position (PySpark getbit).
pub fn getbit(column: &Column, pos: i64) -> Column {
    column.clone().getbit(pos)
}

/// Bitwise AND of two integer/boolean columns (PySpark bit_and).
pub fn bit_and(left: &Column, right: &Column) -> Column {
    left.clone().bit_and(right)
}

/// Bitwise OR of two integer/boolean columns (PySpark bit_or).
pub fn bit_or(left: &Column, right: &Column) -> Column {
    left.clone().bit_or(right)
}

/// Bitwise XOR of two integer/boolean columns (PySpark bit_xor).
pub fn bit_xor(left: &Column, right: &Column) -> Column {
    left.clone().bit_xor(right)
}

/// Count of set bits in the integer representation (PySpark bit_count).
pub fn bit_count(column: &Column) -> Column {
    column.clone().bit_count()
}

/// Bitwise NOT of an integer/boolean column (PySpark bitwise_not / bitwiseNOT).
pub fn bitwise_not(column: &Column) -> Column {
    column.clone().bitwise_not()
}

// --- Bitmap (PySpark 3.5+) ---

/// Map integral value (0–32767) to bit position for bitmap aggregates (PySpark bitmap_bit_position).
pub fn bitmap_bit_position(column: &Column) -> Column {
    use polars::prelude::DataType;
    let expr = column.expr().clone().cast(DataType::Int32);
    Column::from_expr(expr, None)
}

/// Bucket number for distributed bitmap (PySpark bitmap_bucket_number). value / 32768.
pub fn bitmap_bucket_number(column: &Column) -> Column {
    use polars::prelude::DataType;
    let expr = column.expr().clone().cast(DataType::Int64) / lit(32768i64);
    Column::from_expr(expr, None)
}

/// Count set bits in a bitmap binary column (PySpark bitmap_count).
pub fn bitmap_count(column: &Column) -> Column {
    use polars::prelude::{DataType, Field};
    let expr = column.expr().clone().map(
        |s| crate::column::expect_col(crate::udfs::apply_bitmap_count(s)),
        |_schema, field| Ok(Field::new(field.name().clone(), DataType::Int64)),
    );
    Column::from_expr(expr, None)
}

/// Aggregate: bitwise OR of bit positions into one bitmap binary (PySpark bitmap_construct_agg).
/// Use in group_by(...).agg([bitmap_construct_agg(col)]).
pub fn bitmap_construct_agg(column: &Column) -> polars::prelude::Expr {
    use polars::prelude::{DataType, Field};
    column.expr().clone().implode().map(
        |s| crate::column::expect_col(crate::udfs::apply_bitmap_construct_agg(s)),
        |_schema, field| Ok(Field::new(field.name().clone(), DataType::Binary)),
    )
}

/// Aggregate: bitwise OR of bitmap binary column (PySpark bitmap_or_agg).
pub fn bitmap_or_agg(column: &Column) -> polars::prelude::Expr {
    use polars::prelude::{DataType, Field};
    column.expr().clone().implode().map(
        |s| crate::column::expect_col(crate::udfs::apply_bitmap_or_agg(s)),
        |_schema, field| Ok(Field::new(field.name().clone(), DataType::Binary)),
    )
}

/// Alias for getbit (PySpark bit_get).
pub fn bit_get(column: &Column, pos: i64) -> Column {
    getbit(column, pos)
}

/// Assert that all boolean values are true; errors otherwise (PySpark assert_true).
/// When err_msg is Some, it is used in the error message when assertion fails.
pub fn assert_true(column: &Column, err_msg: Option<&str>) -> Column {
    column.clone().assert_true(err_msg)
}

/// Raise an error when evaluated (PySpark raise_error). Always fails with the given message.
pub fn raise_error(message: &str) -> Column {
    let msg = message.to_string();
    let expr = lit(0i64).map(
        move |_col| -> PolarsResult<polars::prelude::Column> {
            Err(PolarsError::ComputeError(msg.clone().into()))
        },
        |_schema, field| Ok(Field::new(field.name().clone(), DataType::Int64)),
    );
    Column::from_expr(expr, Some("raise_error".to_string()))
}

/// Stub partition id - always 0 (PySpark spark_partition_id).
pub fn spark_partition_id() -> Column {
    Column::from_expr(lit(0i32), Some("spark_partition_id".to_string()))
}

/// Stub input file name - empty string (PySpark input_file_name).
pub fn input_file_name() -> Column {
    Column::from_expr(lit(""), Some("input_file_name".to_string()))
}

/// Stub monotonically_increasing_id (JVM stub; **constant 0** for all rows).
///
/// Not a strictly increasing id — see [PYSPARK_DIFFERENCES.md](https://github.com/eddiethedean/robin-sparkless/blob/main/docs/PYSPARK_DIFFERENCES.md#jvm--runtime-stubs).
/// Callers that need uniqueness should generate ids another way (e.g. `row_number().over(...)`).
pub fn monotonically_increasing_id() -> Column {
    Column::from_expr(lit(0i64), Some("monotonically_increasing_id".to_string()))
}

/// Current catalog name stub (PySpark current_catalog).
pub fn current_catalog() -> Column {
    Column::from_expr(lit("spark_catalog"), Some("current_catalog".to_string()))
}

/// Current database/schema name stub (PySpark current_database).
pub fn current_database() -> Column {
    Column::from_expr(lit("default"), Some("current_database".to_string()))
}

/// Current schema name stub (PySpark current_schema).
pub fn current_schema() -> Column {
    Column::from_expr(lit("default"), Some("current_schema".to_string()))
}

/// Current user stub (PySpark current_user).
pub fn current_user() -> Column {
    Column::from_expr(lit("unknown"), Some("current_user".to_string()))
}

/// User stub (PySpark user).
pub fn user() -> Column {
    Column::from_expr(lit("unknown"), Some("user".to_string()))
}

/// Random uniform [0, 1) per row, with optional seed (PySpark rand).
/// When added via with_column, generates one distinct value per row (PySpark-like).
pub fn rand(seed: Option<u64>) -> Column {
    Column::from_rand(seed)
}

/// Random standard normal per row, with optional seed (PySpark randn).
/// When added via with_column, generates one distinct value per row (PySpark-like).
pub fn randn(seed: Option<u64>) -> Column {
    Column::from_randn(seed)
}

/// Call a registered UDF by name. PySpark: F.call_udf(udfName, *cols).
/// Requires a session (set by get_or_create). Raises if UDF not found.
pub fn call_udf(name: &str, cols: &[Column]) -> Result<Column, PolarsError> {
    use polars::prelude::Column as PlColumn;

    let (registry, case_sensitive) =
        crate::udf_context::get_thread_udf_context().ok_or_else(|| {
            PolarsError::InvalidOperation(
                "call_udf: no session. Use SparkSession.builder().get_or_create() first.".into(),
            )
        })?;

    // Rust UDF: build lazy Expr
    let udf = registry
        .get_rust_udf(name, case_sensitive)?
        .ok_or_else(|| {
            PolarsError::InvalidOperation(format!("call_udf: UDF '{name}' not found").into())
        })?;

    let exprs: Vec<Expr> = cols.iter().map(|c| c.expr().clone()).collect();
    let output_type = DataType::String; // PySpark default

    let expr = if exprs.len() == 1 {
        let udf = udf.clone();
        exprs.into_iter().next().unwrap().map(
            move |c| {
                let s = c.take_materialized_series();
                udf.apply(&[s]).map(|out| PlColumn::new("_".into(), out))
            },
            move |_schema, field| Ok(Field::new(field.name().clone(), output_type.clone())),
        )
    } else {
        let udf = udf.clone();
        let first = exprs[0].clone();
        let rest: Vec<Expr> = exprs[1..].to_vec();
        first.map_many(
            move |columns| {
                let series: Vec<Series> = columns
                    .iter_mut()
                    .map(|c| std::mem::take(c).take_materialized_series())
                    .collect();
                udf.apply(&series).map(|out| PlColumn::new("_".into(), out))
            },
            &rest,
            move |_schema, fields| Ok(Field::new(fields[0].name().clone(), output_type.clone())),
        )
    };

    Ok(Column::from_expr(expr, Some(format!("{name}()"))))
}

/// True if two arrays have any element in common (PySpark arrays_overlap).
pub fn arrays_overlap(left: &Column, right: &Column) -> Column {
    left.clone().arrays_overlap(right)
}

/// Zip arrays into array of structs (PySpark arrays_zip).
pub fn arrays_zip(left: &Column, right: &Column) -> Column {
    left.clone().arrays_zip(right)
}

/// Explode; null/empty yields one row with null (PySpark explode_outer).
pub fn explode_outer(column: &Column) -> Column {
    column.clone().explode_outer()
}

/// Posexplode with null preservation (PySpark posexplode_outer).
pub fn posexplode_outer(column: &Column) -> (Column, Column) {
    column.clone().posexplode_outer()
}

/// Collect to array (PySpark array_agg).
pub fn array_agg(column: &Column) -> Column {
    column.clone().array_agg()
}

/// Transform map keys by expr (PySpark transform_keys).
pub fn transform_keys(column: &Column, key_expr: Expr) -> Column {
    column.clone().transform_keys(key_expr)
}

/// Transform map values by expr (PySpark transform_values).
pub fn transform_values(column: &Column, value_expr: Expr) -> Column {
    column.clone().transform_values(value_expr)
}

/// Parse string to map (PySpark str_to_map). Default delims: "," and ":".
pub fn str_to_map(
    column: &Column,
    pair_delim: Option<&str>,
    key_value_delim: Option<&str>,
) -> Column {
    let pd = pair_delim.unwrap_or(",");
    let kvd = key_value_delim.unwrap_or(":");
    column.clone().str_to_map(pd, kvd)
}

/// Extract first match of regex (PySpark regexp_extract). group_index 0 = full match.
pub fn regexp_extract(column: &Column, pattern: &str, group_index: usize) -> Column {
    column.clone().regexp_extract(pattern, group_index)
}

/// Replace all matches of regex (PySpark regexp_replace: global replace)
pub fn regexp_replace(column: &Column, pattern: &str, replacement: &str) -> Column {
    column.clone().regexp_replace(pattern, replacement)
}

/// Split string by delimiter (PySpark split). Optional limit: at most that many parts (remainder in last).
pub fn split(column: &Column, delimiter: &str, limit: Option<i32>) -> Column {
    column.clone().split(delimiter, limit)
}

/// Title case (PySpark initcap)
pub fn initcap(column: &Column) -> Column {
    column.clone().initcap()
}

/// Extract all matches of regex (PySpark regexp_extract_all).
pub fn regexp_extract_all(column: &Column, pattern: &str) -> Column {
    column.clone().regexp_extract_all(pattern)
}

/// Check if string matches regex (PySpark regexp_like / rlike).
pub fn regexp_like(column: &Column, pattern: &str) -> Column {
    column.clone().regexp_like(pattern)
}

/// Count of non-overlapping regex matches (PySpark regexp_count).
pub fn regexp_count(column: &Column, pattern: &str) -> Column {
    column.clone().regexp_count(pattern)
}

/// First substring matching regex (PySpark regexp_substr). Null if no match.
pub fn regexp_substr(column: &Column, pattern: &str) -> Column {
    column.clone().regexp_substr(pattern)
}

/// Split by delimiter and return 1-based part (PySpark split_part).
pub fn split_part(column: &Column, delimiter: &str, part_num: i64) -> Column {
    column.clone().split_part(delimiter, part_num)
}

/// 1-based position of first regex match (PySpark regexp_instr).
pub fn regexp_instr(column: &Column, pattern: &str, group_idx: Option<usize>) -> Column {
    column.clone().regexp_instr(pattern, group_idx)
}

/// 1-based index of str in comma-delimited set (PySpark find_in_set). 0 if not found or str contains comma.
pub fn find_in_set(str_column: &Column, set_column: &Column) -> Column {
    str_column.clone().find_in_set(set_column)
}

/// Printf-style format (PySpark format_string). Supports %s, %d, %i, %f, %g, %%.
pub fn format_string(format: &str, columns: &[&Column]) -> Result<Column, PolarsError> {
    use polars::prelude::*;
    if columns.is_empty() {
        return Err(PolarsError::InvalidOperation(
            "format_string needs at least one column".into(),
        ));
    }
    let format_owned = format.to_string();
    let args: Vec<Expr> = columns.iter().skip(1).map(|c| c.expr().clone()).collect();
    let expr = columns[0].expr().clone().map_many(
        move |cols| {
            crate::column::expect_col(crate::udfs::apply_format_string(cols, &format_owned))
        },
        &args,
        |_schema, fields| Ok(Field::new(fields[0].name().clone(), DataType::String)),
    );
    Ok(crate::column::Column::from_expr(expr, None))
}

/// Alias for format_string (PySpark printf).
pub fn printf(format: &str, columns: &[&Column]) -> Result<Column, PolarsError> {
    format_string(format, columns)
}

/// Repeat string n times (PySpark repeat).
pub fn repeat(column: &Column, n: i32) -> Column {
    column.clone().repeat(n)
}

/// Reverse string (PySpark reverse).
pub fn reverse(column: &Column) -> Column {
    column.clone().reverse()
}

/// Find substring position 1-based; 0 if not found (PySpark instr).
pub fn instr(column: &Column, substr: &str) -> Column {
    column.clone().instr(substr)
}

/// Position of substring in column (PySpark position). Same as instr; (substr, col) argument order.
pub fn position(substr: &str, column: &Column) -> Column {
    column.clone().instr(substr)
}

/// ASCII value of first character (PySpark ascii). Returns Int32.
pub fn ascii(column: &Column) -> Column {
    column.clone().ascii()
}

/// Format numeric as string with fixed decimal places (PySpark format_number).
pub fn format_number(column: &Column, decimals: u32) -> Column {
    column.clone().format_number(decimals)
}

/// Replace substring at 1-based position (PySpark overlay). replace is literal.
pub fn overlay(column: &Column, replace: &str, pos: i64, length: i64) -> Column {
    column.clone().overlay(replace, pos, length)
}

/// Int to single-character string (PySpark char). Valid codepoint only.
pub fn char(column: &Column) -> Column {
    column.clone().char()
}

/// Alias for char (PySpark chr).
pub fn chr(column: &Column) -> Column {
    column.clone().chr()
}

/// Base64 encode string bytes (PySpark base64).
pub fn base64(column: &Column) -> Column {
    column.clone().base64()
}

/// Base64 decode to string (PySpark unbase64). Invalid decode → null.
pub fn unbase64(column: &Column) -> Column {
    column.clone().unbase64()
}

/// SHA1 hash of string bytes, return hex string (PySpark sha1).
pub fn sha1(column: &Column) -> Column {
    column.clone().sha1()
}

/// SHA2 hash; bit_length 256, 384, or 512 (PySpark sha2).
pub fn sha2(column: &Column, bit_length: i32) -> Column {
    column.clone().sha2(bit_length)
}

/// MD5 hash of string bytes, return hex string (PySpark md5).
pub fn md5(column: &Column) -> Column {
    column.clone().md5()
}

/// Left-pad string to length with pad char (PySpark lpad).
pub fn lpad(column: &Column, length: i32, pad: &str) -> Column {
    column.clone().lpad(length, pad)
}

/// Right-pad string to length with pad char (PySpark rpad).
pub fn rpad(column: &Column, length: i32, pad: &str) -> Column {
    column.clone().rpad(length, pad)
}

/// Character-by-character translation (PySpark translate).
pub fn translate(column: &Column, from_str: &str, to_str: &str) -> Column {
    column.clone().translate(from_str, to_str)
}

/// Mask string: replace upper/lower/digit/other with given chars (PySpark mask).
pub fn mask(
    column: &Column,
    upper_char: Option<char>,
    lower_char: Option<char>,
    digit_char: Option<char>,
    other_char: Option<char>,
) -> Column {
    column
        .clone()
        .mask(upper_char, lower_char, digit_char, other_char)
}

/// Substring before/after nth delimiter (PySpark substring_index).
pub fn substring_index(column: &Column, delimiter: &str, count: i64) -> Column {
    column.clone().substring_index(delimiter, count)
}

/// Leftmost n characters (PySpark left).
pub fn left(column: &Column, n: i64) -> Column {
    column.clone().left(n)
}

/// Rightmost n characters (PySpark right).
pub fn right(column: &Column, n: i64) -> Column {
    column.clone().right(n)
}

/// Replace all occurrences of search with replacement (literal). PySpark replace.
pub fn replace(column: &Column, search: &str, replacement: &str) -> Column {
    column.clone().replace(search, replacement)
}

/// True if string starts with prefix (PySpark startswith).
pub fn startswith(column: &Column, prefix: &str) -> Column {
    column.clone().startswith(prefix)
}

/// True if string ends with suffix (PySpark endswith).
pub fn endswith(column: &Column, suffix: &str) -> Column {
    column.clone().endswith(suffix)
}

/// True if string contains substring (literal). PySpark contains.
pub fn contains(column: &Column, substring: &str) -> Column {
    column.clone().contains(substring)
}

/// SQL LIKE pattern (% any, _ one char). PySpark like.
/// When escape_char is Some(esc), esc + char treats that char as literal.
pub fn like(column: &Column, pattern: &str, escape_char: Option<char>) -> Column {
    column.clone().like(pattern, escape_char)
}

/// Case-insensitive LIKE. PySpark ilike.
/// When escape_char is Some(esc), esc + char treats that char as literal.
pub fn ilike(column: &Column, pattern: &str, escape_char: Option<char>) -> Column {
    column.clone().ilike(pattern, escape_char)
}

/// Alias for regexp_like. PySpark rlike / regexp.
pub fn rlike(column: &Column, pattern: &str) -> Column {
    column.clone().regexp_like(pattern)
}

/// Alias for rlike (PySpark regexp).
pub fn regexp(column: &Column, pattern: &str) -> Column {
    rlike(column, pattern)
}

/// Soundex code (PySpark soundex). Not implemented: requires element-wise UDF.
pub fn soundex(column: &Column) -> Column {
    column.clone().soundex()
}

/// Levenshtein distance (PySpark levenshtein). Not implemented: requires element-wise UDF.
pub fn levenshtein(column: &Column, other: &Column) -> Column {
    column.clone().levenshtein(other)
}

/// CRC32 of string bytes (PySpark crc32). Not implemented: requires element-wise UDF.
pub fn crc32(column: &Column) -> Column {
    column.clone().crc32()
}

/// XXH64 hash (PySpark xxhash64). Not implemented: requires element-wise UDF.
pub fn xxhash64(column: &Column) -> Column {
    column.clone().xxhash64()
}

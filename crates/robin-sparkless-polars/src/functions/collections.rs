//! Collections expression builders.

use super::imports::*;
use super::misc::coalesce;

/// Create an array column from multiple columns (PySpark array).
/// With no arguments, returns a column of empty arrays (one per row); PySpark parity.
pub fn array(columns: &[&Column]) -> Result<crate::column::Column, PolarsError> {
    use polars::prelude::*;
    if columns.is_empty() {
        // PySpark F.array() with no args: one empty list per row (broadcast literal).
        // Use .first() so the single-row literal is treated as a scalar and broadcasts to frame height.
        let empty_inner = Series::new("".into(), Vec::<i64>::new());
        let list_series = ListChunked::from_iter([Some(empty_inner)])
            .with_name("array".into())
            .into_series();
        let expr = lit(list_series).first();
        return Ok(crate::column::Column::from_expr(expr, None));
    }
    let exprs: Vec<Expr> = columns.iter().map(|c| c.expr().clone()).collect();
    let expr = concat_list(exprs)
        .map_err(|e| PolarsError::ComputeError(format!("array concat_list: {e}").into()))?;
    let mut col = crate::column::Column::from_expr(expr, None);
    col.is_array_expr = true;
    Ok(col)
}

/// Number of elements in list (PySpark size / array_size). Returns Int32.
pub fn array_size(column: &Column) -> Column {
    column.clone().array_size()
}

/// Alias for array_size (PySpark size).
pub fn size(column: &Column) -> Column {
    column.clone().array_size()
}

/// Cardinality: number of elements in array (PySpark cardinality). Alias for size/array_size.
pub fn cardinality(column: &Column) -> Column {
    column.clone().cardinality()
}

/// Check if list contains value (PySpark array_contains).
pub fn array_contains(column: &Column, value: &Column) -> Column {
    column.clone().array_contains(value.expr().clone())
}

/// Join list of strings with separator (PySpark array_join).
/// If null_replacement is Some, null elements are replaced with that string.
pub fn array_join(column: &Column, separator: &str, null_replacement: Option<&str>) -> Column {
    column.clone().array_join(separator, null_replacement)
}

/// Maximum element in list (PySpark array_max).
pub fn array_max(column: &Column) -> Column {
    column.clone().array_max()
}

/// Minimum element in list (PySpark array_min).
pub fn array_min(column: &Column) -> Column {
    column.clone().array_min()
}

/// Get element at 1-based index (PySpark element_at).
pub fn element_at(column: &Column, index: i64) -> Column {
    column.clone().element_at(index)
}

/// Sort list elements (PySpark array_sort/sort_array).
/// When asc=true (default), sorts ascending with nulls first. When asc=false, sorts descending with nulls last.
pub fn array_sort(column: &Column, asc: bool) -> Column {
    column.clone().array_sort(asc)
}

/// Distinct elements in list (PySpark array_distinct).
pub fn array_distinct(column: &Column) -> Column {
    column.clone().array_distinct()
}

/// Slice list from 1-based start with optional length (PySpark slice).
pub fn array_slice(column: &Column, start: i64, length: Option<i64>) -> Column {
    column.clone().array_slice(start, length)
}

/// Generate array of numbers from start to stop (inclusive) with optional step (PySpark sequence).
/// step defaults to 1.
pub fn sequence(start: &Column, stop: &Column, step: Option<&Column>) -> Column {
    use polars::prelude::{DataType, Field, as_struct, lit};
    let step_expr = step
        .map(|c| c.expr().clone().alias("2"))
        .unwrap_or_else(|| lit(1i64).alias("2"));
    let struct_expr = as_struct(vec![
        start.expr().clone().alias("0"),
        stop.expr().clone().alias("1"),
        step_expr,
    ]);
    let out_dtype = DataType::List(Box::new(DataType::Int64));
    let expr = struct_expr.map(
        |s| crate::column::expect_col(crate::udfs::apply_sequence(s)),
        move |_schema, field| Ok(Field::new(field.name().clone(), out_dtype.clone())),
    );
    crate::column::Column::from_expr(expr, None)
}

/// Random permutation of list elements (PySpark shuffle).
pub fn shuffle(column: &Column) -> Column {
    let expr = column.expr().clone().map(
        |s| crate::column::expect_col(crate::udfs::apply_shuffle(s)),
        |_schema, field| Ok(field.clone()),
    );
    crate::column::Column::from_expr(expr, None)
}

/// Explode list of structs into rows; struct fields become columns after unnest (PySpark inline).
/// Returns the exploded struct column; use unnest to expand struct fields to columns.
pub fn inline(column: &Column) -> Column {
    column.clone().explode()
}

/// Like inline but null/empty yields one row of nulls (PySpark inline_outer).
pub fn inline_outer(column: &Column) -> Column {
    column.clone().explode_outer()
}

/// Explode list into one row per element (PySpark explode).
pub fn explode(column: &Column) -> Column {
    column.clone().explode()
}

/// 1-based index of first occurrence of value in list, or 0 if not found (PySpark array_position).
/// Implemented via Polars list.eval with col("") as element.
pub fn array_position(column: &Column, value: &Column) -> Column {
    column.clone().array_position(value.expr().clone())
}

/// Remove null elements from list (PySpark array_compact).
pub fn array_compact(column: &Column) -> Column {
    column.clone().array_compact()
}

/// New list with all elements equal to value removed (PySpark array_remove).
/// Implemented via Polars list.eval + list.drop_nulls.
pub fn array_remove(column: &Column, value: &Column) -> Column {
    column.clone().array_remove(value.expr().clone())
}

/// Repeat each element n times (PySpark array_repeat). Not implemented: would require list.eval with dynamic repeat.
pub fn array_repeat(column: &Column, n: i64) -> Column {
    column.clone().array_repeat(n)
}

/// Flatten list of lists to one list (PySpark flatten). Not implemented.
pub fn array_flatten(column: &Column) -> Column {
    column.clone().array_flatten()
}

/// True if any list element satisfies the predicate (PySpark exists).
pub fn array_exists(column: &Column, predicate: Expr) -> Column {
    column.clone().array_exists(predicate)
}

/// True if all list elements satisfy the predicate (PySpark forall).
pub fn array_forall(column: &Column, predicate: Expr) -> Column {
    column.clone().array_forall(predicate)
}

/// Filter list elements by predicate (PySpark filter).
pub fn array_filter(column: &Column, predicate: Expr) -> Column {
    column.clone().array_filter(predicate)
}

/// Transform list elements by expression (PySpark transform).
pub fn array_transform(column: &Column, f: Expr) -> Column {
    column.clone().array_transform(f)
}

/// Sum of list elements (PySpark aggregate sum).
pub fn array_sum(column: &Column) -> Column {
    column.clone().array_sum()
}

/// Array fold/aggregate (PySpark aggregate). Simplified: zero + sum(list elements).
pub fn aggregate(column: &Column, zero: &Column) -> Column {
    column.clone().array_aggregate(zero)
}

/// Mean of list elements (PySpark aggregate avg).
pub fn array_mean(column: &Column) -> Column {
    column.clone().array_mean()
}

/// Explode list with position (PySpark posexplode). Returns (pos_column, value_column).
/// pos is 1-based; implemented via list.eval(cum_count()).explode() and explode().
pub fn posexplode(column: &Column) -> (Column, Column) {
    column.clone().posexplode()
}

/// Normalize map keys per PySpark 4.0 (-0.0 -> 0.0) when map key normalization is enabled.
fn normalize_map_key_expr(key: Expr) -> Expr {
    if crate::udf_context::get_thread_runtime_config().disable_map_key_normalization {
        return key;
    }
    use polars::prelude::{lit, when};
    when(key.clone().eq(lit(-0.0f64)))
        .then(lit(0.0f64))
        .otherwise(key)
}

/// Build a map column from alternating key/value expressions (PySpark create_map).
/// Returns List(Struct{key, value}) using Polars as_struct and concat_list.
/// With no args (or empty slice), returns a column of empty maps per row (PySpark parity #275).
pub fn create_map(key_values: &[&Column]) -> Result<Column, PolarsError> {
    use polars::chunked_array::StructChunked;
    use polars::prelude::{IntoSeries, ListChunked, as_struct, concat_list, lit};
    if key_values.is_empty() {
        // PySpark F.create_map() with no args: one empty map {} per row (broadcast literal).
        let key_s = Series::new("key".into(), Vec::<String>::new());
        let value_s = Series::new("value".into(), Vec::<String>::new());
        let fields: [&Series; 2] = [&key_s, &value_s];
        let empty_struct = StructChunked::from_series(
            polars::prelude::PlSmallStr::EMPTY,
            0,
            fields.iter().copied(),
        )
        .map_err(|e| PolarsError::ComputeError(format!("create_map empty struct: {e}").into()))?
        .into_series();
        let list_series = ListChunked::from_iter([Some(empty_struct)])
            .with_name("create_map".into())
            .into_series();
        let expr = lit(list_series).first();
        return Ok(crate::column::Column::from_expr(expr, None));
    }
    let mut struct_exprs: Vec<Expr> = Vec::new();
    for i in (0..key_values.len()).step_by(2) {
        if i + 1 < key_values.len() {
            let k = normalize_map_key_expr(key_values[i].expr().clone()).alias("key");
            let v = key_values[i + 1].expr().clone().alias("value");
            struct_exprs.push(as_struct(vec![k, v]));
        }
    }
    let expr = concat_list(struct_exprs)
        .map_err(|e| PolarsError::ComputeError(format!("create_map concat_list: {e}").into()))?;
    Ok(crate::column::Column::from_expr(expr, None))
}

/// Extract keys from a map column (PySpark map_keys). Map is List(Struct{key, value}).
pub fn map_keys(column: &Column) -> Column {
    column.clone().map_keys()
}

/// Extract values from a map column (PySpark map_values).
pub fn map_values(column: &Column) -> Column {
    column.clone().map_values()
}

/// Return map as list of structs {key, value} (PySpark map_entries).
pub fn map_entries(column: &Column) -> Column {
    column.clone().map_entries()
}

/// Build map from two array columns keys and values (PySpark map_from_arrays). Implemented via UDF.
pub fn map_from_arrays(keys: &Column, values: &Column) -> Column {
    keys.clone().map_from_arrays(values)
}

/// Merge two map columns (PySpark map_concat). Last value wins for duplicate keys.
pub fn map_concat(a: &Column, b: &Column) -> Column {
    a.clone().map_concat(b)
}

/// Array of structs {key, value} to map (PySpark map_from_entries).
pub fn map_from_entries(column: &Column) -> Column {
    column.clone().map_from_entries()
}

/// True if map contains key (PySpark map_contains_key).
pub fn map_contains_key(map_col: &Column, key: &Column) -> Column {
    map_col.clone().map_contains_key(key)
}

/// Get value for key from map, or null (PySpark get).
pub fn get(map_col: &Column, key: &Column) -> Column {
    map_col.clone().get(key)
}

/// Filter map entries by predicate (PySpark map_filter).
pub fn map_filter(map_col: &Column, predicate: Expr) -> Column {
    map_col.clone().map_filter(predicate)
}

/// Merge two maps by key with merge function (PySpark map_zip_with).
pub fn map_zip_with(map1: &Column, map2: &Column, merge: Expr) -> Column {
    map1.clone().map_zip_with(map2, merge)
}

/// Convenience: zip_with with coalesce(left, right) merge.
pub fn zip_with_coalesce(left: &Column, right: &Column) -> Column {
    use polars::prelude::col;
    let left_field = col("").struct_().field_by_name("left");
    let right_field = col("").struct_().field_by_name("right");
    let merge = crate::column::Column::from_expr(
        coalesce(&[
            &crate::column::Column::from_expr(left_field, None),
            &crate::column::Column::from_expr(right_field, None),
        ])
        .expect("zip_with_coalesce passes two columns")
        .into_expr(),
        None,
    );
    left.clone().zip_with(right, merge.into_expr())
}

/// Convenience: map_zip_with with coalesce(value1, value2) merge.
pub fn map_zip_with_coalesce(map1: &Column, map2: &Column) -> Column {
    use polars::prelude::col;
    let v1 = col("").struct_().field_by_name("value1");
    let v2 = col("").struct_().field_by_name("value2");
    let merge = coalesce(&[
        &crate::column::Column::from_expr(v1, None),
        &crate::column::Column::from_expr(v2, None),
    ])
    .expect("map_zip_with_coalesce passes two columns")
    .into_expr();
    map1.clone().map_zip_with(map2, merge)
}

/// Convenience: map_filter with value > threshold predicate.
pub fn map_filter_value_gt(map_col: &Column, threshold: f64) -> Column {
    use polars::prelude::{col, lit};
    let pred = col("").struct_().field_by_name("value").gt(lit(threshold));
    map_col.clone().map_filter(pred)
}

/// Create struct from columns using column names as field names (PySpark struct).
pub fn struct_(columns: &[&Column]) -> Result<Column, PolarsError> {
    use polars::prelude::as_struct;
    if columns.is_empty() {
        return Err(PolarsError::InvalidOperation(
            "struct requires at least one column".into(),
        ));
    }
    let exprs: Vec<Expr> = columns.iter().map(|c| c.expr().clone()).collect();
    Ok(crate::column::Column::from_expr(as_struct(exprs), None))
}

/// Create struct with explicit field names (PySpark named_struct). Pairs of (name, column).
pub fn named_struct(pairs: &[(&str, &Column)]) -> Result<Column, PolarsError> {
    use polars::prelude::as_struct;
    if pairs.is_empty() {
        return Err(PolarsError::InvalidOperation(
            "named_struct requires at least one (name, column) pair".into(),
        ));
    }
    let exprs: Vec<Expr> = pairs
        .iter()
        .map(|(name, col)| col.expr().clone().alias(*name))
        .collect();
    Ok(crate::column::Column::from_expr(as_struct(exprs), None))
}

/// Append element to end of list (PySpark array_append).
pub fn array_append(array: &Column, elem: &Column) -> Column {
    array.clone().array_append(elem)
}

/// Prepend element to start of list (PySpark array_prepend).
pub fn array_prepend(array: &Column, elem: &Column) -> Column {
    array.clone().array_prepend(elem)
}

/// Insert element at 1-based position (PySpark array_insert).
pub fn array_insert(array: &Column, pos: &Column, elem: &Column) -> Column {
    array.clone().array_insert(pos, elem)
}

/// Elements in first array not in second (PySpark array_except).
pub fn array_except(a: &Column, b: &Column) -> Column {
    a.clone().array_except(b)
}

/// Elements in both arrays (PySpark array_intersect).
pub fn array_intersect(a: &Column, b: &Column) -> Column {
    a.clone().array_intersect(b)
}

/// Distinct elements from both arrays (PySpark array_union).
pub fn array_union(a: &Column, b: &Column) -> Column {
    a.clone().array_union(b)
}

/// Zip two arrays element-wise with merge function (PySpark zip_with).
pub fn zip_with(left: &Column, right: &Column, merge: Expr) -> Column {
    left.clone().zip_with(right, merge)
}

/// Extract JSON path from string column (PySpark get_json_object).
pub fn get_json_object(column: &Column, path: &str) -> Column {
    column.clone().get_json_object(path)
}

/// Keys of JSON object (PySpark json_object_keys). Returns list of strings.
pub fn json_object_keys(column: &Column) -> Column {
    column.clone().json_object_keys()
}

/// Extract keys from JSON as struct (PySpark json_tuple). keys: e.g. ["a", "b"].
pub fn json_tuple(column: &Column, keys: &[&str]) -> Column {
    column.clone().json_tuple(keys)
}

/// Parse CSV string to struct (PySpark from_csv). Minimal implementation.
pub fn from_csv(column: &Column) -> Column {
    column.clone().from_csv()
}

/// Format struct as CSV string (PySpark to_csv). Minimal implementation.
pub fn to_csv(column: &Column) -> Column {
    column.clone().to_csv()
}

/// Schema of CSV string (PySpark schema_of_csv). Returns literal schema string; minimal stub.
pub fn schema_of_csv(_column: &Column) -> Column {
    Column::from_expr(
        lit("STRUCT<_c0: STRING, _c1: STRING>".to_string()),
        Some("schema_of_csv".to_string()),
    )
}

/// Schema of JSON string (PySpark schema_of_json). Returns literal schema string; minimal stub.
pub fn schema_of_json(_column: &Column) -> Column {
    Column::from_expr(
        lit("STRUCT<>".to_string()),
        Some("schema_of_json".to_string()),
    )
}

/// Parse string column as JSON into VARIANT (PySpark 4 parse_json). Stored as validated JSON string.
pub fn parse_json(column: &Column) -> Column {
    column.clone().parse_json_variant()
}

/// Parse string column as JSON into struct (PySpark from_json).
pub fn from_json(column: &Column, schema: Option<polars::datatypes::DataType>) -> Column {
    column.clone().from_json(schema)
}

/// Serialize struct column to JSON string (PySpark to_json).
pub fn to_json(column: &Column) -> Column {
    column.clone().to_json()
}

/// Check if column values are in the given list (PySpark isin). Uses Polars is_in.
pub fn isin(column: &Column, other: &Column) -> Column {
    column.clone().isin(other)
}

/// Check if column values are in the given i64 slice (PySpark isin with literal list).
/// #986: Use string series and cast column to string so string column isin(1, 2) works (PySpark parity).
pub fn isin_i64(column: &Column, values: &[i64]) -> Column {
    let str_vals: Vec<String> = values.iter().map(|n| n.to_string()).collect();
    let s: Series = Series::from_iter(str_vals.iter().map(String::as_str));
    Column::from_expr(
        column
            .expr()
            .clone()
            .cast(DataType::String)
            .is_in(lit(s).implode(), false),
        None,
    )
}

/// Check if column values are in the given string slice (PySpark isin with literal list).
/// #1588: Cast column to string so int column isin(["1", "2"]) works (PySpark parity).
pub fn isin_str(column: &Column, values: &[&str]) -> Column {
    let s: Series = Series::from_iter(values.iter().copied());
    Column::from_expr(
        column
            .expr()
            .clone()
            .cast(DataType::String)
            .is_in(lit(s).implode(), false),
        None,
    )
}

/// Percent-decode URL-encoded string (PySpark url_decode).
pub fn url_decode(column: &Column) -> Column {
    column.clone().url_decode()
}

/// Percent-encode string for URL (PySpark url_encode).
pub fn url_encode(column: &Column) -> Column {
    column.clone().url_encode()
}

/// Bitwise left shift (PySpark shiftLeft). col << n.
pub fn shift_left(column: &Column, n: i32) -> Column {
    column.clone().shift_left(n)
}

/// Bitwise signed right shift (PySpark shiftRight). col >> n.
pub fn shift_right(column: &Column, n: i32) -> Column {
    column.clone().shift_right(n)
}

/// Bitwise unsigned right shift (PySpark shiftRightUnsigned). Logical shift for Long.
pub fn shift_right_unsigned(column: &Column, n: i32) -> Column {
    column.clone().shift_right_unsigned(n)
}

/// Session/library version string (PySpark version).
pub fn version() -> Column {
    Column::from_expr(
        lit(concat!("robin-sparkless-", env!("CARGO_PKG_VERSION"))),
        None,
    )
}

/// Null-safe equality: true if both null or both equal (PySpark equal_null). Alias for eq_null_safe.
pub fn equal_null(left: &Column, right: &Column) -> Column {
    left.clone().eq_null_safe(right)
}

/// Length of JSON array at path (PySpark json_array_length).
pub fn json_array_length(column: &Column, path: &str) -> Column {
    column.clone().json_array_length(path)
}

/// Parse URL and extract part: PROTOCOL, HOST, PATH, etc. (PySpark parse_url).
/// When key is Some(k) and part is QUERY/QUERYSTRING, returns the value for that query parameter only.
pub fn parse_url(column: &Column, part: &str, key: Option<&str>) -> Column {
    column.clone().parse_url(part, key)
}

/// Hash of column values (PySpark hash). Uses Murmur3 32-bit for parity with PySpark.
pub fn hash(columns: &[&Column]) -> Column {
    use polars::prelude::*;
    if columns.is_empty() {
        return crate::column::Column::from_expr(lit(0i64), None);
    }
    if columns.len() == 1 {
        return columns[0].clone().hash();
    }
    let exprs: Vec<Expr> = columns.iter().map(|c| c.expr().clone()).collect();
    let struct_expr = polars::prelude::as_struct(exprs);
    let name = columns[0].name().to_string();
    let expr = struct_expr.map(
        |s| crate::column::expect_col(crate::udfs::apply_hash_struct(s)),
        |_schema, field| Ok(Field::new(field.name().clone(), DataType::Int64)),
    );
    crate::column::Column::from_expr(expr, Some(name))
}

/// Stack columns into struct (PySpark stack). Alias for struct_.
pub fn stack(columns: &[&Column]) -> Result<Column, PolarsError> {
    struct_(columns)
}

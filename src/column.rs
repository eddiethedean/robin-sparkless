use polars::prelude::{
    col, lit, DataType, Expr, GetOutput, ListNameSpaceExtension, RankMethod, RankOptions, TimeUnit,
};

/// Convert SQL LIKE pattern (% = any sequence, _ = one char) to regex. Escapes regex specials.
/// When escape_char is Some(esc), esc + any char treats that char as literal (no %/_ expansion).
fn like_pattern_to_regex(pattern: &str, escape_char: Option<char>) -> String {
    let mut out = String::with_capacity(pattern.len() * 2);
    let mut it = pattern.chars();
    while let Some(c) = it.next() {
        if escape_char == Some(c) {
            if let Some(next) = it.next() {
                // Literal: escape for regex
                if "\\.*+?[](){}^$|".contains(next) {
                    out.push('\\');
                }
                out.push(next);
            } else {
                out.push('\\');
                out.push(c);
            }
        } else {
            match c {
                '%' => out.push_str(".*"),
                '_' => out.push('.'),
                '\\' | '.' | '+' | '*' | '?' | '[' | ']' | '(' | ')' | '{' | '}' | '^' | '$'
                | '|' => {
                    out.push('\\');
                    out.push(c);
                }
                _ => out.push(c),
            }
        }
    }
    format!("^{out}$")
}

/// Deferred random column: when added via with_column, we generate a full-length series in one go (PySpark-like).
#[derive(Debug, Clone, Copy)]
pub enum DeferredRandom {
    Rand(Option<u64>),
    Randn(Option<u64>),
}

/// Column - represents a column in a DataFrame, used for building expressions
/// Thin wrapper around Polars `Expr`. May carry a DeferredRandom for rand/randn so with_column can produce one value per row.
#[derive(Debug, Clone)]
pub struct Column {
    name: String,
    expr: Expr, // Polars expression for lazy evaluation
    /// When Some, with_column generates a full-length random series instead of using expr (PySpark-like per-row rand/randn).
    pub(crate) deferred: Option<DeferredRandom>,
}

impl Column {
    /// Create a new Column from a column name
    pub fn new(name: String) -> Self {
        Column {
            name: name.clone(),
            expr: col(&name),
            deferred: None,
        }
    }

    /// Create a Column from a Polars Expr
    pub fn from_expr(expr: Expr, name: Option<String>) -> Self {
        let display_name = name.unwrap_or_else(|| "<expr>".to_string());
        Column {
            name: display_name,
            expr,
            deferred: None,
        }
    }

    /// Create a Column for rand(seed). When used in with_column, generates one value per row (PySpark-like).
    pub fn from_rand(seed: Option<u64>) -> Self {
        let expr = lit(1i64).cum_sum(false).map(
            move |c| crate::udfs::apply_rand_with_seed(c, seed),
            GetOutput::from_type(DataType::Float64),
        );
        Column {
            name: "rand".to_string(),
            expr,
            deferred: Some(DeferredRandom::Rand(seed)),
        }
    }

    /// Create a Column for randn(seed). When used in with_column, generates one value per row (PySpark-like).
    pub fn from_randn(seed: Option<u64>) -> Self {
        let expr = lit(1i64).cum_sum(false).map(
            move |c| crate::udfs::apply_randn_with_seed(c, seed),
            GetOutput::from_type(DataType::Float64),
        );
        Column {
            name: "randn".to_string(),
            expr,
            deferred: Some(DeferredRandom::Randn(seed)),
        }
    }

    /// Get the underlying Polars Expr
    pub fn expr(&self) -> &Expr {
        &self.expr
    }

    /// Convert to Polars Expr (consumes self)
    pub fn into_expr(self) -> Expr {
        self.expr
    }

    /// Get the column name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Alias the column
    pub fn alias(&self, name: &str) -> Column {
        Column {
            name: name.to_string(),
            expr: self.expr.clone().alias(name),
            deferred: self.deferred,
        }
    }

    /// Ascending sort, nulls first (Spark default for ASC). PySpark asc.
    pub fn asc(&self) -> crate::functions::SortOrder {
        crate::functions::asc(self)
    }

    /// Ascending sort, nulls first. PySpark asc_nulls_first.
    pub fn asc_nulls_first(&self) -> crate::functions::SortOrder {
        crate::functions::asc_nulls_first(self)
    }

    /// Ascending sort, nulls last. PySpark asc_nulls_last.
    pub fn asc_nulls_last(&self) -> crate::functions::SortOrder {
        crate::functions::asc_nulls_last(self)
    }

    /// Descending sort, nulls last (Spark default for DESC). PySpark desc.
    pub fn desc(&self) -> crate::functions::SortOrder {
        crate::functions::desc(self)
    }

    /// Descending sort, nulls first. PySpark desc_nulls_first.
    pub fn desc_nulls_first(&self) -> crate::functions::SortOrder {
        crate::functions::desc_nulls_first(self)
    }

    /// Descending sort, nulls last. PySpark desc_nulls_last.
    pub fn desc_nulls_last(&self) -> crate::functions::SortOrder {
        crate::functions::desc_nulls_last(self)
    }

    /// Check if column is null
    pub fn is_null(&self) -> Column {
        Column {
            name: format!("({} IS NULL)", self.name),
            expr: self.expr.clone().is_null(),
            deferred: None,
        }
    }

    /// Check if column is not null
    pub fn is_not_null(&self) -> Column {
        Column {
            name: format!("({} IS NOT NULL)", self.name),
            expr: self.expr.clone().is_not_null(),
            deferred: None,
        }
    }

    /// Alias for is_null. PySpark isnull.
    pub fn isnull(&self) -> Column {
        self.is_null()
    }

    /// Alias for is_not_null. PySpark isnotnull.
    pub fn isnotnull(&self) -> Column {
        self.is_not_null()
    }

    /// Create a null boolean expression
    fn null_boolean_expr() -> Expr {
        use polars::prelude::*;
        // Create an expression that is always a null boolean
        lit(NULL).cast(DataType::Boolean)
    }

    /// SQL LIKE pattern matching (% = any chars, _ = one char). PySpark like.
    /// When escape_char is Some(esc), esc + char treats that char as literal (e.g. \\% = literal %).
    pub fn like(&self, pattern: &str, escape_char: Option<char>) -> Column {
        let regex = like_pattern_to_regex(pattern, escape_char);
        self.regexp_like(&regex)
    }

    /// Case-insensitive LIKE. PySpark ilike.
    /// When escape_char is Some(esc), esc + char treats that char as literal.
    pub fn ilike(&self, pattern: &str, escape_char: Option<char>) -> Column {
        use polars::prelude::*;
        let regex = format!("(?i){}", like_pattern_to_regex(pattern, escape_char));
        Self::from_expr(self.expr().clone().str().contains(lit(regex), false), None)
    }

    /// PySpark-style equality comparison (NULL == NULL returns NULL, not True)
    /// Any comparison involving NULL returns NULL
    ///
    /// Explicitly wraps comparisons with null checks to ensure PySpark semantics.
    /// If either side is NULL, the result is NULL.
    pub fn eq_pyspark(&self, other: &Column) -> Column {
        // Check if either side is NULL
        let left_null = self.expr().clone().is_null();
        let right_null = other.expr().clone().is_null();
        let either_null = left_null.clone().or(right_null.clone());

        // Standard equality comparison
        let eq_result = self.expr().clone().eq(other.expr().clone());

        // Wrap: if either is null, return null boolean, else return comparison result
        let null_boolean = Self::null_boolean_expr();
        let null_aware_expr = crate::functions::when(&Self::from_expr(either_null, None))
            .then(&Self::from_expr(null_boolean, None))
            .otherwise(&Self::from_expr(eq_result, None));

        Self::from_expr(null_aware_expr.into_expr(), None)
    }

    /// PySpark-style inequality comparison (NULL != NULL returns NULL, not False)
    /// Any comparison involving NULL returns NULL
    pub fn ne_pyspark(&self, other: &Column) -> Column {
        // Check if either side is NULL
        let left_null = self.expr().clone().is_null();
        let right_null = other.expr().clone().is_null();
        let either_null = left_null.clone().or(right_null.clone());

        // Standard inequality comparison
        let ne_result = self.expr().clone().neq(other.expr().clone());

        // Wrap: if either is null, return null boolean, else return comparison result
        let null_boolean = Self::null_boolean_expr();
        let null_aware_expr = crate::functions::when(&Self::from_expr(either_null, None))
            .then(&Self::from_expr(null_boolean, None))
            .otherwise(&Self::from_expr(ne_result, None));

        Self::from_expr(null_aware_expr.into_expr(), None)
    }

    /// Null-safe equality (NULL <=> NULL returns True)
    /// PySpark's eqNullSafe() method
    pub fn eq_null_safe(&self, other: &Column) -> Column {
        use crate::functions::{lit_bool, when};

        let left_null = self.expr().clone().is_null();
        let right_null = other.expr().clone().is_null();
        let both_null = left_null.clone().and(right_null.clone());
        let either_null = left_null.clone().or(right_null.clone());

        // Standard equality
        let eq_result = self.expr().clone().eq(other.expr().clone());

        // If both are null, return True
        // If either is null (but not both), return False
        // Otherwise, return standard equality result
        when(&Self::from_expr(both_null, None))
            .then(&lit_bool(true))
            .otherwise(
                &when(&Self::from_expr(either_null, None))
                    .then(&lit_bool(false))
                    .otherwise(&Self::from_expr(eq_result, None)),
            )
    }

    /// PySpark-style greater-than comparison (NULL > value returns NULL)
    /// Any comparison involving NULL returns NULL
    pub fn gt_pyspark(&self, other: &Column) -> Column {
        // Check if either side is NULL
        let left_null = self.expr().clone().is_null();
        let right_null = other.expr().clone().is_null();
        let either_null = left_null.clone().or(right_null.clone());

        // Standard greater-than comparison
        let gt_result = self.expr().clone().gt(other.expr().clone());

        // Wrap: if either is null, return null boolean, else return comparison result
        let null_boolean = Self::null_boolean_expr();
        let null_aware_expr = crate::functions::when(&Self::from_expr(either_null, None))
            .then(&Self::from_expr(null_boolean, None))
            .otherwise(&Self::from_expr(gt_result, None));

        Self::from_expr(null_aware_expr.into_expr(), None)
    }

    /// PySpark-style greater-than-or-equal comparison
    /// Any comparison involving NULL returns NULL
    pub fn ge_pyspark(&self, other: &Column) -> Column {
        // Check if either side is NULL
        let left_null = self.expr().clone().is_null();
        let right_null = other.expr().clone().is_null();
        let either_null = left_null.clone().or(right_null.clone());

        // Standard greater-than-or-equal comparison
        let ge_result = self.expr().clone().gt_eq(other.expr().clone());

        // Wrap: if either is null, return null boolean, else return comparison result
        let null_boolean = Self::null_boolean_expr();
        let null_aware_expr = crate::functions::when(&Self::from_expr(either_null, None))
            .then(&Self::from_expr(null_boolean, None))
            .otherwise(&Self::from_expr(ge_result, None));

        Self::from_expr(null_aware_expr.into_expr(), None)
    }

    /// PySpark-style less-than comparison
    /// Any comparison involving NULL returns NULL
    pub fn lt_pyspark(&self, other: &Column) -> Column {
        // Check if either side is NULL
        let left_null = self.expr().clone().is_null();
        let right_null = other.expr().clone().is_null();
        let either_null = left_null.clone().or(right_null.clone());

        // Standard less-than comparison
        let lt_result = self.expr().clone().lt(other.expr().clone());

        // Wrap: if either is null, return null boolean, else return comparison result
        let null_boolean = Self::null_boolean_expr();
        let null_aware_expr = crate::functions::when(&Self::from_expr(either_null, None))
            .then(&Self::from_expr(null_boolean, None))
            .otherwise(&Self::from_expr(lt_result, None));

        Self::from_expr(null_aware_expr.into_expr(), None)
    }

    /// PySpark-style less-than-or-equal comparison
    /// Any comparison involving NULL returns NULL
    pub fn le_pyspark(&self, other: &Column) -> Column {
        // Check if either side is NULL
        let left_null = self.expr().clone().is_null();
        let right_null = other.expr().clone().is_null();
        let either_null = left_null.clone().or(right_null.clone());

        // Standard less-than-or-equal comparison
        let le_result = self.expr().clone().lt_eq(other.expr().clone());

        // Wrap: if either is null, return null boolean, else return comparison result
        let null_boolean = Self::null_boolean_expr();
        let null_aware_expr = crate::functions::when(&Self::from_expr(either_null, None))
            .then(&Self::from_expr(null_boolean, None))
            .otherwise(&Self::from_expr(le_result, None));

        Self::from_expr(null_aware_expr.into_expr(), None)
    }

    // Standard comparison methods that work with Expr (for literals and columns)
    // These delegate to Polars and may not match PySpark null semantics exactly
    // Use _pyspark variants for explicit PySpark semantics

    /// Greater than comparison
    pub fn gt(&self, other: Expr) -> Column {
        Self::from_expr(self.expr().clone().gt(other), None)
    }

    /// Greater than or equal comparison
    pub fn gt_eq(&self, other: Expr) -> Column {
        Self::from_expr(self.expr().clone().gt_eq(other), None)
    }

    /// Less than comparison
    pub fn lt(&self, other: Expr) -> Column {
        Self::from_expr(self.expr().clone().lt(other), None)
    }

    /// Less than or equal comparison
    pub fn lt_eq(&self, other: Expr) -> Column {
        Self::from_expr(self.expr().clone().lt_eq(other), None)
    }

    /// Equality comparison
    pub fn eq(&self, other: Expr) -> Column {
        Self::from_expr(self.expr().clone().eq(other), None)
    }

    /// Inequality comparison
    pub fn neq(&self, other: Expr) -> Column {
        Self::from_expr(self.expr().clone().neq(other), None)
    }

    // --- String functions ---

    /// Convert string column to uppercase (PySpark upper)
    pub fn upper(&self) -> Column {
        Self::from_expr(self.expr().clone().str().to_uppercase(), None)
    }

    /// Convert string column to lowercase (PySpark lower)
    pub fn lower(&self) -> Column {
        Self::from_expr(self.expr().clone().str().to_lowercase(), None)
    }

    /// Alias for lower. PySpark lcase.
    pub fn lcase(&self) -> Column {
        self.lower()
    }

    /// Alias for upper. PySpark ucase.
    pub fn ucase(&self) -> Column {
        self.upper()
    }

    /// Substring with 1-based start (PySpark substring semantics)
    pub fn substr(&self, start: i64, length: Option<i64>) -> Column {
        use polars::prelude::*;
        let offset = (start - 1).max(0);
        let offset_expr = lit(offset);
        let length_expr = length.map(lit).unwrap_or_else(|| lit(i64::MAX)); // No length = rest of string
        Self::from_expr(
            self.expr().clone().str().slice(offset_expr, length_expr),
            None,
        )
    }

    /// String length in characters (PySpark length)
    pub fn length(&self) -> Column {
        Self::from_expr(self.expr().clone().str().len_chars(), None)
    }

    /// Bit length of string in bytes * 8 (PySpark bit_length).
    pub fn bit_length(&self) -> Column {
        use polars::prelude::*;
        let len_bytes = self.expr().clone().str().len_bytes().cast(DataType::Int32);
        Self::from_expr(len_bytes * lit(8i32), None)
    }

    /// Length of string in bytes (PySpark octet_length).
    pub fn octet_length(&self) -> Column {
        use polars::prelude::*;
        Self::from_expr(
            self.expr().clone().str().len_bytes().cast(DataType::Int32),
            None,
        )
    }

    /// Length of string in characters (PySpark char_length). Alias of length().
    pub fn char_length(&self) -> Column {
        self.length()
    }

    /// Length of string in characters (PySpark character_length). Alias of length().
    pub fn character_length(&self) -> Column {
        self.length()
    }

    /// Encode string to binary (PySpark encode). Charset: UTF-8. Returns hex string.
    pub fn encode(&self, charset: &str) -> Column {
        let charset = charset.to_string();
        let expr = self.expr().clone().map(
            move |s| crate::udfs::apply_encode(s, &charset),
            GetOutput::from_type(DataType::String),
        );
        Self::from_expr(expr, None)
    }

    /// Decode binary (hex string) to string (PySpark decode). Charset: UTF-8.
    pub fn decode(&self, charset: &str) -> Column {
        let charset = charset.to_string();
        let expr = self.expr().clone().map(
            move |s| crate::udfs::apply_decode(s, &charset),
            GetOutput::from_type(DataType::String),
        );
        Self::from_expr(expr, None)
    }

    /// Convert to binary (PySpark to_binary). fmt: 'utf-8', 'hex'. Returns hex string.
    pub fn to_binary(&self, fmt: &str) -> Column {
        let fmt = fmt.to_string();
        let expr = self.expr().clone().map(
            move |s| crate::udfs::apply_to_binary(s, &fmt),
            GetOutput::from_type(DataType::String),
        );
        Self::from_expr(expr, None)
    }

    /// Try convert to binary; null on failure (PySpark try_to_binary).
    pub fn try_to_binary(&self, fmt: &str) -> Column {
        let fmt = fmt.to_string();
        let expr = self.expr().clone().map(
            move |s| crate::udfs::apply_try_to_binary(s, &fmt),
            GetOutput::from_type(DataType::String),
        );
        Self::from_expr(expr, None)
    }

    /// AES encrypt (PySpark aes_encrypt). Key as string; AES-128-GCM. Output hex(nonce||ciphertext).
    pub fn aes_encrypt(&self, key: &str) -> Column {
        let key = key.to_string();
        let expr = self.expr().clone().map(
            move |s| crate::udfs::apply_aes_encrypt(s, &key),
            GetOutput::from_type(DataType::String),
        );
        Self::from_expr(expr, None)
    }

    /// AES decrypt (PySpark aes_decrypt). Input hex(nonce||ciphertext). Null on failure.
    pub fn aes_decrypt(&self, key: &str) -> Column {
        let key = key.to_string();
        let expr = self.expr().clone().map(
            move |s| crate::udfs::apply_aes_decrypt(s, &key),
            GetOutput::from_type(DataType::String),
        );
        Self::from_expr(expr, None)
    }

    /// Try AES decrypt (PySpark try_aes_decrypt). Returns null on failure.
    pub fn try_aes_decrypt(&self, key: &str) -> Column {
        let key = key.to_string();
        let expr = self.expr().clone().map(
            move |s| crate::udfs::apply_try_aes_decrypt(s, &key),
            GetOutput::from_type(DataType::String),
        );
        Self::from_expr(expr, None)
    }

    /// Data type as string (PySpark typeof). Uses dtype from schema.
    pub fn typeof_(&self) -> Column {
        Self::from_expr(
            self.expr().clone().map(
                crate::udfs::apply_typeof,
                GetOutput::from_type(DataType::String),
            ),
            None,
        )
    }

    /// Trim leading and trailing whitespace (PySpark trim)
    pub fn trim(&self) -> Column {
        use polars::prelude::*;
        Self::from_expr(self.expr().clone().str().strip_chars(lit(" \t\n\r")), None)
    }

    /// Trim leading whitespace (PySpark ltrim)
    pub fn ltrim(&self) -> Column {
        use polars::prelude::*;
        Self::from_expr(
            self.expr().clone().str().strip_chars_start(lit(" \t\n\r")),
            None,
        )
    }

    /// Trim trailing whitespace (PySpark rtrim)
    pub fn rtrim(&self) -> Column {
        use polars::prelude::*;
        Self::from_expr(
            self.expr().clone().str().strip_chars_end(lit(" \t\n\r")),
            None,
        )
    }

    /// Trim leading and trailing characters (PySpark btrim). trim_str defaults to whitespace.
    pub fn btrim(&self, trim_str: Option<&str>) -> Column {
        use polars::prelude::*;
        let chars = trim_str.unwrap_or(" \t\n\r");
        Self::from_expr(self.expr().clone().str().strip_chars(lit(chars)), None)
    }

    /// Find substring position 1-based, starting at pos (PySpark locate). 0 if not found.
    pub fn locate(&self, substr: &str, pos: i64) -> Column {
        use polars::prelude::*;
        if substr.is_empty() {
            return Self::from_expr(lit(1i64), None);
        }
        let start = (pos - 1).max(0);
        let slice_expr = self.expr().clone().str().slice(lit(start), lit(i64::MAX));
        let found = slice_expr.str().find_literal(lit(substr.to_string()));
        Self::from_expr(
            (found.cast(DataType::Int64) + lit(start + 1)).fill_null(lit(0i64)),
            None,
        )
    }

    /// Base conversion (PySpark conv). num_str from from_base to to_base.
    pub fn conv(&self, from_base: i32, to_base: i32) -> Column {
        let expr = self.expr().clone().map(
            move |s| crate::udfs::apply_conv(s, from_base, to_base),
            GetOutput::from_type(DataType::String),
        );
        Self::from_expr(expr, None)
    }

    /// Convert to hex string (PySpark hex). Int or string input.
    pub fn hex(&self) -> Column {
        let expr = self.expr().clone().map(
            crate::udfs::apply_hex,
            GetOutput::from_type(DataType::String),
        );
        Self::from_expr(expr, None)
    }

    /// Convert hex string to binary/string (PySpark unhex).
    pub fn unhex(&self) -> Column {
        let expr = self.expr().clone().map(
            crate::udfs::apply_unhex,
            GetOutput::from_type(DataType::String),
        );
        Self::from_expr(expr, None)
    }

    /// Convert integer to binary string (PySpark bin).
    pub fn bin(&self) -> Column {
        let expr = self.expr().clone().map(
            crate::udfs::apply_bin,
            GetOutput::from_type(DataType::String),
        );
        Self::from_expr(expr, None)
    }

    /// Get bit at 0-based position (PySpark getbit).
    pub fn getbit(&self, pos: i64) -> Column {
        let expr = self.expr().clone().map(
            move |s| crate::udfs::apply_getbit(s, pos),
            GetOutput::from_type(DataType::Int64),
        );
        Self::from_expr(expr, None)
    }

    /// Bitwise AND of two integer/boolean columns (PySpark bit_and).
    pub fn bit_and(&self, other: &Column) -> Column {
        let args = [other.expr().clone()];
        let expr = self.expr().clone().cast(DataType::Int64).map_many(
            crate::udfs::apply_bit_and,
            &args,
            GetOutput::from_type(DataType::Int64),
        );
        Self::from_expr(expr, None)
    }

    /// Bitwise OR of two integer/boolean columns (PySpark bit_or).
    pub fn bit_or(&self, other: &Column) -> Column {
        let args = [other.expr().clone()];
        let expr = self.expr().clone().cast(DataType::Int64).map_many(
            crate::udfs::apply_bit_or,
            &args,
            GetOutput::from_type(DataType::Int64),
        );
        Self::from_expr(expr, None)
    }

    /// Bitwise XOR of two integer/boolean columns (PySpark bit_xor).
    pub fn bit_xor(&self, other: &Column) -> Column {
        let args = [other.expr().clone()];
        let expr = self.expr().clone().cast(DataType::Int64).map_many(
            crate::udfs::apply_bit_xor,
            &args,
            GetOutput::from_type(DataType::Int64),
        );
        Self::from_expr(expr, None)
    }

    /// Count of set bits in the integer representation (PySpark bit_count).
    pub fn bit_count(&self) -> Column {
        let expr = self.expr().clone().map(
            crate::udfs::apply_bit_count,
            GetOutput::from_type(DataType::Int64),
        );
        Self::from_expr(expr, None)
    }

    /// Assert that all boolean values are true; errors otherwise (PySpark assert_true).
    /// When err_msg is Some, it is used in the error message when assertion fails.
    pub fn assert_true(&self, err_msg: Option<&str>) -> Column {
        let msg = err_msg.map(String::from);
        let expr = self.expr().clone().map(
            move |c| crate::udfs::apply_assert_true(c, msg.as_deref()),
            GetOutput::same_type(),
        );
        Self::from_expr(expr, None)
    }

    /// Bitwise NOT of an integer/boolean column (PySpark bitwise_not / bitwiseNOT).
    pub fn bitwise_not(&self) -> Column {
        // Use arithmetic identity: !n == -1 - n for two's-complement integers.
        let expr = (lit(-1i64) - self.expr().clone().cast(DataType::Int64)).cast(DataType::Int64);
        Self::from_expr(expr, None)
    }

    /// Parse string to map (PySpark str_to_map). "k1:v1,k2:v2" -> map.
    pub fn str_to_map(&self, pair_delim: &str, key_value_delim: &str) -> Column {
        let pair_delim = pair_delim.to_string();
        let key_value_delim = key_value_delim.to_string();
        let expr = self.expr().clone().map(
            move |s| crate::udfs::apply_str_to_map(s, &pair_delim, &key_value_delim),
            GetOutput::same_type(),
        );
        Self::from_expr(expr, None)
    }

    /// Extract first match of regex pattern (PySpark regexp_extract). Group 0 = full match.
    pub fn regexp_extract(&self, pattern: &str, group_index: usize) -> Column {
        use polars::prelude::*;
        let pat = pattern.to_string();
        Self::from_expr(
            self.expr().clone().str().extract(lit(pat), group_index),
            None,
        )
    }

    /// Replace first match of regex pattern (PySpark regexp_replace). literal=false for regex.
    pub fn regexp_replace(&self, pattern: &str, replacement: &str) -> Column {
        use polars::prelude::*;
        let pat = pattern.to_string();
        let rep = replacement.to_string();
        Self::from_expr(
            self.expr().clone().str().replace(lit(pat), lit(rep), false),
            None,
        )
    }

    /// Leftmost n characters (PySpark left).
    pub fn left(&self, n: i64) -> Column {
        use polars::prelude::*;
        let len = n.max(0) as u32;
        Self::from_expr(
            self.expr().clone().str().slice(lit(0i64), lit(len as i64)),
            None,
        )
    }

    /// Rightmost n characters (PySpark right).
    pub fn right(&self, n: i64) -> Column {
        use polars::prelude::*;
        let n_val = n.max(0);
        let n_expr = lit(n_val);
        let len_chars = self.expr().clone().str().len_chars().cast(DataType::Int64);
        let start = when((len_chars.clone() - n_expr.clone()).lt_eq(lit(0i64)))
            .then(lit(0i64))
            .otherwise(len_chars - n_expr.clone());
        Self::from_expr(self.expr().clone().str().slice(start, n_expr), None)
    }

    /// Replace all occurrences of literal search string with replacement (PySpark replace for literal).
    pub fn replace(&self, search: &str, replacement: &str) -> Column {
        use polars::prelude::*;
        Self::from_expr(
            self.expr().clone().str().replace_all(
                lit(search.to_string()),
                lit(replacement.to_string()),
                true,
            ),
            None,
        )
    }

    /// True if string starts with prefix (PySpark startswith).
    pub fn startswith(&self, prefix: &str) -> Column {
        use polars::prelude::*;
        Self::from_expr(
            self.expr()
                .clone()
                .str()
                .starts_with(lit(prefix.to_string())),
            None,
        )
    }

    /// True if string ends with suffix (PySpark endswith).
    pub fn endswith(&self, suffix: &str) -> Column {
        use polars::prelude::*;
        Self::from_expr(
            self.expr().clone().str().ends_with(lit(suffix.to_string())),
            None,
        )
    }

    /// True if string contains substring (literal, not regex). PySpark contains.
    pub fn contains(&self, substring: &str) -> Column {
        use polars::prelude::*;
        Self::from_expr(
            self.expr()
                .clone()
                .str()
                .contains(lit(substring.to_string()), true),
            None,
        )
    }

    /// Split string by delimiter (PySpark split). Returns list of strings.
    pub fn split(&self, delimiter: &str) -> Column {
        use polars::prelude::*;
        Self::from_expr(
            self.expr().clone().str().split(lit(delimiter.to_string())),
            None,
        )
    }

    /// Title case: first letter of each word uppercase (PySpark initcap).
    /// Approximates with lowercase when Polars to_titlecase is not enabled.
    pub fn initcap(&self) -> Column {
        Self::from_expr(self.expr().clone().str().to_lowercase(), None)
    }

    /// Extract all matches of regex (PySpark regexp_extract_all). Returns list of strings.
    pub fn regexp_extract_all(&self, pattern: &str) -> Column {
        use polars::prelude::*;
        Self::from_expr(
            self.expr()
                .clone()
                .str()
                .extract_all(lit(pattern.to_string())),
            None,
        )
    }

    /// Check if string matches regex (PySpark regexp_like / rlike).
    pub fn regexp_like(&self, pattern: &str) -> Column {
        use polars::prelude::*;
        Self::from_expr(
            self.expr()
                .clone()
                .str()
                .contains(lit(pattern.to_string()), false),
            None,
        )
    }

    /// Count of non-overlapping regex matches (PySpark regexp_count).
    pub fn regexp_count(&self, pattern: &str) -> Column {
        use polars::prelude::*;
        Self::from_expr(
            self.expr()
                .clone()
                .str()
                .count_matches(lit(pattern.to_string()), false)
                .cast(DataType::Int64),
            None,
        )
    }

    /// First substring matching regex (PySpark regexp_substr). Null if no match.
    pub fn regexp_substr(&self, pattern: &str) -> Column {
        self.regexp_extract(pattern, 0)
    }

    /// 1-based position of first regex match (PySpark regexp_instr). group_idx 0 = full match; null if no match.
    pub fn regexp_instr(&self, pattern: &str, group_idx: Option<usize>) -> Column {
        let idx = group_idx.unwrap_or(0);
        let pattern = pattern.to_string();
        let expr = self.expr().clone().map(
            move |s| crate::udfs::apply_regexp_instr(s, pattern.clone(), idx),
            GetOutput::from_type(DataType::Int64),
        );
        Self::from_expr(expr, None)
    }

    /// 1-based index of self in comma-delimited set column (PySpark find_in_set). 0 if not found or self contains comma.
    pub fn find_in_set(&self, set_column: &Column) -> Column {
        let args = [set_column.expr().clone()];
        let expr = self.expr().clone().map_many(
            crate::udfs::apply_find_in_set,
            &args,
            GetOutput::from_type(DataType::Int64),
        );
        Self::from_expr(expr, None)
    }

    /// Repeat string column n times (PySpark repeat). Each element repeated n times.
    pub fn repeat(&self, n: i32) -> Column {
        use polars::prelude::*;
        // repeat_by yields List[str]; join to get a single string per row.
        Self::from_expr(
            self.expr()
                .clone()
                .repeat_by(lit(n as u32))
                .list()
                .join(lit(""), false),
            None,
        )
    }

    /// Reverse string (PySpark reverse).
    pub fn reverse(&self) -> Column {
        Self::from_expr(self.expr().clone().str().reverse(), None)
    }

    /// Find substring position (1-based; 0 if not found). PySpark instr(col, substr).
    pub fn instr(&self, substr: &str) -> Column {
        use polars::prelude::*;
        let found = self
            .expr()
            .clone()
            .str()
            .find_literal(lit(substr.to_string()));
        // Polars find_literal returns 0-based index (null if not found); PySpark is 1-based, 0 when not found.
        Self::from_expr(
            (found.cast(DataType::Int64) + lit(1i64)).fill_null(lit(0i64)),
            None,
        )
    }

    /// Left-pad string to length with pad character (PySpark lpad).
    pub fn lpad(&self, length: i32, pad: &str) -> Column {
        let pad_str = if pad.is_empty() { " " } else { pad };
        let fill = pad_str.chars().next().unwrap_or(' ');
        Self::from_expr(
            self.expr().clone().str().pad_start(length as usize, fill),
            None,
        )
    }

    /// Right-pad string to length with pad character (PySpark rpad).
    pub fn rpad(&self, length: i32, pad: &str) -> Column {
        let pad_str = if pad.is_empty() { " " } else { pad };
        let fill = pad_str.chars().next().unwrap_or(' ');
        Self::from_expr(
            self.expr().clone().str().pad_end(length as usize, fill),
            None,
        )
    }

    /// Character-by-character translation (PySpark translate). Replaces each char in from_str with corresponding in to_str; if to_str is shorter, extra from chars are removed.
    pub fn translate(&self, from_str: &str, to_str: &str) -> Column {
        use polars::prelude::*;
        let mut e = self.expr().clone();
        let from_chars: Vec<char> = from_str.chars().collect();
        let to_chars: Vec<char> = to_str.chars().collect();
        for (i, fc) in from_chars.iter().enumerate() {
            let f = fc.to_string();
            let t = to_chars
                .get(i)
                .map(|c| c.to_string())
                .unwrap_or_else(String::new); // PySpark: no replacement = drop char
            e = e.str().replace_all(lit(f), lit(t), true);
        }
        Self::from_expr(e, None)
    }

    /// Mask string: replace uppercase with upper_char, lowercase with lower_char, digits with digit_char (PySpark mask).
    /// Defaults: upper 'X', lower 'x', digit 'n'; other chars unchanged.
    pub fn mask(
        &self,
        upper_char: Option<char>,
        lower_char: Option<char>,
        digit_char: Option<char>,
        other_char: Option<char>,
    ) -> Column {
        use polars::prelude::*;
        let upper = upper_char.unwrap_or('X').to_string();
        let lower = lower_char.unwrap_or('x').to_string();
        let digit = digit_char.unwrap_or('n').to_string();
        let other = other_char.map(|c| c.to_string());
        let mut e = self
            .expr()
            .clone()
            .str()
            .replace_all(lit("[A-Z]".to_string()), lit(upper), false)
            .str()
            .replace_all(lit("[a-z]".to_string()), lit(lower), false)
            .str()
            .replace_all(lit(r"\d".to_string()), lit(digit), false);
        if let Some(o) = other {
            e = e
                .str()
                .replace_all(lit("[^A-Za-z0-9]".to_string()), lit(o), false);
        }
        Self::from_expr(e, None)
    }

    /// Split by delimiter and return 1-based part (PySpark split_part).
    /// part_num > 0: from left; part_num < 0: from right; part_num = 0: null; out-of-range: empty string.
    pub fn split_part(&self, delimiter: &str, part_num: i64) -> Column {
        use polars::prelude::*;
        if part_num == 0 {
            return Self::from_expr(Expr::Literal(LiteralValue::Null), None);
        }
        let delim = delimiter.to_string();
        let split_expr = self.expr().clone().str().split(lit(delim));
        // Polars list.get: 0-based; -1 = last. part_num 1 -> index 0, part_num -1 -> index -1.
        let index = if part_num > 0 {
            lit(part_num - 1)
        } else {
            lit(part_num) // -1, -2, etc. work for list.get
        };
        let get_expr = split_expr.list().get(index, true).fill_null(lit(""));
        // Preserve null when source string was null
        let expr = when(self.expr().clone().is_null())
            .then(Expr::Literal(LiteralValue::Null))
            .otherwise(get_expr);
        Self::from_expr(expr, None)
    }

    /// Substring before/after nth delimiter (PySpark substring_index). count > 0: before nth from left; count < 0: after nth from right.
    pub fn substring_index(&self, delimiter: &str, count: i64) -> Column {
        use polars::prelude::*;
        let delim = delimiter.to_string();
        let split_expr = self.expr().clone().str().split(lit(delim.clone()));
        let n = count.unsigned_abs() as i64;
        let expr = if count > 0 {
            split_expr
                .clone()
                .list()
                .slice(lit(0i64), lit(n))
                .list()
                .join(lit(delim), false)
        } else {
            let len = split_expr.clone().list().len();
            let start = when(len.clone().gt(lit(n)))
                .then(len.clone() - lit(n))
                .otherwise(lit(0i64));
            let slice_len = when(len.clone().gt(lit(n))).then(lit(n)).otherwise(len);
            split_expr
                .list()
                .slice(start, slice_len)
                .list()
                .join(lit(delim), false)
        };
        Self::from_expr(expr, None)
    }

    /// Soundex code (PySpark soundex). Implemented via map UDF (strsim/soundex crates).
    pub fn soundex(&self) -> Column {
        let expr = self
            .expr()
            .clone()
            .map(crate::udfs::apply_soundex, GetOutput::same_type());
        Self::from_expr(expr, None)
    }

    /// Levenshtein distance to another string (PySpark levenshtein). Implemented via map_many UDF (strsim).
    pub fn levenshtein(&self, other: &Column) -> Column {
        let args = [other.expr().clone()];
        let expr = self.expr().clone().map_many(
            crate::udfs::apply_levenshtein,
            &args,
            GetOutput::from_type(DataType::Int64),
        );
        Self::from_expr(expr, None)
    }

    /// CRC32 checksum of string bytes (PySpark crc32). Implemented via map UDF (crc32fast).
    pub fn crc32(&self) -> Column {
        let expr = self.expr().clone().map(
            crate::udfs::apply_crc32,
            GetOutput::from_type(DataType::Int64),
        );
        Self::from_expr(expr, None)
    }

    /// XXH64 hash of string (PySpark xxhash64). Implemented via map UDF (twox-hash).
    pub fn xxhash64(&self) -> Column {
        let expr = self.expr().clone().map(
            crate::udfs::apply_xxhash64,
            GetOutput::from_type(DataType::Int64),
        );
        Self::from_expr(expr, None)
    }

    /// ASCII value of first character (PySpark ascii). Returns Int32.
    pub fn ascii(&self) -> Column {
        let expr = self.expr().clone().map(
            crate::udfs::apply_ascii,
            GetOutput::from_type(DataType::Int32),
        );
        Self::from_expr(expr, None)
    }

    /// Format numeric as string with fixed decimal places (PySpark format_number).
    pub fn format_number(&self, decimals: u32) -> Column {
        let expr = self.expr().clone().map(
            move |s| crate::udfs::apply_format_number(s, decimals),
            GetOutput::from_type(DataType::String),
        );
        Self::from_expr(expr, None)
    }

    /// Int to single-character string (PySpark char / chr). Valid codepoint only.
    pub fn char(&self) -> Column {
        let expr = self.expr().clone().map(
            crate::udfs::apply_char,
            GetOutput::from_type(DataType::String),
        );
        Self::from_expr(expr, None)
    }

    /// Alias for char (PySpark chr).
    pub fn chr(&self) -> Column {
        self.char()
    }

    /// Base64 encode string bytes (PySpark base64).
    pub fn base64(&self) -> Column {
        let expr = self
            .expr()
            .clone()
            .map(crate::udfs::apply_base64, GetOutput::same_type());
        Self::from_expr(expr, None)
    }

    /// Base64 decode to string (PySpark unbase64). Invalid decode → null.
    pub fn unbase64(&self) -> Column {
        let expr = self
            .expr()
            .clone()
            .map(crate::udfs::apply_unbase64, GetOutput::same_type());
        Self::from_expr(expr, None)
    }

    /// SHA1 hash of string bytes, return hex string (PySpark sha1).
    pub fn sha1(&self) -> Column {
        let expr = self
            .expr()
            .clone()
            .map(crate::udfs::apply_sha1, GetOutput::same_type());
        Self::from_expr(expr, None)
    }

    /// SHA2 hash; bit_length 256, 384, or 512 (PySpark sha2). Default 256.
    pub fn sha2(&self, bit_length: i32) -> Column {
        let expr = self.expr().clone().map(
            move |s| crate::udfs::apply_sha2(s, bit_length),
            GetOutput::same_type(),
        );
        Self::from_expr(expr, None)
    }

    /// MD5 hash of string bytes, return hex string (PySpark md5).
    pub fn md5(&self) -> Column {
        let expr = self
            .expr()
            .clone()
            .map(crate::udfs::apply_md5, GetOutput::same_type());
        Self::from_expr(expr, None)
    }

    /// Replace substring at 1-based position (PySpark overlay). replace is literal string.
    pub fn overlay(&self, replace: &str, pos: i64, length: i64) -> Column {
        use polars::prelude::*;
        let pos = pos.max(1);
        let replace_len = length.max(0);
        let start_left = 0i64;
        let len_left = (pos - 1).max(0);
        let start_right = (pos - 1 + replace_len).max(0);
        let len_right = 1_000_000i64; // "rest of string"
        let left = self
            .expr()
            .clone()
            .str()
            .slice(lit(start_left), lit(len_left));
        let mid = lit(replace.to_string());
        let right = self
            .expr()
            .clone()
            .str()
            .slice(lit(start_right), lit(len_right));
        let exprs = [left, mid, right];
        let concat_expr = polars::prelude::concat_str(&exprs, "", false);
        Self::from_expr(concat_expr, None)
    }

    // --- Math functions ---

    /// Absolute value (PySpark abs)
    pub fn abs(&self) -> Column {
        Self::from_expr(self.expr().clone().abs(), None)
    }

    /// Ceiling (PySpark ceil)
    pub fn ceil(&self) -> Column {
        Self::from_expr(self.expr().clone().ceil(), None)
    }

    /// Alias for ceil. PySpark ceiling.
    pub fn ceiling(&self) -> Column {
        self.ceil()
    }

    /// Floor (PySpark floor)
    pub fn floor(&self) -> Column {
        Self::from_expr(self.expr().clone().floor(), None)
    }

    /// Round to given decimal places (PySpark round)
    pub fn round(&self, decimals: u32) -> Column {
        Self::from_expr(self.expr().clone().round(decimals), None)
    }

    /// Banker's rounding - round half to even (PySpark bround).
    pub fn bround(&self, scale: i32) -> Column {
        let expr = self.expr().clone().map(
            move |s| crate::udfs::apply_bround(s, scale),
            GetOutput::from_type(DataType::Float64),
        );
        Self::from_expr(expr, None)
    }

    /// Unary minus (PySpark negate, negative).
    pub fn negate(&self) -> Column {
        use polars::prelude::*;
        Self::from_expr(self.expr().clone() * lit(-1), None)
    }

    /// Square root (PySpark sqrt)
    pub fn sqrt(&self) -> Column {
        Self::from_expr(self.expr().clone().sqrt(), None)
    }

    /// Power (PySpark pow). Exponent can be literal or expression.
    pub fn pow(&self, exp: i64) -> Column {
        use polars::prelude::*;
        Self::from_expr(self.expr().clone().pow(lit(exp)), None)
    }

    /// Alias for pow. PySpark power.
    pub fn power(&self, exp: i64) -> Column {
        self.pow(exp)
    }

    /// Exponential (PySpark exp)
    pub fn exp(&self) -> Column {
        Self::from_expr(self.expr().clone().exp(), None)
    }

    /// Natural logarithm (PySpark log)
    pub fn log(&self) -> Column {
        Self::from_expr(self.expr().clone().log(std::f64::consts::E), None)
    }

    /// Alias for log. PySpark ln.
    pub fn ln(&self) -> Column {
        self.log()
    }

    /// Sine (radians). PySpark sin.
    pub fn sin(&self) -> Column {
        let expr = self.expr().clone().map(
            crate::udfs::apply_sin,
            GetOutput::from_type(DataType::Float64),
        );
        Self::from_expr(expr, None)
    }

    /// Cosine (radians). PySpark cos.
    pub fn cos(&self) -> Column {
        let expr = self.expr().clone().map(
            crate::udfs::apply_cos,
            GetOutput::from_type(DataType::Float64),
        );
        Self::from_expr(expr, None)
    }

    /// Tangent (radians). PySpark tan.
    pub fn tan(&self) -> Column {
        let expr = self.expr().clone().map(
            crate::udfs::apply_tan,
            GetOutput::from_type(DataType::Float64),
        );
        Self::from_expr(expr, None)
    }

    /// Cotangent: 1/tan (PySpark cot).
    pub fn cot(&self) -> Column {
        let expr = self.expr().clone().map(
            crate::udfs::apply_cot,
            GetOutput::from_type(DataType::Float64),
        );
        Self::from_expr(expr, None)
    }

    /// Cosecant: 1/sin (PySpark csc).
    pub fn csc(&self) -> Column {
        let expr = self.expr().clone().map(
            crate::udfs::apply_csc,
            GetOutput::from_type(DataType::Float64),
        );
        Self::from_expr(expr, None)
    }

    /// Secant: 1/cos (PySpark sec).
    pub fn sec(&self) -> Column {
        let expr = self.expr().clone().map(
            crate::udfs::apply_sec,
            GetOutput::from_type(DataType::Float64),
        );
        Self::from_expr(expr, None)
    }

    /// Arc sine. PySpark asin.
    pub fn asin(&self) -> Column {
        let expr = self.expr().clone().map(
            crate::udfs::apply_asin,
            GetOutput::from_type(DataType::Float64),
        );
        Self::from_expr(expr, None)
    }

    /// Arc cosine. PySpark acos.
    pub fn acos(&self) -> Column {
        let expr = self.expr().clone().map(
            crate::udfs::apply_acos,
            GetOutput::from_type(DataType::Float64),
        );
        Self::from_expr(expr, None)
    }

    /// Arc tangent. PySpark atan.
    pub fn atan(&self) -> Column {
        let expr = self.expr().clone().map(
            crate::udfs::apply_atan,
            GetOutput::from_type(DataType::Float64),
        );
        Self::from_expr(expr, None)
    }

    /// Two-argument arc tangent (y, x) -> angle in radians. PySpark atan2.
    pub fn atan2(&self, x: &Column) -> Column {
        let args = [x.expr().clone()];
        let expr = self.expr().clone().map_many(
            crate::udfs::apply_atan2,
            &args,
            GetOutput::from_type(DataType::Float64),
        );
        Self::from_expr(expr, None)
    }

    /// Convert radians to degrees. PySpark degrees.
    pub fn degrees(&self) -> Column {
        let expr = self.expr().clone().map(
            crate::udfs::apply_degrees,
            GetOutput::from_type(DataType::Float64),
        );
        Self::from_expr(expr, None)
    }

    /// Alias for degrees. PySpark toDegrees.
    pub fn to_degrees(&self) -> Column {
        self.degrees()
    }

    /// Convert degrees to radians. PySpark radians.
    pub fn radians(&self) -> Column {
        let expr = self.expr().clone().map(
            crate::udfs::apply_radians,
            GetOutput::from_type(DataType::Float64),
        );
        Self::from_expr(expr, None)
    }

    /// Alias for radians. PySpark toRadians.
    pub fn to_radians(&self) -> Column {
        self.radians()
    }

    /// Sign of the number (-1, 0, or 1). PySpark signum.
    pub fn signum(&self) -> Column {
        let expr = self.expr().clone().map(
            crate::udfs::apply_signum,
            GetOutput::from_type(DataType::Float64),
        );
        Self::from_expr(expr, None)
    }

    /// Hyperbolic cosine. PySpark cosh.
    pub fn cosh(&self) -> Column {
        let expr = self.expr().clone().map(
            crate::udfs::apply_cosh,
            GetOutput::from_type(DataType::Float64),
        );
        Self::from_expr(expr, None)
    }
    /// Hyperbolic sine. PySpark sinh.
    pub fn sinh(&self) -> Column {
        let expr = self.expr().clone().map(
            crate::udfs::apply_sinh,
            GetOutput::from_type(DataType::Float64),
        );
        Self::from_expr(expr, None)
    }
    /// Hyperbolic tangent. PySpark tanh.
    pub fn tanh(&self) -> Column {
        let expr = self.expr().clone().map(
            crate::udfs::apply_tanh,
            GetOutput::from_type(DataType::Float64),
        );
        Self::from_expr(expr, None)
    }
    /// Inverse hyperbolic cosine. PySpark acosh.
    pub fn acosh(&self) -> Column {
        let expr = self.expr().clone().map(
            crate::udfs::apply_acosh,
            GetOutput::from_type(DataType::Float64),
        );
        Self::from_expr(expr, None)
    }
    /// Inverse hyperbolic sine. PySpark asinh.
    pub fn asinh(&self) -> Column {
        let expr = self.expr().clone().map(
            crate::udfs::apply_asinh,
            GetOutput::from_type(DataType::Float64),
        );
        Self::from_expr(expr, None)
    }
    /// Inverse hyperbolic tangent. PySpark atanh.
    pub fn atanh(&self) -> Column {
        let expr = self.expr().clone().map(
            crate::udfs::apply_atanh,
            GetOutput::from_type(DataType::Float64),
        );
        Self::from_expr(expr, None)
    }
    /// Cube root. PySpark cbrt.
    pub fn cbrt(&self) -> Column {
        let expr = self.expr().clone().map(
            crate::udfs::apply_cbrt,
            GetOutput::from_type(DataType::Float64),
        );
        Self::from_expr(expr, None)
    }
    /// exp(x) - 1. PySpark expm1.
    pub fn expm1(&self) -> Column {
        let expr = self.expr().clone().map(
            crate::udfs::apply_expm1,
            GetOutput::from_type(DataType::Float64),
        );
        Self::from_expr(expr, None)
    }
    /// log(1 + x). PySpark log1p.
    pub fn log1p(&self) -> Column {
        let expr = self.expr().clone().map(
            crate::udfs::apply_log1p,
            GetOutput::from_type(DataType::Float64),
        );
        Self::from_expr(expr, None)
    }
    /// Base-10 logarithm. PySpark log10.
    pub fn log10(&self) -> Column {
        let expr = self.expr().clone().map(
            crate::udfs::apply_log10,
            GetOutput::from_type(DataType::Float64),
        );
        Self::from_expr(expr, None)
    }
    /// Base-2 logarithm. PySpark log2.
    pub fn log2(&self) -> Column {
        let expr = self.expr().clone().map(
            crate::udfs::apply_log2,
            GetOutput::from_type(DataType::Float64),
        );
        Self::from_expr(expr, None)
    }
    /// Round to nearest integer. PySpark rint.
    pub fn rint(&self) -> Column {
        let expr = self.expr().clone().map(
            crate::udfs::apply_rint,
            GetOutput::from_type(DataType::Float64),
        );
        Self::from_expr(expr, None)
    }

    /// sqrt(x^2 + y^2). PySpark hypot.
    pub fn hypot(&self, other: &Column) -> Column {
        let xx = self.expr().clone() * self.expr().clone();
        let yy = other.expr().clone() * other.expr().clone();
        Self::from_expr((xx + yy).sqrt(), None)
    }

    /// Cast to the given type (PySpark cast). Fails on invalid conversion.
    pub fn cast_to(&self, type_name: &str) -> Result<Column, String> {
        crate::functions::cast(self, type_name)
    }

    /// Cast to the given type, null on invalid conversion (PySpark try_cast).
    pub fn try_cast_to(&self, type_name: &str) -> Result<Column, String> {
        crate::functions::try_cast(self, type_name)
    }

    /// True where the float value is NaN (PySpark isnan).
    pub fn is_nan(&self) -> Column {
        Self::from_expr(self.expr().clone().is_nan(), None)
    }

    // --- Datetime functions ---

    /// Extract year from datetime column (PySpark year)
    pub fn year(&self) -> Column {
        Self::from_expr(self.expr().clone().dt().year(), None)
    }

    /// Extract month from datetime column (PySpark month)
    pub fn month(&self) -> Column {
        Self::from_expr(self.expr().clone().dt().month(), None)
    }

    /// Extract day of month from datetime column (PySpark day)
    pub fn day(&self) -> Column {
        Self::from_expr(self.expr().clone().dt().day(), None)
    }

    /// Alias for day. PySpark dayofmonth.
    pub fn dayofmonth(&self) -> Column {
        self.day()
    }

    /// Extract quarter (1-4) from date/datetime column (PySpark quarter).
    pub fn quarter(&self) -> Column {
        Self::from_expr(self.expr().clone().dt().quarter(), None)
    }

    /// Extract ISO week of year (1-53) (PySpark weekofyear / week).
    pub fn weekofyear(&self) -> Column {
        Self::from_expr(self.expr().clone().dt().week(), None)
    }

    /// Alias for weekofyear (PySpark week).
    pub fn week(&self) -> Column {
        self.weekofyear()
    }

    /// Day of week: 1 = Sunday, 2 = Monday, ..., 7 = Saturday (PySpark dayofweek).
    /// Polars weekday is Mon=1..Sun=7; we convert to Sun=1..Sat=7.
    pub fn dayofweek(&self) -> Column {
        let w = self.expr().clone().dt().weekday();
        let dayofweek = (w % lit(7i32)) + lit(1i32); // 7->1 (Sun), 1->2 (Mon), ..., 6->7 (Sat)
        Self::from_expr(dayofweek, None)
    }

    /// Day of year (1-366) (PySpark dayofyear).
    pub fn dayofyear(&self) -> Column {
        Self::from_expr(
            self.expr().clone().dt().ordinal_day().cast(DataType::Int32),
            None,
        )
    }

    /// Cast to date (PySpark to_date). Drops time component from datetime/timestamp.
    pub fn to_date(&self) -> Column {
        use polars::prelude::DataType;
        Self::from_expr(self.expr().clone().cast(DataType::Date), None)
    }

    /// Format date/datetime as string (PySpark date_format). Uses chrono strftime format.
    pub fn date_format(&self, format: &str) -> Column {
        Self::from_expr(self.expr().clone().dt().strftime(format), None)
    }

    /// Extract hour from datetime column (PySpark hour).
    pub fn hour(&self) -> Column {
        Self::from_expr(self.expr().clone().dt().hour(), None)
    }

    /// Extract minute from datetime column (PySpark minute).
    pub fn minute(&self) -> Column {
        Self::from_expr(self.expr().clone().dt().minute(), None)
    }

    /// Extract second from datetime column (PySpark second).
    pub fn second(&self) -> Column {
        Self::from_expr(self.expr().clone().dt().second(), None)
    }

    /// Extract field from date/datetime (PySpark extract). field: "year","month","day","hour","minute","second","quarter","week","dayofweek","dayofyear".
    pub fn extract(&self, field: &str) -> Column {
        use polars::prelude::*;
        let e = self.expr().clone();
        let expr = match field.trim().to_lowercase().as_str() {
            "year" => e.dt().year(),
            "month" => e.dt().month(),
            "day" => e.dt().day(),
            "hour" => e.dt().hour(),
            "minute" => e.dt().minute(),
            "second" => e.dt().second(),
            "quarter" => e.dt().quarter(),
            "week" | "weekofyear" => e.dt().week(),
            "dayofweek" | "dow" => {
                let w = e.dt().weekday();
                (w % lit(7i32)) + lit(1i32)
            }
            "dayofyear" | "doy" => e.dt().ordinal_day().cast(DataType::Int32),
            _ => e.dt().year(), // fallback
        };
        Self::from_expr(expr, None)
    }

    /// Timestamp to microseconds since epoch (PySpark unix_micros).
    pub fn unix_micros(&self) -> Column {
        use polars::prelude::*;
        Self::from_expr(self.expr().clone().cast(DataType::Int64), None)
    }

    /// Timestamp to milliseconds since epoch (PySpark unix_millis).
    pub fn unix_millis(&self) -> Column {
        use polars::prelude::*;
        let micros = self.expr().clone().cast(DataType::Int64);
        Self::from_expr(micros / lit(1000i64), None)
    }

    /// Timestamp to seconds since epoch (PySpark unix_seconds).
    pub fn unix_seconds(&self) -> Column {
        use polars::prelude::*;
        let micros = self.expr().clone().cast(DataType::Int64);
        Self::from_expr(micros / lit(1_000_000i64), None)
    }

    /// Weekday name "Mon","Tue",... (PySpark dayname).
    pub fn dayname(&self) -> Column {
        let expr = self.expr().clone().map(
            crate::udfs::apply_dayname,
            GetOutput::from_type(DataType::String),
        );
        Self::from_expr(expr, None)
    }

    /// Weekday 0=Mon, 6=Sun (PySpark weekday).
    pub fn weekday(&self) -> Column {
        let expr = self.expr().clone().map(
            crate::udfs::apply_weekday,
            GetOutput::from_type(DataType::Int32),
        );
        Self::from_expr(expr, None)
    }

    /// Add n days to date/datetime column (PySpark date_add).
    pub fn date_add(&self, n: i32) -> Column {
        use polars::prelude::*;
        let date_expr = self.expr().clone().cast(DataType::Date);
        let dur = duration(DurationArgs::new().with_days(lit(n as i64)));
        Self::from_expr(date_expr + dur, None)
    }

    /// Subtract n days from date/datetime column (PySpark date_sub).
    pub fn date_sub(&self, n: i32) -> Column {
        use polars::prelude::*;
        let date_expr = self.expr().clone().cast(DataType::Date);
        let dur = duration(DurationArgs::new().with_days(lit(n as i64)));
        Self::from_expr(date_expr - dur, None)
    }

    /// Number of days between two date/datetime columns (PySpark datediff). (end - start).
    pub fn datediff(&self, other: &Column) -> Column {
        use polars::prelude::*;
        let start = self.expr().clone().cast(DataType::Date);
        let end = other.expr().clone().cast(DataType::Date);
        Self::from_expr((end - start).dt().total_days(), None)
    }

    /// Last day of the month for date/datetime column (PySpark last_day).
    pub fn last_day(&self) -> Column {
        Self::from_expr(self.expr().clone().dt().month_end(), None)
    }

    /// Add amount of unit to timestamp (PySpark timestampadd). unit: DAY, HOUR, MINUTE, SECOND, etc.
    pub fn timestampadd(&self, unit: &str, amount: &Column) -> Column {
        use polars::prelude::*;
        let ts = self.expr().clone();
        let amt = amount.expr().clone().cast(DataType::Int64);
        let dur = match unit.trim().to_uppercase().as_str() {
            "DAY" | "DAYS" => duration(DurationArgs::new().with_days(amt)),
            "HOUR" | "HOURS" => duration(DurationArgs::new().with_hours(amt)),
            "MINUTE" | "MINUTES" => duration(DurationArgs::new().with_minutes(amt)),
            "SECOND" | "SECONDS" => duration(DurationArgs::new().with_seconds(amt)),
            "WEEK" | "WEEKS" => duration(DurationArgs::new().with_weeks(amt)),
            _ => duration(DurationArgs::new().with_days(amt)),
        };
        Self::from_expr(ts + dur, None)
    }

    /// Difference between timestamps in given unit (PySpark timestampdiff). unit: DAY, HOUR, MINUTE, SECOND.
    pub fn timestampdiff(&self, unit: &str, other: &Column) -> Column {
        let start = self.expr().clone();
        let end = other.expr().clone();
        let diff = end - start;
        let expr = match unit.trim().to_uppercase().as_str() {
            "HOUR" | "HOURS" => diff.dt().total_hours(),
            "MINUTE" | "MINUTES" => diff.dt().total_minutes(),
            "SECOND" | "SECONDS" => diff.dt().total_seconds(),
            "DAY" | "DAYS" => diff.dt().total_days(),
            _ => diff.dt().total_days(),
        };
        Self::from_expr(expr, None)
    }

    /// Interpret timestamp as UTC, convert to target timezone (PySpark from_utc_timestamp).
    pub fn from_utc_timestamp(&self, tz: &str) -> Column {
        let tz = tz.to_string();
        let expr = self.expr().clone().map(
            move |s| crate::udfs::apply_from_utc_timestamp(s, &tz),
            GetOutput::same_type(),
        );
        Self::from_expr(expr, None)
    }

    /// Interpret timestamp as in tz, convert to UTC (PySpark to_utc_timestamp).
    pub fn to_utc_timestamp(&self, tz: &str) -> Column {
        let tz = tz.to_string();
        let expr = self.expr().clone().map(
            move |s| crate::udfs::apply_to_utc_timestamp(s, &tz),
            GetOutput::same_type(),
        );
        Self::from_expr(expr, None)
    }

    /// Truncate date/datetime to unit (e.g. "mo", "wk", "day"). PySpark trunc.
    pub fn trunc(&self, format: &str) -> Column {
        use polars::prelude::*;
        Self::from_expr(
            self.expr().clone().dt().truncate(lit(format.to_string())),
            None,
        )
    }

    /// Add n months to date/datetime column (PySpark add_months). Month-aware.
    pub fn add_months(&self, n: i32) -> Column {
        let expr = self.expr().clone().map(
            move |col| crate::udfs::apply_add_months(col, n),
            GetOutput::from_type(DataType::Date),
        );
        Self::from_expr(expr, None)
    }

    /// Number of months between end and start dates, as fractional (PySpark months_between).
    /// When round_off is true, rounds to 8 decimal places (PySpark default).
    pub fn months_between(&self, start: &Column, round_off: bool) -> Column {
        let args = [start.expr().clone()];
        let expr = self.expr().clone().map_many(
            move |cols| crate::udfs::apply_months_between(cols, round_off),
            &args,
            GetOutput::from_type(DataType::Float64),
        );
        Self::from_expr(expr, None)
    }

    /// Next date that is the given day of week (e.g. "Mon", "Tue") (PySpark next_day).
    pub fn next_day(&self, day_of_week: &str) -> Column {
        let day = day_of_week.to_string();
        let expr = self.expr().clone().map(
            move |col| crate::udfs::apply_next_day(col, &day),
            GetOutput::from_type(DataType::Date),
        );
        Self::from_expr(expr, None)
    }

    /// Parse string timestamp to seconds since epoch (PySpark unix_timestamp).
    pub fn unix_timestamp(&self, format: Option<&str>) -> Column {
        let fmt = format.map(String::from);
        let expr = self.expr().clone().map(
            move |col| crate::udfs::apply_unix_timestamp(col, fmt.as_deref()),
            GetOutput::from_type(DataType::Int64),
        );
        Self::from_expr(expr, None)
    }

    /// Convert seconds since epoch to formatted string (PySpark from_unixtime).
    pub fn from_unixtime(&self, format: Option<&str>) -> Column {
        let fmt = format.map(String::from);
        let expr = self.expr().clone().map(
            move |col| crate::udfs::apply_from_unixtime(col, fmt.as_deref()),
            GetOutput::from_type(DataType::String),
        );
        Self::from_expr(expr, None)
    }

    /// Convert seconds since epoch to timestamp (PySpark timestamp_seconds).
    pub fn timestamp_seconds(&self) -> Column {
        let expr = (self.expr().clone().cast(DataType::Int64) * lit(1_000_000i64))
            .cast(DataType::Datetime(TimeUnit::Microseconds, None));
        Self::from_expr(expr, None)
    }

    /// Convert milliseconds since epoch to timestamp (PySpark timestamp_millis).
    pub fn timestamp_millis(&self) -> Column {
        let expr = (self.expr().clone().cast(DataType::Int64) * lit(1000i64))
            .cast(DataType::Datetime(TimeUnit::Microseconds, None));
        Self::from_expr(expr, None)
    }

    /// Convert microseconds since epoch to timestamp (PySpark timestamp_micros).
    pub fn timestamp_micros(&self) -> Column {
        let expr = self
            .expr()
            .clone()
            .cast(DataType::Int64)
            .cast(DataType::Datetime(TimeUnit::Microseconds, None));
        Self::from_expr(expr, None)
    }

    /// Date to days since 1970-01-01 (PySpark unix_date).
    pub fn unix_date(&self) -> Column {
        let expr = self.expr().clone().map(
            crate::udfs::apply_unix_date,
            GetOutput::from_type(DataType::Int32),
        );
        Self::from_expr(expr, None)
    }

    /// Days since epoch to date (PySpark date_from_unix_date).
    pub fn date_from_unix_date(&self) -> Column {
        let expr = self.expr().clone().map(
            crate::udfs::apply_date_from_unix_date,
            GetOutput::from_type(DataType::Date),
        );
        Self::from_expr(expr, None)
    }

    /// Positive modulus (PySpark pmod). Column method: pmod(self, other).
    pub fn pmod(&self, divisor: &Column) -> Column {
        let args = [divisor.expr().clone()];
        let expr = self.expr().clone().map_many(
            crate::udfs::apply_pmod,
            &args,
            GetOutput::from_type(DataType::Float64),
        );
        Self::from_expr(expr, None)
    }

    /// Factorial n! for n in 0..=20 (PySpark factorial).
    pub fn factorial(&self) -> Column {
        let expr = self.expr().clone().map(
            crate::udfs::apply_factorial,
            GetOutput::from_type(DataType::Int64),
        );
        Self::from_expr(expr, None)
    }

    // --- Window functions ---

    /// Apply window partitioning. Returns a new Column with `.over(partition_by)`.
    /// Use after rank(), dense_rank(), row_number(), lag(), lead().
    pub fn over(&self, partition_by: &[&str]) -> Column {
        let partition_exprs: Vec<Expr> = partition_by.iter().map(|s| col(*s)).collect();
        Self::from_expr(self.expr().clone().over(partition_exprs), None)
    }

    /// Rank (with ties, gaps). Use with `.over(partition_by)`.
    pub fn rank(&self, descending: bool) -> Column {
        let opts = RankOptions {
            method: RankMethod::Min,
            descending,
        };
        Self::from_expr(self.expr().clone().rank(opts, None), None)
    }

    /// Dense rank (no gaps). Use with `.over(partition_by)`.
    pub fn dense_rank(&self, descending: bool) -> Column {
        let opts = RankOptions {
            method: RankMethod::Dense,
            descending,
        };
        Self::from_expr(self.expr().clone().rank(opts, None), None)
    }

    /// Row number (1, 2, 3 by this column's order). Use with `.over(partition_by)`.
    pub fn row_number(&self, descending: bool) -> Column {
        let opts = RankOptions {
            method: RankMethod::Ordinal,
            descending,
        };
        Self::from_expr(self.expr().clone().rank(opts, None), None)
    }

    /// Lag: value from n rows before. Use with `.over(partition_by)`.
    pub fn lag(&self, n: i64) -> Column {
        Self::from_expr(self.expr().clone().shift(polars::prelude::lit(n)), None)
    }

    /// Lead: value from n rows after. Use with `.over(partition_by)`.
    pub fn lead(&self, n: i64) -> Column {
        Self::from_expr(self.expr().clone().shift(polars::prelude::lit(-n)), None)
    }

    /// First value in partition (PySpark first_value). Use with `.over(partition_by)`.
    pub fn first_value(&self) -> Column {
        Self::from_expr(self.expr().clone().first(), None)
    }

    /// Last value in partition (PySpark last_value). Use with `.over(partition_by)`.
    pub fn last_value(&self) -> Column {
        Self::from_expr(self.expr().clone().last(), None)
    }

    /// Percent rank in partition: (rank - 1) / (count - 1). Window is applied; do not call .over() again.
    pub fn percent_rank(&self, partition_by: &[&str], descending: bool) -> Column {
        use polars::prelude::*;
        let partition_exprs: Vec<Expr> = partition_by.iter().map(|s| col(*s)).collect();
        let opts = RankOptions {
            method: RankMethod::Min,
            descending,
        };
        let rank_expr = self
            .expr()
            .clone()
            .rank(opts, None)
            .over(partition_exprs.clone());
        let count_expr = self.expr().clone().count().over(partition_exprs.clone());
        let rank_f = (rank_expr - lit(1i64)).cast(DataType::Float64);
        let count_f = (count_expr - lit(1i64)).cast(DataType::Float64);
        let pct = rank_f / count_f;
        Self::from_expr(pct, None)
    }

    /// Cumulative distribution in partition: row_number / count. Window is applied; do not call .over() again.
    pub fn cume_dist(&self, partition_by: &[&str], descending: bool) -> Column {
        use polars::prelude::*;
        let partition_exprs: Vec<Expr> = partition_by.iter().map(|s| col(*s)).collect();
        let opts = RankOptions {
            method: RankMethod::Ordinal,
            descending,
        };
        let row_num = self
            .expr()
            .clone()
            .rank(opts, None)
            .over(partition_exprs.clone());
        let count_expr = self.expr().clone().count().over(partition_exprs.clone());
        let cume = row_num / count_expr;
        Self::from_expr(cume.cast(DataType::Float64), None)
    }

    /// Ntile: bucket 1..n by rank within partition (ceil(rank * n / count)). Window is applied; do not call .over() again.
    pub fn ntile(&self, n: u32, partition_by: &[&str], descending: bool) -> Column {
        use polars::prelude::*;
        let partition_exprs: Vec<Expr> = partition_by.iter().map(|s| col(*s)).collect();
        let opts = RankOptions {
            method: RankMethod::Ordinal,
            descending,
        };
        let rank_expr = self
            .expr()
            .clone()
            .rank(opts, None)
            .over(partition_exprs.clone());
        let count_expr = self.expr().clone().count().over(partition_exprs.clone());
        let n_expr = lit(n as f64);
        let rank_f = rank_expr.cast(DataType::Float64);
        let count_f = count_expr.cast(DataType::Float64);
        let bucket = (rank_f * n_expr / count_f).ceil();
        let clamped = bucket.clip(lit(1.0), lit(n as f64));
        Self::from_expr(clamped.cast(DataType::Int32), None)
    }

    /// Nth value in partition by order (1-based n). Returns a Column with window already applied; do not call .over() again.
    pub fn nth_value(&self, n: i64, partition_by: &[&str], descending: bool) -> Column {
        use polars::prelude::*;
        let partition_exprs: Vec<Expr> = partition_by.iter().map(|s| col(*s)).collect();
        let opts = RankOptions {
            method: RankMethod::Ordinal,
            descending,
        };
        let rank_expr = self
            .expr()
            .clone()
            .rank(opts, None)
            .over(partition_exprs.clone());
        let cond_col = Self::from_expr(rank_expr.eq(lit(n)), None);
        let null_col = Self::from_expr(Expr::Literal(LiteralValue::Null), None);
        let value_col = Self::from_expr(self.expr().clone(), None);
        let when_expr = crate::functions::when(&cond_col)
            .then(&value_col)
            .otherwise(&null_col)
            .into_expr();
        let windowed = when_expr.max().over(partition_exprs);
        Self::from_expr(windowed, None)
    }

    /// Number of elements in list (PySpark size / array_size). Returns Int32.
    pub fn array_size(&self) -> Column {
        use polars::prelude::*;
        Self::from_expr(
            self.expr().clone().list().len().cast(DataType::Int32),
            Some("size".to_string()),
        )
    }

    /// Cardinality: number of elements in array/list (PySpark cardinality). Alias for array_size.
    pub fn cardinality(&self) -> Column {
        self.array_size()
    }

    /// Check if list contains value (PySpark array_contains).
    pub fn array_contains(&self, value: Expr) -> Column {
        Self::from_expr(self.expr().clone().list().contains(value), None)
    }

    /// Join list of strings with separator (PySpark array_join).
    pub fn array_join(&self, separator: &str) -> Column {
        use polars::prelude::*;
        Self::from_expr(
            self.expr()
                .clone()
                .list()
                .join(lit(separator.to_string()), false),
            None,
        )
    }

    /// Maximum element in list (PySpark array_max).
    pub fn array_max(&self) -> Column {
        Self::from_expr(self.expr().clone().list().max(), None)
    }

    /// Minimum element in list (PySpark array_min).
    pub fn array_min(&self) -> Column {
        Self::from_expr(self.expr().clone().list().min(), None)
    }

    /// Get element at 1-based index (PySpark element_at). Returns null if out of bounds.
    pub fn element_at(&self, index: i64) -> Column {
        use polars::prelude::*;
        // PySpark uses 1-based indexing; Polars uses 0-based. index 1 -> get(0).
        let idx = if index >= 1 { index - 1 } else { index };
        Self::from_expr(self.expr().clone().list().get(lit(idx), true), None)
    }

    /// Sort list elements (PySpark array_sort). Ascending, nulls last.
    pub fn array_sort(&self) -> Column {
        use polars::prelude::SortOptions;
        let opts = SortOptions {
            descending: false,
            nulls_last: true,
            ..Default::default()
        };
        Self::from_expr(self.expr().clone().list().sort(opts), None)
    }

    /// Distinct elements in list (PySpark array_distinct). Order not guaranteed.
    pub fn array_distinct(&self) -> Column {
        Self::from_expr(self.expr().clone().list().unique(), None)
    }

    /// Mode aggregation - most frequent value (PySpark mode).
    /// Uses value_counts sorted by count descending, then first.
    pub fn mode(&self) -> Column {
        // value_counts(sort=true, parallel=false, name="count", normalize=false)
        // puts highest count first; first() gives the mode
        // Struct has "count" and value field; field 0 is typically the value
        let vc = self
            .expr()
            .clone()
            .value_counts(true, false, "count", false);
        let first_struct = vc.first();
        let val_expr = first_struct.struct_().field_by_index(0);
        Self::from_expr(val_expr, Some("mode".to_string()))
    }

    /// Slice list from start with optional length (PySpark slice). 1-based start.
    pub fn array_slice(&self, start: i64, length: Option<i64>) -> Column {
        use polars::prelude::*;
        let start_expr = lit((start - 1).max(0)); // 1-based to 0-based
        let length_expr = length.map(lit).unwrap_or_else(|| lit(i64::MAX));
        Self::from_expr(
            self.expr().clone().list().slice(start_expr, length_expr),
            None,
        )
    }

    /// Explode list into one row per element (PySpark explode).
    pub fn explode(&self) -> Column {
        Self::from_expr(self.expr().clone().explode(), None)
    }

    /// Explode list; null/empty produces one row with null (PySpark explode_outer).
    pub fn explode_outer(&self) -> Column {
        Self::from_expr(self.expr().clone().explode(), None)
    }

    /// Posexplode with null preservation (PySpark posexplode_outer).
    pub fn posexplode_outer(&self) -> (Column, Column) {
        self.posexplode()
    }

    /// Zip two arrays element-wise into array of structs (PySpark arrays_zip).
    pub fn arrays_zip(&self, other: &Column) -> Column {
        let args = [other.expr().clone()];
        let expr = self.expr().clone().map_many(
            crate::udfs::apply_arrays_zip,
            &args,
            GetOutput::same_type(),
        );
        Self::from_expr(expr, None)
    }

    /// True if two arrays have any element in common (PySpark arrays_overlap).
    pub fn arrays_overlap(&self, other: &Column) -> Column {
        let args = [other.expr().clone()];
        let expr = self.expr().clone().map_many(
            crate::udfs::apply_arrays_overlap,
            &args,
            GetOutput::from_type(DataType::Boolean),
        );
        Self::from_expr(expr, None)
    }

    /// Collect to array (PySpark array_agg). Alias for implode in group context.
    pub fn array_agg(&self) -> Column {
        Self::from_expr(self.expr().clone().implode(), None)
    }

    /// 1-based index of first occurrence of value in list, or 0 if not found (PySpark array_position).
    /// Uses Polars list.eval with col("") as element (requires polars list_eval feature).
    pub fn array_position(&self, value: Expr) -> Column {
        use polars::prelude::{DataType, NULL};
        // In list.eval context, col("") refers to the current list element.
        let cond = Self::from_expr(col("").eq(value), None);
        let then_val = Self::from_expr(col("").cum_count(false), None);
        let else_val = Self::from_expr(lit(NULL), None);
        let idx_expr = crate::functions::when(&cond)
            .then(&then_val)
            .otherwise(&else_val)
            .into_expr();
        let list_expr = self
            .expr()
            .clone()
            .list()
            .eval(idx_expr, false)
            .list()
            .min()
            .fill_null(lit(0i64))
            .cast(DataType::Int64);
        Self::from_expr(list_expr, Some("array_position".to_string()))
    }

    /// Remove null elements from list (PySpark array_compact). Preserves order.
    pub fn array_compact(&self) -> Column {
        let list_expr = self.expr().clone().list().drop_nulls();
        Self::from_expr(list_expr, None)
    }

    /// New list with all elements equal to value removed (PySpark array_remove).
    /// Uses list.eval + drop_nulls (requires polars list_eval and list_drop_nulls).
    pub fn array_remove(&self, value: Expr) -> Column {
        use polars::prelude::NULL;
        // when(element != value) then element else null; then drop_nulls.
        let cond = Self::from_expr(col("").neq(value), None);
        let then_val = Self::from_expr(col(""), None);
        let else_val = Self::from_expr(lit(NULL), None);
        let elem_neq = crate::functions::when(&cond)
            .then(&then_val)
            .otherwise(&else_val)
            .into_expr();
        let list_expr = self
            .expr()
            .clone()
            .list()
            .eval(elem_neq, false)
            .list()
            .drop_nulls();
        Self::from_expr(list_expr, None)
    }

    /// Repeat each element n times (PySpark array_repeat). Implemented via map UDF.
    pub fn array_repeat(&self, n: i64) -> Column {
        let expr = self.expr().clone().map(
            move |c| crate::udfs::apply_array_repeat(c, n),
            GetOutput::same_type(),
        );
        Self::from_expr(expr, None)
    }

    /// Flatten list of lists to one list (PySpark flatten). Implemented via map UDF.
    pub fn array_flatten(&self) -> Column {
        let expr = self
            .expr()
            .clone()
            .map(crate::udfs::apply_array_flatten, GetOutput::same_type());
        Self::from_expr(expr, None)
    }

    /// Append element to end of list (PySpark array_append).
    pub fn array_append(&self, elem: &Column) -> Column {
        let args = [elem.expr().clone()];
        let expr = self.expr().clone().map_many(
            crate::udfs::apply_array_append,
            &args,
            GetOutput::same_type(),
        );
        Self::from_expr(expr, None)
    }

    /// Prepend element to start of list (PySpark array_prepend).
    pub fn array_prepend(&self, elem: &Column) -> Column {
        let args = [elem.expr().clone()];
        let expr = self.expr().clone().map_many(
            crate::udfs::apply_array_prepend,
            &args,
            GetOutput::same_type(),
        );
        Self::from_expr(expr, None)
    }

    /// Insert element at 1-based position (PySpark array_insert).
    pub fn array_insert(&self, pos: &Column, elem: &Column) -> Column {
        let args = [pos.expr().clone(), elem.expr().clone()];
        let expr = self.expr().clone().map_many(
            crate::udfs::apply_array_insert,
            &args,
            GetOutput::same_type(),
        );
        Self::from_expr(expr, None)
    }

    /// Elements in first array not in second (PySpark array_except).
    pub fn array_except(&self, other: &Column) -> Column {
        let args = [other.expr().clone()];
        let expr = self.expr().clone().map_many(
            crate::udfs::apply_array_except,
            &args,
            GetOutput::same_type(),
        );
        Self::from_expr(expr, None)
    }

    /// Elements in both arrays (PySpark array_intersect).
    pub fn array_intersect(&self, other: &Column) -> Column {
        let args = [other.expr().clone()];
        let expr = self.expr().clone().map_many(
            crate::udfs::apply_array_intersect,
            &args,
            GetOutput::same_type(),
        );
        Self::from_expr(expr, None)
    }

    /// Distinct elements from both arrays (PySpark array_union).
    pub fn array_union(&self, other: &Column) -> Column {
        let args = [other.expr().clone()];
        let expr = self.expr().clone().map_many(
            crate::udfs::apply_array_union,
            &args,
            GetOutput::same_type(),
        );
        Self::from_expr(expr, None)
    }

    /// Zip two arrays element-wise with merge function (PySpark zip_with). Shorter array padded with null.
    /// Merge Expr uses col("").struct_().field_by_name("left") and field_by_name("right").
    pub fn zip_with(&self, other: &Column, merge: Expr) -> Column {
        let args = [other.expr().clone()];
        let zip_expr = self.expr().clone().map_many(
            crate::udfs::apply_zip_arrays_to_struct,
            &args,
            GetOutput::same_type(),
        );
        let list_expr = zip_expr.list().eval(merge, false);
        Self::from_expr(list_expr, None)
    }

    /// True if any list element satisfies the predicate (PySpark exists). Uses list.eval(pred).list().any().
    pub fn array_exists(&self, predicate: Expr) -> Column {
        let pred_expr = self
            .expr()
            .clone()
            .list()
            .eval(predicate, false)
            .list()
            .any();
        Self::from_expr(pred_expr, Some("exists".to_string()))
    }

    /// True if all list elements satisfy the predicate (PySpark forall). Uses list.eval(pred).list().all().
    pub fn array_forall(&self, predicate: Expr) -> Column {
        let pred_expr = self
            .expr()
            .clone()
            .list()
            .eval(predicate, false)
            .list()
            .all();
        Self::from_expr(pred_expr, Some("forall".to_string()))
    }

    /// Filter list elements by predicate (PySpark filter). Keeps elements where predicate is true.
    pub fn array_filter(&self, predicate: Expr) -> Column {
        use polars::prelude::NULL;
        let then_val = Self::from_expr(col(""), None);
        let else_val = Self::from_expr(lit(NULL), None);
        let elem_expr = crate::functions::when(&Self::from_expr(predicate, None))
            .then(&then_val)
            .otherwise(&else_val)
            .into_expr();
        let list_expr = self
            .expr()
            .clone()
            .list()
            .eval(elem_expr, false)
            .list()
            .drop_nulls();
        Self::from_expr(list_expr, None)
    }

    /// Transform list elements by expression (PySpark transform). list.eval(expr).
    pub fn array_transform(&self, f: Expr) -> Column {
        let list_expr = self.expr().clone().list().eval(f, false);
        Self::from_expr(list_expr, None)
    }

    /// Sum of list elements (PySpark aggregate with sum). Uses list.sum().
    pub fn array_sum(&self) -> Column {
        Self::from_expr(self.expr().clone().list().sum(), None)
    }

    /// Array fold/aggregate (PySpark aggregate). Simplified: zero + sum(list). Full (zero, merge, finish) deferred.
    pub fn array_aggregate(&self, zero: &Column) -> Column {
        let sum_expr = self.expr().clone().list().sum();
        Self::from_expr(sum_expr + zero.expr().clone(), None)
    }

    /// Mean of list elements (PySpark aggregate with avg). Uses list.mean().
    pub fn array_mean(&self) -> Column {
        Self::from_expr(self.expr().clone().list().mean(), None)
    }

    /// Explode list with position (PySpark posexplode). Returns (pos_col, value_col).
    /// pos is 1-based; uses list.eval(cum_count()).explode() and explode().
    pub fn posexplode(&self) -> (Column, Column) {
        let pos_expr = self
            .expr()
            .clone()
            .list()
            .eval(col("").cum_count(false), false)
            .explode();
        let val_expr = self.expr().clone().explode();
        (
            Self::from_expr(pos_expr, Some("pos".to_string())),
            Self::from_expr(val_expr, Some("col".to_string())),
        )
    }

    /// Extract keys from a map column (PySpark map_keys). Map column is List(Struct{key, value}).
    pub fn map_keys(&self) -> Column {
        let elem_key = col("").struct_().field_by_name("key");
        let list_expr = self.expr().clone().list().eval(elem_key, false);
        Self::from_expr(list_expr, None)
    }

    /// Extract values from a map column (PySpark map_values). Map column is List(Struct{key, value}).
    pub fn map_values(&self) -> Column {
        let elem_val = col("").struct_().field_by_name("value");
        let list_expr = self.expr().clone().list().eval(elem_val, false);
        Self::from_expr(list_expr, None)
    }

    /// Return map as list of structs {key, value} (PySpark map_entries). Identity for List(Struct) column.
    pub fn map_entries(&self) -> Column {
        Self::from_expr(self.expr().clone(), None)
    }

    /// Build map from two array columns (keys, values) (PySpark map_from_arrays). Implemented via map_many UDF.
    pub fn map_from_arrays(&self, values: &Column) -> Column {
        let args = [values.expr().clone()];
        let expr = self.expr().clone().map_many(
            crate::udfs::apply_map_from_arrays,
            &args,
            GetOutput::same_type(),
        );
        Self::from_expr(expr, None)
    }

    /// Merge two map columns (PySpark map_concat). Last value wins for duplicate keys.
    pub fn map_concat(&self, other: &Column) -> Column {
        let args = [other.expr().clone()];
        let expr = self.expr().clone().map_many(
            crate::udfs::apply_map_concat,
            &args,
            GetOutput::same_type(),
        );
        Self::from_expr(expr, None)
    }

    /// Transform each map key by expr (PySpark transform_keys). key_expr should use col("").struct_().field_by_name("key").
    pub fn transform_keys(&self, key_expr: Expr) -> Column {
        use polars::prelude::as_struct;
        let value = col("").struct_().field_by_name("value");
        let new_struct = as_struct(vec![key_expr.alias("key"), value.alias("value")]);
        let list_expr = self.expr().clone().list().eval(new_struct, false);
        Self::from_expr(list_expr, None)
    }

    /// Transform each map value by expr (PySpark transform_values). value_expr should use col("").struct_().field_by_name("value").
    pub fn transform_values(&self, value_expr: Expr) -> Column {
        use polars::prelude::as_struct;
        let key = col("").struct_().field_by_name("key");
        let new_struct = as_struct(vec![key.alias("key"), value_expr.alias("value")]);
        let list_expr = self.expr().clone().list().eval(new_struct, false);
        Self::from_expr(list_expr, None)
    }

    /// Merge two maps by key with merge function (PySpark map_zip_with).
    /// Merge Expr uses col("").struct_().field_by_name("value1") and field_by_name("value2").
    pub fn map_zip_with(&self, other: &Column, merge: Expr) -> Column {
        use polars::prelude::as_struct;
        let args = [other.expr().clone()];
        let zip_expr = self.expr().clone().map_many(
            crate::udfs::apply_map_zip_to_struct,
            &args,
            GetOutput::same_type(),
        );
        let key_field = col("").struct_().field_by_name("key").alias("key");
        let value_field = merge.alias("value");
        let merge_expr = as_struct(vec![key_field, value_field]);
        let list_expr = zip_expr.list().eval(merge_expr, false);
        Self::from_expr(list_expr, None)
    }

    /// Filter map entries by predicate (PySpark map_filter). Keeps key-value pairs where predicate is true.
    /// Predicate uses col("").struct_().field_by_name("key") and field_by_name("value") to reference key/value.
    pub fn map_filter(&self, predicate: Expr) -> Column {
        use polars::prelude::NULL;
        let then_val = Self::from_expr(col(""), None);
        let else_val = Self::from_expr(lit(NULL), None);
        let elem_expr = crate::functions::when(&Self::from_expr(predicate, None))
            .then(&then_val)
            .otherwise(&else_val)
            .into_expr();
        let list_expr = self
            .expr()
            .clone()
            .list()
            .eval(elem_expr, false)
            .list()
            .drop_nulls();
        Self::from_expr(list_expr, None)
    }

    /// Array of structs {key, value} to map (PySpark map_from_entries). Identity for List(Struct) format.
    pub fn map_from_entries(&self) -> Column {
        Self::from_expr(self.expr().clone(), None)
    }

    /// True if map contains key (PySpark map_contains_key).
    pub fn map_contains_key(&self, key: &Column) -> Column {
        let args = [key.expr().clone()];
        let expr = self.expr().clone().map_many(
            crate::udfs::apply_map_contains_key,
            &args,
            GetOutput::from_type(DataType::Boolean),
        );
        Self::from_expr(expr, None)
    }

    /// Get value for key from map, or null (PySpark get).
    pub fn get(&self, key: &Column) -> Column {
        let args = [key.expr().clone()];
        let expr =
            self.expr()
                .clone()
                .map_many(crate::udfs::apply_get, &args, GetOutput::same_type());
        Self::from_expr(expr, None)
    }

    /// Extract JSON path from string column (PySpark get_json_object). Uses Polars str().json_path_match.
    pub fn get_json_object(&self, path: &str) -> Column {
        let path_expr = polars::prelude::lit(path.to_string());
        let out = self.expr().clone().str().json_path_match(path_expr);
        Self::from_expr(out, None)
    }

    /// Parse string column as JSON into struct (PySpark from_json). Uses Polars str().json_decode.
    pub fn from_json(&self, schema: Option<polars::datatypes::DataType>) -> Column {
        let out = self.expr().clone().str().json_decode(schema, None);
        Self::from_expr(out, None)
    }

    /// Serialize struct column to JSON string (PySpark to_json). Uses Polars struct().json_encode.
    pub fn to_json(&self) -> Column {
        let out = self.expr().clone().struct_().json_encode();
        Self::from_expr(out, None)
    }

    /// Length of JSON array at path (PySpark json_array_length). UDF.
    pub fn json_array_length(&self, path: &str) -> Column {
        let path = path.to_string();
        let expr = self.expr().clone().map(
            move |s| crate::udfs::apply_json_array_length(s, &path),
            GetOutput::from_type(DataType::Int64),
        );
        Self::from_expr(expr, None)
    }

    /// Keys of JSON object (PySpark json_object_keys). Returns list of strings. UDF.
    pub fn json_object_keys(&self) -> Column {
        let expr = self.expr().clone().map(
            crate::udfs::apply_json_object_keys,
            GetOutput::from_type(DataType::List(Box::new(DataType::String))),
        );
        Self::from_expr(expr, None)
    }

    /// Extract keys from JSON as struct (PySpark json_tuple). UDF. Returns struct with one string field per key.
    pub fn json_tuple(&self, keys: &[&str]) -> Column {
        let keys_vec: Vec<String> = keys.iter().map(|s| (*s).to_string()).collect();
        let struct_fields: Vec<polars::datatypes::Field> = keys_vec
            .iter()
            .map(|k| polars::datatypes::Field::new(k.as_str().into(), DataType::String))
            .collect();
        let expr = self.expr().clone().map(
            move |s| crate::udfs::apply_json_tuple(s, &keys_vec),
            GetOutput::from_type(DataType::Struct(struct_fields)),
        );
        Self::from_expr(expr, None)
    }

    /// Parse CSV string to struct (PySpark from_csv). Minimal: split by comma, up to 32 columns. UDF.
    pub fn from_csv(&self) -> Column {
        let expr = self.expr().clone().map(
            crate::udfs::apply_from_csv,
            GetOutput::from_type(DataType::Struct(vec![])),
        );
        Self::from_expr(expr, None)
    }

    /// Format struct as CSV string (PySpark to_csv). Minimal. UDF.
    pub fn to_csv(&self) -> Column {
        let expr = self.expr().clone().map(
            crate::udfs::apply_to_csv,
            GetOutput::from_type(DataType::String),
        );
        Self::from_expr(expr, None)
    }

    /// Parse URL and extract part (PySpark parse_url). UDF.
    /// When part is QUERY/QUERYSTRING and key is Some(k), returns the value for that query parameter only.
    pub fn parse_url(&self, part: &str, key: Option<&str>) -> Column {
        let part = part.to_string();
        let key_owned = key.map(String::from);
        let expr = self.expr().clone().map(
            move |s| crate::udfs::apply_parse_url(s, &part, key_owned.as_deref()),
            GetOutput::from_type(DataType::String),
        );
        Self::from_expr(expr, None)
    }

    /// Hash of column value (PySpark hash). Single-column version.
    pub fn hash(&self) -> Column {
        let expr = self.expr().clone().map(
            crate::udfs::apply_hash_one,
            GetOutput::from_type(DataType::Int64),
        );
        Self::from_expr(expr, None)
    }

    /// Check if column values are in the other column's list/series (PySpark isin).
    pub fn isin(&self, other: &Column) -> Column {
        let out = self.expr().clone().is_in(other.expr().clone());
        Self::from_expr(out, None)
    }

    /// Percent-decode URL-encoded string (PySpark url_decode). Uses UDF.
    pub fn url_decode(&self) -> Column {
        let expr = self.expr().clone().map(
            crate::udfs::apply_url_decode,
            GetOutput::from_type(DataType::String),
        );
        Self::from_expr(expr, None)
    }

    /// Percent-encode string for URL (PySpark url_encode). Uses UDF.
    pub fn url_encode(&self) -> Column {
        let expr = self.expr().clone().map(
            crate::udfs::apply_url_encode,
            GetOutput::from_type(DataType::String),
        );
        Self::from_expr(expr, None)
    }

    /// Bitwise left shift (PySpark shiftLeft). col << n = col * 2^n.
    pub fn shift_left(&self, n: i32) -> Column {
        use polars::prelude::*;
        let pow = lit(2i64).pow(lit(n as i64));
        Self::from_expr(
            (self.expr().clone().cast(DataType::Int64) * pow).cast(DataType::Int64),
            None,
        )
    }

    /// Bitwise signed right shift (PySpark shiftRight). col >> n = col / 2^n.
    pub fn shift_right(&self, n: i32) -> Column {
        use polars::prelude::*;
        let pow = lit(2i64).pow(lit(n as i64));
        Self::from_expr(
            (self.expr().clone().cast(DataType::Int64) / pow).cast(DataType::Int64),
            None,
        )
    }

    /// Bitwise unsigned right shift (PySpark shiftRightUnsigned). Logical shift.
    pub fn shift_right_unsigned(&self, n: i32) -> Column {
        let expr = self.expr().clone().map(
            move |s| crate::udfs::apply_shift_right_unsigned(s, n),
            GetOutput::from_type(DataType::Int64),
        );
        Self::from_expr(expr, None)
    }
}

#[cfg(test)]
mod tests {
    use super::Column;
    use polars::prelude::{col, df, lit, IntoLazy};

    /// Helper to create a simple DataFrame for testing
    fn test_df() -> polars::prelude::DataFrame {
        df!(
            "a" => &[1, 2, 3, 4, 5],
            "b" => &[10, 20, 30, 40, 50]
        )
        .unwrap()
    }

    /// Helper to create a DataFrame with nulls for testing
    fn test_df_with_nulls() -> polars::prelude::DataFrame {
        df!(
            "a" => &[Some(1), Some(2), None, Some(4), None],
            "b" => &[Some(10), None, Some(30), None, None]
        )
        .unwrap()
    }

    #[test]
    fn test_column_new() {
        let column = Column::new("age".to_string());
        assert_eq!(column.name(), "age");
    }

    #[test]
    fn test_column_from_expr() {
        let expr = col("test");
        let column = Column::from_expr(expr, Some("test".to_string()));
        assert_eq!(column.name(), "test");
    }

    #[test]
    fn test_column_from_expr_default_name() {
        let expr = col("test").gt(lit(5));
        let column = Column::from_expr(expr, None);
        assert_eq!(column.name(), "<expr>");
    }

    #[test]
    fn test_column_alias() {
        let column = Column::new("original".to_string());
        let aliased = column.alias("new_name");
        assert_eq!(aliased.name(), "new_name");
    }

    #[test]
    fn test_column_gt() {
        let df = test_df();
        let column = Column::new("a".to_string());
        let result = column.gt(lit(3));

        // Apply the expression to filter the DataFrame
        let filtered = df.lazy().filter(result.into_expr()).collect().unwrap();
        assert_eq!(filtered.height(), 2); // rows with a > 3: 4, 5
    }

    #[test]
    fn test_column_lt() {
        let df = test_df();
        let column = Column::new("a".to_string());
        let result = column.lt(lit(3));

        let filtered = df.lazy().filter(result.into_expr()).collect().unwrap();
        assert_eq!(filtered.height(), 2); // rows with a < 3: 1, 2
    }

    #[test]
    fn test_column_eq() {
        let df = test_df();
        let column = Column::new("a".to_string());
        let result = column.eq(lit(3));

        let filtered = df.lazy().filter(result.into_expr()).collect().unwrap();
        assert_eq!(filtered.height(), 1); // only row with a == 3
    }

    #[test]
    fn test_column_neq() {
        let df = test_df();
        let column = Column::new("a".to_string());
        let result = column.neq(lit(3));

        let filtered = df.lazy().filter(result.into_expr()).collect().unwrap();
        assert_eq!(filtered.height(), 4); // rows with a != 3
    }

    #[test]
    fn test_column_gt_eq() {
        let df = test_df();
        let column = Column::new("a".to_string());
        let result = column.gt_eq(lit(3));

        let filtered = df.lazy().filter(result.into_expr()).collect().unwrap();
        assert_eq!(filtered.height(), 3); // rows with a >= 3: 3, 4, 5
    }

    #[test]
    fn test_column_lt_eq() {
        let df = test_df();
        let column = Column::new("a".to_string());
        let result = column.lt_eq(lit(3));

        let filtered = df.lazy().filter(result.into_expr()).collect().unwrap();
        assert_eq!(filtered.height(), 3); // rows with a <= 3: 1, 2, 3
    }

    #[test]
    fn test_column_is_null() {
        let df = test_df_with_nulls();
        let column = Column::new("a".to_string());
        let result = column.is_null();

        let filtered = df.lazy().filter(result.into_expr()).collect().unwrap();
        assert_eq!(filtered.height(), 2); // 2 null values in column 'a'
    }

    #[test]
    fn test_column_is_not_null() {
        let df = test_df_with_nulls();
        let column = Column::new("a".to_string());
        let result = column.is_not_null();

        let filtered = df.lazy().filter(result.into_expr()).collect().unwrap();
        assert_eq!(filtered.height(), 3); // 3 non-null values in column 'a'
    }

    #[test]
    fn test_eq_null_safe_both_null() {
        // Create a DataFrame where both columns have NULL at the same row
        let df = df!(
            "a" => &[Some(1), None, Some(3)],
            "b" => &[Some(1), None, Some(4)]
        )
        .unwrap();

        let col_a = Column::new("a".to_string());
        let col_b = Column::new("b".to_string());
        let result = col_a.eq_null_safe(&col_b);

        // Apply the expression and collect
        let result_df = df
            .lazy()
            .with_column(result.into_expr().alias("eq_null_safe"))
            .collect()
            .unwrap();

        // Get the result column
        let eq_col = result_df.column("eq_null_safe").unwrap();
        let values: Vec<Option<bool>> = eq_col.bool().unwrap().into_iter().collect();

        // Row 0: 1 == 1 -> true
        // Row 1: NULL <=> NULL -> true
        // Row 2: 3 == 4 -> false
        assert_eq!(values[0], Some(true));
        assert_eq!(values[1], Some(true)); // NULL-safe: both NULL = true
        assert_eq!(values[2], Some(false));
    }

    #[test]
    fn test_eq_null_safe_one_null() {
        // Create a DataFrame where only one column has NULL
        let df = df!(
            "a" => &[Some(1), None, Some(3)],
            "b" => &[Some(1), Some(2), None]
        )
        .unwrap();

        let col_a = Column::new("a".to_string());
        let col_b = Column::new("b".to_string());
        let result = col_a.eq_null_safe(&col_b);

        let result_df = df
            .lazy()
            .with_column(result.into_expr().alias("eq_null_safe"))
            .collect()
            .unwrap();

        let eq_col = result_df.column("eq_null_safe").unwrap();
        let values: Vec<Option<bool>> = eq_col.bool().unwrap().into_iter().collect();

        // Row 0: 1 == 1 -> true
        // Row 1: NULL <=> 2 -> false (one is null, not both)
        // Row 2: 3 <=> NULL -> false (one is null, not both)
        assert_eq!(values[0], Some(true));
        assert_eq!(values[1], Some(false));
        assert_eq!(values[2], Some(false));
    }
}

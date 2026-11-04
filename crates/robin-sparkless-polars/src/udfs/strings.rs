//! Strings UDF implementations.

use super::*;

/// ASCII value of first character (PySpark ascii). Returns Int32.
pub fn apply_ascii(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series.str().map_err(|e| compute_err("ascii", e))?;
    let out = Int32Chunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter()
            .map(|opt_s| opt_s.and_then(|s| s.chars().next().map(|c| c as i32))),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// Format numeric column as string with fixed decimal places (PySpark format_number).
pub fn apply_format_number(column: Column, decimals: u32) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let prec = decimals as usize;
    let out: StringChunked = match series.dtype() {
        DataType::Float64 => {
            let ca = series.f64().map_err(|e| compute_err("format_number", e))?;
            StringChunked::from_iter_options(
                name.as_str().into(),
                ca.into_iter()
                    .map(|opt_v| opt_v.map(|v| format!("{v:.prec$}"))),
            )
        }
        DataType::Float32 => {
            let ca = series.f32().map_err(|e| compute_err("format_number", e))?;
            StringChunked::from_iter_options(
                name.as_str().into(),
                ca.into_iter()
                    .map(|opt_v| opt_v.map(|v| format!("{v:.prec$}"))),
            )
        }
        _ => {
            let f64_series = series
                .cast(&DataType::Float64)
                .map_err(|e| compute_err("format_number cast", e))?;
            let ca = f64_series
                .f64()
                .map_err(|e| compute_err("format_number", e))?;
            StringChunked::from_iter_options(
                name.as_str().into(),
                ca.into_iter()
                    .map(|opt_v| opt_v.map(|v| format!("{v:.prec$}"))),
            )
        }
    };
    Ok(Some(Column::new(name, out.into_series())))
}

/// Return the value at index `i` in `s` as a string, or None if null/unsupported type.
fn series_value_at_as_string(s: &Series, i: usize) -> Option<String> {
    match s.dtype() {
        DataType::String => s.str().ok()?.get(i).map(|v| v.to_string()),
        DataType::Int32 => s.i32().ok()?.get(i).map(|v| v.to_string()),
        DataType::Int64 => s.i64().ok()?.get(i).map(|v| v.to_string()),
        DataType::Float32 => s.f32().ok()?.get(i).map(|v| v.to_string()),
        DataType::Float64 => s.f64().ok()?.get(i).map(|v| v.to_string()),
        DataType::Boolean => s.bool().ok()?.get(i).map(|v| v.to_string()),
        _ => s.get(i).ok().map(|av| av.to_string()),
    }
}

/// Format columns with printf-style format string (PySpark format_string / printf).
/// Supports %s, %d, %i, %f, %g, %%. Null in any column yields null result.
pub fn apply_format_string(columns: &mut [Column], format: &str) -> PolarsResult<Option<Column>> {
    let n = columns.len();
    if n == 0 {
        return Err(PolarsError::ComputeError(
            "format_string needs at least one column".into(),
        ));
    }
    let name = columns[0].field().into_owned().name;
    let mut series_vec: Vec<Series> = Vec::with_capacity(n);
    for col in columns.iter_mut() {
        series_vec.push(std::mem::take(col).take_materialized_series());
    }
    let len = series_vec[0].len();
    let mut result = Vec::with_capacity(len);
    for i in 0..len {
        let values: Vec<Option<String>> = series_vec
            .iter()
            .map(|s| series_value_at_as_string(s, i))
            .collect();
        let out = format_string_row(format, &values);
        result.push(out);
    }
    let out = StringChunked::from_iter_options(name.as_str().into(), result.into_iter());
    Ok(Some(Column::new(name, out.into_series())))
}

/// PySpark format_string: null in any column is rendered as the literal string "null", not as null result.
fn format_string_row(format: &str, values: &[Option<String>]) -> Option<String> {
    const NULL_STR: &str = "null";
    let mut out = String::new();
    let mut idx = 0usize;
    let mut chars = format.chars().peekable();

    while let Some(c) = chars.next() {
        if c != '%' {
            out.push(c);
            continue;
        }

        // "%%" -> literal '%'
        if let Some('%') = chars.peek().copied() {
            chars.next();
            out.push('%');
            continue;
        }

        // Parse flags
        let mut flags = String::new();
        while let Some(ch) = chars.peek().copied() {
            if "-+ 0#".contains(ch) {
                flags.push(ch);
                chars.next();
            } else {
                break;
            }
        }

        // Parse width
        let mut width_str = String::new();
        while let Some(ch) = chars.peek().copied() {
            if ch.is_ascii_digit() {
                width_str.push(ch);
                chars.next();
            } else {
                break;
            }
        }
        let width = width_str.parse::<usize>().ok();

        // Parse precision
        let mut precision: Option<usize> = None;
        if let Some('.') = chars.peek().copied() {
            chars.next();
            let mut prec_str = String::new();
            while let Some(ch) = chars.peek().copied() {
                if ch.is_ascii_digit() {
                    prec_str.push(ch);
                    chars.next();
                } else {
                    break;
                }
            }
            precision = prec_str.parse::<usize>().ok();
        }

        // Ignore length modifiers (h, l, L)
        while let Some(ch) = chars.peek().copied() {
            if matches!(ch, 'h' | 'l' | 'L') {
                chars.next();
            } else {
                break;
            }
        }

        let spec = chars.next()?;

        if idx >= values.len() {
            return None;
        }
        let val_opt = &values[idx];
        idx += 1;

        let zero_pad = flags.contains('0');

        match spec {
            's' => {
                let s = val_opt.as_deref().unwrap_or(NULL_STR);
                out.push_str(s);
            }
            'd' | 'i' | 'o' | 'x' | 'X' => {
                if val_opt.is_none() {
                    out.push_str(NULL_STR);
                    continue;
                }
                let s_raw = val_opt.as_deref().unwrap();
                let mut n: i64 = match s_raw.parse() {
                    Ok(v) => v,
                    Err(_) => {
                        out.push_str(s_raw);
                        continue;
                    }
                };
                let negative = n < 0;
                if negative {
                    n = -n;
                }
                let mut s = match spec {
                    'd' | 'i' => format!("{n}"),
                    'o' => format!("{n:o}"),
                    'x' => format!("{n:x}"),
                    'X' => format!("{n:X}"),
                    _ => unreachable!(),
                };
                if let Some(w) = width {
                    if s.len() < w {
                        let pad_len = w - s.len();
                        let pad_char = if zero_pad { '0' } else { ' ' };
                        let pad = pad_char.to_string().repeat(pad_len);
                        s = format!("{pad}{s}");
                    }
                }
                if negative {
                    if zero_pad {
                        if let Some(w) = width {
                            if s.len() < w {
                                let pad_len = w - s.len();
                                let pad = "0".repeat(pad_len);
                                s = format!("-{pad}{s}");
                            } else {
                                s = format!("-{s}");
                            }
                        } else {
                            s = format!("-{s}");
                        }
                    } else {
                        s = format!("-{s}");
                    }
                }
                out.push_str(&s);
            }
            'f' | 'g' | 'e' | 'E' => {
                if val_opt.is_none() {
                    out.push_str(NULL_STR);
                    continue;
                }
                let s_raw = val_opt.as_deref().unwrap();
                let v: f64 = match s_raw.parse() {
                    Ok(v) => v,
                    Err(_) => {
                        out.push_str(s_raw);
                        continue;
                    }
                };
                let prec = precision.unwrap_or(6);
                let formatted = match spec {
                    'f' => format!("{v:.prec$}"),
                    'g' => format!("{v:.prec$}"),
                    'e' => format!("{v:.prec$e}"),
                    'E' => format!("{v:.prec$E}"),
                    _ => unreachable!(),
                };
                out.push_str(&formatted);
            }
            '%' => {
                out.push('%');
            }
            _ => {
                // Unsupported specifier: treat as null result.
                return None;
            }
        }
    }

    Some(out)
}

/// Find 1-based index of str in comma-delimited set (PySpark find_in_set).
/// Returns 0 if not found or if str contains comma.
/// map_many: `columns[0]`=str, `columns[1]`=set
pub fn apply_find_in_set(columns: &mut [Column]) -> PolarsResult<Option<Column>> {
    if columns.len() < 2 {
        return Err(PolarsError::ComputeError(
            "find_in_set needs two columns".into(),
        ));
    }
    let name = columns[0].field().into_owned().name;
    let str_series = std::mem::take(&mut columns[0]).take_materialized_series();
    let set_series = std::mem::take(&mut columns[1]).take_materialized_series();
    let str_ca = str_series
        .str()
        .map_err(|e| compute_err("find_in_set", e))?;
    let set_ca = set_series
        .str()
        .map_err(|e| compute_err("find_in_set", e))?;
    let out = Int64Chunked::from_iter_options(
        name.as_str().into(),
        str_ca
            .into_iter()
            .zip(set_ca)
            .map(|(opt_str, opt_set)| match (opt_str, opt_set) {
                (Some(s), Some(set_str)) => {
                    if s.contains(',') {
                        Some(0i64)
                    } else {
                        let parts: Vec<&str> = set_str.split(',').collect();
                        let idx = parts.iter().position(|p| *p == s);
                        Some(idx.map(|i| (i + 1) as i64).unwrap_or(0))
                    }
                }
                _ => None,
            }),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// regexp_extract using fancy-regex when pattern has lookahead/lookbehind (PySpark parity).
/// Polars str().extract() uses regex crate which does not support lookaround.
pub fn apply_regexp_extract_lookaround(
    column: Column,
    pattern: &str,
    group_index: usize,
) -> PolarsResult<Option<Column>> {
    use fancy_regex::Regex;
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series.str().map_err(|e| compute_err("regexp_extract", e))?;
    let re = Regex::new(pattern).map_err(|e| {
        compute_err(
            &format!("regexp_extract invalid regex (lookaround) '{pattern}'"),
            e,
        )
    })?;
    let out = StringChunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter().map(|opt_s| {
            // Null input stays null; no-match yields empty string (PySpark parity).
            opt_s.map(|s| {
                let caps_opt = re.captures(s).ok().flatten();
                if let Some(caps) = caps_opt {
                    if let Some(m) = caps.get(group_index) {
                        return m.as_str().to_string();
                    }
                }
                String::new()
            })
        }),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// regexp_extract using regex crate (no lookaround). PySpark semantics:
/// - null input -> null
/// - non-null, no match -> empty string
/// - non-null, match -> captured group (or full match for group 0).
pub fn apply_regexp_extract(
    column: Column,
    pattern: &str,
    group_index: usize,
) -> PolarsResult<Option<Column>> {
    use polars::prelude::*;
    use regex::Regex;
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let casted = if series.dtype() == &DataType::String {
        series
    } else {
        series
            .cast(&DataType::String)
            .map_err(|e| compute_err("regexp_extract cast", e))?
    };
    let ca = casted.str().map_err(|e| compute_err("regexp_extract", e))?;
    let re = Regex::new(pattern)
        .map_err(|e| compute_err(&format!("regexp_extract invalid regex '{pattern}'"), e))?;
    let out = StringChunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter().map(|opt_s| {
            // Null input stays null; no-match yields empty string (PySpark parity).
            opt_s.map(|s| {
                let caps_opt = re.captures(s);
                if let Some(caps) = caps_opt {
                    if let Some(m) = caps.get(group_index) {
                        return m.as_str().to_string();
                    }
                }
                String::new()
            })
        }),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// regexp_extract_all(column, pattern, group_index) using fancy-regex for capture groups and lookaround.
/// Returns list of strings; null input -> null output; non-matching -> empty list.
pub fn apply_regexp_extract_all_group(
    column: Column,
    pattern: &str,
    group_index: usize,
) -> PolarsResult<Option<Column>> {
    use fancy_regex::Regex;
    use polars::prelude::*;
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series
        .str()
        .map_err(|e| compute_err("regexp_extract_all", e))?;
    let re = Regex::new(pattern)
        .map_err(|e| compute_err(&format!("regexp_extract_all invalid regex '{pattern}'"), e))?;

    let mut out: ListChunked = ca
        .into_iter()
        .map(|opt_s| {
            opt_s.map(|s| {
                let mut vals: Vec<String> = Vec::new();
                for caps in re.captures_iter(s).flatten() {
                    if let Some(m) = caps.get(group_index) {
                        vals.push(m.as_str().to_string());
                    }
                }
                Series::new(PlSmallStr::EMPTY, vals)
            })
        })
        .collect();
    out.rename(name.clone());
    Ok(Some(Column::new(name, out.into_series())))
}

/// regexp_like / rlike using fancy-regex when pattern needs lookaround support.
pub fn apply_regexp_like_lookaround(column: Column, pattern: &str) -> PolarsResult<Option<Column>> {
    use fancy_regex::Regex;
    use polars::prelude::*;
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let casted = if series.dtype() == &DataType::String {
        series
    } else {
        series
            .cast(&DataType::String)
            .map_err(|e| compute_err("rlike cast", e))?
    };
    let ca = casted.str().map_err(|e| compute_err("rlike", e))?;
    let re = Regex::new(pattern)
        .map_err(|e| compute_err(&format!("rlike invalid regex '{pattern}'"), e))?;
    let out = BooleanChunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter()
            .map(|opt_s| opt_s.map(|s| re.is_match(s).unwrap_or(false))),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// Regexp instr: 1-based position of first regex match (PySpark regexp_instr).
/// group_idx: 0 = full match, 1+ = capture group. Returns null if no match.
pub fn apply_regexp_instr(
    column: Column,
    pattern: String,
    group_idx: usize,
) -> PolarsResult<Option<Column>> {
    use regex::Regex;
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series.str().map_err(|e| compute_err("regexp_instr", e))?;
    let re = Regex::new(&pattern)
        .map_err(|e| compute_err(&format!("regexp_instr invalid regex '{pattern}'"), e))?;
    let out = Int64Chunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter().map(|opt_s| {
            opt_s.and_then(|s| {
                let m = if group_idx == 0 {
                    re.find(s)
                } else {
                    re.captures(s).and_then(|cap| cap.get(group_idx))
                };
                m.map(|m| (m.start() + 1) as i64)
            })
        }),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// Base64 encode string bytes (PySpark base64). Input string UTF-8, output base64 string.
pub fn apply_base64(column: Column) -> PolarsResult<Option<Column>> {
    use base64::Engine;
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series.str().map_err(|e| compute_err("base64", e))?;
    let out = StringChunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter().map(|opt_s| {
            opt_s.map(|s| base64::engine::general_purpose::STANDARD.encode(s.as_bytes()))
        }),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// Base64 decode to string (PySpark unbase64). Output UTF-8 string; invalid decode → null.
pub fn apply_unbase64(column: Column) -> PolarsResult<Option<Column>> {
    use base64::Engine;
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series.str().map_err(|e| compute_err("unbase64", e))?;
    let out = StringChunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter().map(|opt_s| {
            opt_s.and_then(|s| {
                let decoded = base64::engine::general_purpose::STANDARD
                    .decode(s.as_bytes())
                    .ok()?;
                String::from_utf8(decoded).ok()
            })
        }),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// SHA1 hash of string bytes, return hex string (PySpark sha1).
pub fn apply_sha1(column: Column) -> PolarsResult<Option<Column>> {
    use sha1::Digest;
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series.str().map_err(|e| compute_err("sha1", e))?;
    let out = StringChunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter().map(|opt_s| {
            opt_s.map(|s| {
                let mut hasher = sha1::Sha1::new();
                hasher.update(s.as_bytes());
                format!("{:x}", hasher.finalize())
            })
        }),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// SHA2 hash of string bytes, return hex string (PySpark sha2). bit_length 256 or 384 or 512.
pub fn apply_sha2(column: Column, bit_length: i32) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series.str().map_err(|e| compute_err("sha2", e))?;
    let out = StringChunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter().map(|opt_s| {
            opt_s.map(|s| {
                let bytes = s.as_bytes();
                use sha2::Digest;
                match bit_length {
                    256 => format!("{:x}", sha2::Sha256::digest(bytes)),
                    384 => format!("{:x}", sha2::Sha384::digest(bytes)),
                    512 => format!("{:x}", sha2::Sha512::digest(bytes)),
                    _ => format!("{:x}", sha2::Sha256::digest(bytes)),
                }
            })
        }),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// MD5 hash of string bytes, return hex string (PySpark md5).
pub fn apply_md5(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series.str().map_err(|e| compute_err("md5", e))?;
    let out = StringChunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter()
            .map(|opt_s| opt_s.map(|s| format!("{:x}", md5::compute(s.as_bytes())))),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// Encode string to binary (PySpark encode). Charset: UTF-8, hex. Returns hex string representation of bytes.
pub fn apply_encode(column: Column, charset: &str) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series.str().map_err(|e| compute_err("encode", e))?;
    let cs = charset.to_lowercase();
    let out = StringChunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter().map(|opt_s| {
            opt_s.map(|s| {
                let bytes: Vec<u8> = match cs.as_str() {
                    "utf-8" | "utf8" => s.as_bytes().to_vec(),
                    _ => s.as_bytes().to_vec(), // default UTF-8
                };
                hex::encode(bytes)
            })
        }),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// Decode binary (hex string) to string (PySpark decode). Charset: UTF-8.
pub fn apply_decode(column: Column, charset: &str) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series.str().map_err(|e| compute_err("decode", e))?;
    let _ = charset;
    let out = StringChunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter().map(|opt_s| {
            opt_s.and_then(|s| {
                let bytes = hex::decode(s.as_bytes()).ok()?;
                String::from_utf8(bytes).ok()
            })
        }),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// to_binary(expr, fmt): PySpark to_binary. fmt 'utf-8' => hex(utf8 bytes), 'hex' => validate and return hex. Returns hex string.
pub fn apply_to_binary(column: Column, fmt: &str) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series.str().map_err(|e| compute_err("to_binary", e))?;
    let fmt_lower = fmt.to_lowercase();
    let out = StringChunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter().map(|opt_s| {
            opt_s.and_then(|s| {
                let hex_str = match fmt_lower.as_str() {
                    "hex" => {
                        // Input is hex string; validate and return as-is (binary representation)
                        hex::decode(s.as_bytes()).ok()?;
                        Some(s.to_string())
                    }
                    "utf-8" | "utf8" => Some(hex::encode(s.as_bytes())),
                    _ => Some(hex::encode(s.as_bytes())),
                };
                hex_str
            })
        }),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// try_to_binary: like to_binary but returns null on failure.
pub fn apply_try_to_binary(column: Column, fmt: &str) -> PolarsResult<Option<Column>> {
    apply_to_binary(column, fmt)
}

/// AES-GCM encrypt (PySpark aes_encrypt). Key: UTF-8 string, 16 or 32 bytes for AES-128/256. Output: hex(nonce||ciphertext).
fn aes_gcm_encrypt_one(plaintext: &[u8], key: &[u8]) -> Option<String> {
    use aes_gcm::Aes128Gcm;
    use aes_gcm::aead::generic_array::GenericArray;
    use aes_gcm::aead::{Aead, KeyInit};
    use rand::RngCore;
    let key_arr: [u8; 16] = key
        .iter()
        .copied()
        .chain(std::iter::repeat(0))
        .take(16)
        .collect::<Vec<_>>()
        .try_into()
        .ok()?;
    let cipher = Aes128Gcm::new(GenericArray::from_slice(&key_arr));
    let mut nonce = [0u8; 12];
    rand::thread_rng().fill_bytes(&mut nonce);
    let nonce = GenericArray::from_slice(&nonce);
    let ciphertext = cipher.encrypt(nonce, plaintext).ok()?;
    let mut out = nonce.as_slice().to_vec();
    out.extend(ciphertext);
    Some(hex::encode(out))
}

/// AES-GCM decrypt (PySpark aes_decrypt). Input: hex(nonce||ciphertext).
fn aes_gcm_decrypt_one(hex_input: &str, key: &[u8]) -> Option<String> {
    use aes_gcm::Aes128Gcm;
    use aes_gcm::aead::generic_array::GenericArray;
    use aes_gcm::aead::{Aead, KeyInit};
    let bytes = hex::decode(hex_input.as_bytes()).ok()?;
    if bytes.len() < 12 + 16 {
        return None; // nonce + at least tag
    }
    let (nonce_bytes, ct) = bytes.split_at(12);
    let key_arr: [u8; 16] = key
        .iter()
        .copied()
        .chain(std::iter::repeat(0))
        .take(16)
        .collect::<Vec<_>>()
        .try_into()
        .ok()?;
    let cipher = Aes128Gcm::new(GenericArray::from_slice(&key_arr));
    let nonce = GenericArray::from_slice(nonce_bytes);
    let plaintext = cipher.decrypt(nonce, ct).ok()?;
    String::from_utf8(plaintext).ok()
}

/// AES encrypt (PySpark aes_encrypt). Key as string; uses AES-128-GCM. Output hex.
pub fn apply_aes_encrypt(column: Column, key: &str) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series.str().map_err(|e| compute_err("aes_encrypt", e))?;
    let key_bytes = key.as_bytes();
    let out = StringChunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter()
            .map(|opt_s| opt_s.and_then(|s| aes_gcm_encrypt_one(s.as_bytes(), key_bytes))),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// AES decrypt (PySpark aes_decrypt). Input hex(nonce||ciphertext). Returns null on failure.
pub fn apply_aes_decrypt(column: Column, key: &str) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series.str().map_err(|e| compute_err("aes_decrypt", e))?;
    let key_bytes = key.as_bytes();
    let out = StringChunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter()
            .map(|opt_s| opt_s.and_then(|s| aes_gcm_decrypt_one(s, key_bytes))),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// try_aes_decrypt: same as aes_decrypt, returns null on failure (PySpark try_aes_decrypt).
pub fn apply_try_aes_decrypt(column: Column, key: &str) -> PolarsResult<Option<Column>> {
    apply_aes_decrypt(column, key)
}

/// Int column to single-character string (PySpark char / chr). Valid codepoint only.
pub fn apply_char(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let to_char = |v: i64| -> String {
        let u = v as u32;
        if u <= 0x10FFFF {
            char::from_u32(u).map(|c| c.to_string()).unwrap_or_default()
        } else {
            String::new()
        }
    };
    let out: StringChunked = match series.dtype() {
        DataType::Int32 => {
            let ca = series.i32().map_err(|e| compute_err("char", e))?;
            StringChunked::from_iter_options(
                name.as_str().into(),
                ca.into_iter().map(|opt_v| opt_v.map(|v| to_char(v as i64))),
            )
        }
        DataType::Int64 => {
            let ca = series.i64().map_err(|e| compute_err("char", e))?;
            StringChunked::from_iter_options(
                name.as_str().into(),
                ca.into_iter().map(|opt_v| opt_v.map(to_char)),
            )
        }
        _ => {
            let i64_series = series
                .cast(&DataType::Int64)
                .map_err(|e| compute_err("char cast", e))?;
            let ca = i64_series.i64().map_err(|e| compute_err("char", e))?;
            StringChunked::from_iter_options(
                name.as_str().into(),
                ca.into_iter().map(|opt_v| opt_v.map(to_char)),
            )
        }
    };
    Ok(Some(Column::new(name, out.into_series())))
}

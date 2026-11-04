//! Bitmap UDF implementations.

use super::*;

/// Count set bits in a bitmap (binary column). PySpark bitmap_count.
pub fn apply_bitmap_count(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let ca = series
        .binary()
        .map_err(|e| compute_err("bitmap_count", e))?;
    let out = Int64Chunked::from_iter_options(
        name.as_str().into(),
        ca.into_iter().map(|opt_b| {
            opt_b.map(|b| b.iter().map(|&byte| byte.count_ones() as i64).sum::<i64>())
        }),
    );
    Ok(Some(Column::new(name, out.into_series())))
}

/// Build one bitmap from a list of bit positions (0..32767). Used after implode for bitmap_construct_agg.
pub fn apply_bitmap_construct_agg(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let list_ca = series
        .list()
        .map_err(|e| compute_err("bitmap_construct_agg", e))?;
    let out: BinaryChunked = list_ca
        .amortized_iter()
        .map(|opt_list| {
            opt_list.and_then(|list_series| {
                let mut buf = vec![0u8; BITMAP_BYTES];
                let ca = list_series.as_ref().i64().ok()?;
                for pos in ca.into_iter().flatten() {
                    let pos = pos as usize;
                    if pos < BITMAP_BYTES * 8 {
                        let byte_idx = pos / 8;
                        let bit_idx = pos % 8;
                        buf[byte_idx] |= 1 << bit_idx;
                    }
                }
                Some(bytes::Bytes::from(buf))
            })
        })
        .collect();
    Ok(Some(Column::new(name, out.into_series())))
}

/// Bitwise OR of a list of bitmaps (binary). Used after implode for bitmap_or_agg.
pub fn apply_bitmap_or_agg(column: Column) -> PolarsResult<Option<Column>> {
    let name = column.field().into_owned().name;
    let series = column.take_materialized_series();
    let list_ca = series.list().map_err(|e| compute_err("bitmap_or_agg", e))?;
    let out: BinaryChunked = list_ca
        .amortized_iter()
        .map(|opt_list| {
            opt_list.and_then(|list_series| {
                let list_c = list_series.as_ref().as_list();
                let mut buf = vec![0u8; BITMAP_BYTES];
                for opt_bin in list_c.amortized_iter().flatten() {
                    let bin_ca: &BinaryChunked = opt_bin.as_ref().binary().ok()?;
                    for b in bin_ca.into_iter().flatten() {
                        let b: &[u8] = b;
                        for (i, &byte) in b.iter().take(BITMAP_BYTES).enumerate() {
                            buf[i] |= byte;
                        }
                    }
                }
                Some(bytes::Bytes::from(buf))
            })
        })
        .collect();
    Ok(Some(Column::new(name, out.into_series())))
}

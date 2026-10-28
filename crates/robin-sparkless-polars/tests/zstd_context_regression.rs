//! RUSTSEC-2026-0391: an abandoned stream must not contaminate the next stream.

use std::io::{Read, Write};

use polars_parquet::parquet::compression::{Compression, DecompressionContext, decompress};

#[test]
fn abandoned_encoder_does_not_leak_to_next_stream() {
    let mut context = zstd::zstd_safe::CCtx::create();
    zstd::Encoder::with_context(Vec::new(), &mut context)
        .write_all(b"first request secret")
        .unwrap();

    let mut encoder = zstd::Encoder::with_context(Vec::new(), &mut context);
    encoder.write_all(b"second request").unwrap();
    let encoded = encoder.finish().unwrap();

    assert_eq!(zstd::decode_all(&encoded[..]).unwrap(), b"second request");
}

#[test]
fn abandoned_decoder_does_not_leak_to_next_stream() {
    let first = zstd::encode_all(&b"first request secret"[..], 1).unwrap();
    let second = zstd::encode_all(&b"second request"[..], 1).unwrap();
    let mut context = zstd::zstd_safe::DCtx::create();
    zstd::Decoder::with_context(&first[..], &mut context)
        .read_exact(&mut [0u8; 5])
        .unwrap();

    let mut output = Vec::new();
    zstd::Decoder::with_context(&second[..], &mut context)
        .read_to_end(&mut output)
        .unwrap();

    assert_eq!(output, b"second request");
}

#[test]
fn polars_parquet_reused_context_does_not_leak_to_next_page() {
    let first = zstd::encode_all(&b"first request secret"[..], 1).unwrap();
    let second = zstd::encode_all(&b"second request"[..], 1).unwrap();
    let mut context = DecompressionContext::Unset;

    // A malformed page can declare less output than its compressed frame contains.
    // Polars reads exactly that output length, leaving the frame unfinished.
    decompress(Compression::Zstd, &first, &mut [0u8; 5], &mut context).unwrap();

    let mut output = [0u8; 14];
    decompress(Compression::Zstd, &second, &mut output, &mut context).unwrap();

    assert_eq!(&output, b"second request");
}

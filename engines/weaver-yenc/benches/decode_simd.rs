use std::{env, fs};

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use weaver_yenc::crc::Crc32;
use weaver_yenc::decode::{
    DecodeOptions, DecodeState, decode_body, decode_body_chunk_until_control, decode_chunk,
    decode_rapidyenc,
};

const OUTPUT_BATCH_TARGET: usize = 512 * 1024;

fn encode_yenc_byte(byte: u8) -> Vec<u8> {
    let encoded = byte.wrapping_add(42);
    match encoded {
        0x00 | 0x0a | 0x0d | 0x3d => vec![b'=', encoded.wrapping_add(64)],
        _ => vec![encoded],
    }
}

fn encode_yenc_raw(input: &[u8]) -> Vec<u8> {
    let mut output = Vec::with_capacity(input.len() + input.len() / 64);
    for &byte in input {
        output.extend(encode_yenc_byte(byte));
    }
    output
}

fn clean_body() -> Vec<u8> {
    let mut body = Vec::with_capacity(512 * 1024);
    for idx in 0..512 * 1024 {
        let byte = 32u8.wrapping_add((idx % 80) as u8);
        body.push(byte);
        if idx % 128 == 127 {
            body.extend_from_slice(b"\r\n");
        }
    }
    body
}

fn escape_heavy_body() -> Vec<u8> {
    let source = [214u8, 224, 227, 19, b'A', b'B', b'C'];
    let mut raw = Vec::with_capacity(256 * 1024);
    for idx in 0..256 * 1024 {
        raw.push(source[idx % source.len()]);
    }
    encode_yenc_raw(&raw)
}

fn dot_stuffed_body() -> Vec<u8> {
    let mut body = Vec::with_capacity(512 * 1024);
    for line in 0..4096usize {
        if line % 4 == 0 {
            body.extend_from_slice(b"..dot-stuffed-line");
        } else {
            body.extend_from_slice(b"ordinary-yenc-line");
        }
        body.extend_from_slice(b"\r\n");
    }
    body
}

fn bigbang_like_body() -> Vec<u8> {
    let mut body = Vec::with_capacity(792 * 1024);
    for idx in 0..768_000usize {
        let byte = ((idx * 31 + 17) & 0xff) as u8;
        body.extend(encode_yenc_byte(byte));
        if idx % 128 == 127 {
            body.extend_from_slice(b"\r\n");
        }
    }
    body
}

/// Column-accurate yEnc encoding at 128 encoded columns, matching what real
/// posts (and rapidyenc's own bench) look like. `bigbang_like_body` breaks
/// after 128 *input* bytes instead, which shifts line-break phase relative to
/// the decoder's 64-byte windows and historically flattered the kernel.
fn real_yenc_128col_body() -> Vec<u8> {
    let mut body = Vec::with_capacity(800 * 1024);
    let mut col = 0usize;
    for idx in 0..768_000usize {
        let byte = ((idx * 31 + 17) & 0xff) as u8;
        let encoded = byte.wrapping_add(42);
        match encoded {
            0x00 | 0x0a | 0x0d | 0x3d => {
                body.push(b'=');
                body.push(encoded.wrapping_add(64));
                col += 2;
            }
            0x2e if col == 0 => {
                body.push(b'=');
                body.push(encoded.wrapping_add(64));
                col += 2;
            }
            _ => {
                body.push(encoded);
                col += 1;
            }
        }
        if col >= 128 {
            body.extend_from_slice(b"\r\n");
            col = 0;
        }
    }
    if col > 0 {
        body.extend_from_slice(b"\r\n");
    }
    body
}

/// Kernel-only lane: rapidyenc-semantics raw decode, no CRC. Directly
/// comparable to `rapidyenc_decode` numbers from the parity bench.
fn bench_decode_only(c: &mut Criterion, name: &str, input: &[u8]) {
    let mut output = vec![0u8; input.len() + 64];
    c.bench_function(name, |b| {
        b.iter(|| {
            let written = decode_rapidyenc(black_box(input), &mut output).unwrap();
            black_box(written);
        });
    });
}

fn raw_terminator_body() -> Vec<u8> {
    let mut body = dot_stuffed_body();
    body.extend_from_slice(b".\r\nignored-after-terminator");
    body
}

fn bench_body(c: &mut Criterion, name: &str, input: &[u8], options: DecodeOptions) {
    let mut output = vec![0u8; input.len() + 64];
    c.bench_function(name, |b| {
        b.iter(|| {
            let mut crc = Crc32::new();
            let written = decode_body(black_box(input), &mut output, &mut crc, options).unwrap();
            black_box(written);
            black_box(crc.finalize());
        });
    });
}

fn bench_until_control(c: &mut Criterion, input: &[u8]) {
    let mut output = Vec::with_capacity(input.len() + 64);
    c.bench_function("yenc_decode_bigbang_like_until_control_3_chunks", |b| {
        b.iter(|| {
            output.clear();
            let mut state = DecodeState::new();
            let chunk_len = input.len().div_ceil(3);
            let mut start = 0usize;
            while start < input.len() {
                let end = (start + chunk_len).min(input.len());
                let progress = decode_body_chunk_until_control(
                    &mut state,
                    black_box(&input[start..end]),
                    &mut output,
                )
                .unwrap();
                start += progress.source_consumed;
            }
            black_box(output.len());
            black_box(state.finalize_crc());
        });
    });
}

fn find_from(haystack: &[u8], needle: &[u8], start: usize) -> Option<usize> {
    haystack
        .get(start..)?
        .windows(needle.len())
        .position(|window| window == needle)
        .map(|idx| start + idx)
}

fn extract_yenc_body_from_article(article: &[u8]) -> Option<Vec<u8>> {
    let mut pos = find_from(article, b"\r\n\r\n", 0)? + 4;
    loop {
        let line_end = find_from(article, b"\r\n", pos)?;
        let line = &article[pos..line_end];
        if line.starts_with(b"=ybegin ") || line.starts_with(b"=ypart ") {
            pos = line_end + 2;
            continue;
        }
        break;
    }

    let yend_marker = find_from(article, b"\r\n=yend ", pos)?;
    Some(article[pos..yend_marker + 2].to_vec())
}

fn decode_body_len(input: &[u8]) -> usize {
    let mut output = vec![0u8; input.len() + 64];
    let mut crc = Crc32::new();
    decode_body(
        input,
        &mut output,
        &mut crc,
        DecodeOptions {
            dot_unstuffing: true,
        },
    )
    .unwrap()
}

fn flush_macro_output(
    output: &mut Vec<u8>,
    chunks: &mut Vec<Box<[u8]>>,
    decoded: usize,
    expected: usize,
) {
    while output.len() >= OUTPUT_BATCH_TARGET {
        let full_batch = if output.len() == OUTPUT_BATCH_TARGET {
            let next_capacity = expected.saturating_sub(decoded).min(OUTPUT_BATCH_TARGET);
            std::mem::replace(output, Vec::with_capacity(next_capacity))
        } else {
            let remainder = output.split_off(OUTPUT_BATCH_TARGET);
            std::mem::replace(output, remainder)
        };
        chunks.push(full_batch.into_boxed_slice());
    }
}

fn decode_macro_chunk_shape(input: &[u8], decoded_len: usize) -> (usize, u32, usize, u64) {
    let mut output = Vec::with_capacity(decoded_len.min(OUTPUT_BATCH_TARGET));
    let mut chunks = Vec::new();
    let mut state = DecodeState::new();
    let mut start = 0usize;
    let mut decode_calls = 0u64;

    while start < input.len() {
        flush_macro_output(
            &mut output,
            &mut chunks,
            state.bytes_decoded as usize,
            decoded_len,
        );
        let input_len = (input.len() - start).min(OUTPUT_BATCH_TARGET.saturating_sub(output.len()));
        if input_len == 0 {
            continue;
        }
        let progress = decode_body_chunk_until_control(
            &mut state,
            &input[start..start + input_len],
            &mut output,
        )
        .unwrap();
        decode_calls += 1;
        start += progress.source_consumed;
        flush_macro_output(
            &mut output,
            &mut chunks,
            state.bytes_decoded as usize,
            decoded_len,
        );
    }

    chunks.push(output.into_boxed_slice());
    (
        state.bytes_decoded as usize,
        state.finalize_crc(),
        chunks.len(),
        decode_calls,
    )
}

fn decode_macro_chunk_shape_with_input_chunks(
    input: &[u8],
    decoded_len: usize,
    input_chunk_size: usize,
) -> (usize, u32, usize, u64) {
    assert!(input_chunk_size > 0);

    let mut output = Vec::with_capacity(decoded_len.min(OUTPUT_BATCH_TARGET));
    let mut chunks = Vec::new();
    let mut state = DecodeState::new();
    let mut consumed = 0usize;
    let mut available = 0usize;
    let mut decode_calls = 0u64;

    while consumed < input.len() {
        if consumed == available {
            available = (available + input_chunk_size).min(input.len());
        }

        loop {
            flush_macro_output(
                &mut output,
                &mut chunks,
                state.bytes_decoded as usize,
                decoded_len,
            );
            let available_len = available.saturating_sub(consumed);
            if available_len == 0 {
                break;
            }
            let input_len = available_len.min(OUTPUT_BATCH_TARGET.saturating_sub(output.len()));
            if input_len == 0 {
                continue;
            }
            let progress = decode_body_chunk_until_control(
                &mut state,
                &input[consumed..consumed + input_len],
                &mut output,
            )
            .unwrap();
            decode_calls += 1;
            if progress.source_consumed == 0 {
                break;
            }
            consumed += progress.source_consumed;
            flush_macro_output(
                &mut output,
                &mut chunks,
                state.bytes_decoded as usize,
                decoded_len,
            );
        }
    }

    chunks.push(output.into_boxed_slice());
    (
        state.bytes_decoded as usize,
        state.finalize_crc(),
        chunks.len(),
        decode_calls,
    )
}

fn bench_real_article_if_configured(c: &mut Criterion) {
    let Ok(path) = env::var("WEAVER_YENC_REAL_ARTICLE") else {
        return;
    };
    let article = fs::read(&path).expect("read WEAVER_YENC_REAL_ARTICLE");
    let body = extract_yenc_body_from_article(&article).expect("extract yEnc article body");
    let decoded_len = decode_body_len(&body);

    c.bench_function("yenc_decode_real_article_body", |b| {
        let mut output = vec![0u8; body.len() + 64];
        b.iter(|| {
            let mut crc = Crc32::new();
            let written = decode_body(
                black_box(&body),
                &mut output,
                &mut crc,
                DecodeOptions {
                    dot_unstuffing: true,
                },
            )
            .unwrap();
            black_box(written);
            black_box(crc.finalize());
        });
    });

    c.bench_function("yenc_decode_real_article_macro_chunks", |b| {
        b.iter(|| {
            let (written, crc, chunks, calls) =
                decode_macro_chunk_shape(black_box(&body), decoded_len);
            black_box(written);
            black_box(crc);
            black_box(chunks);
            black_box(calls);
        });
    });

    c.bench_function("yenc_decode_real_article_macro_input_360k", |b| {
        b.iter(|| {
            let (written, crc, chunks, calls) = decode_macro_chunk_shape_with_input_chunks(
                black_box(&body),
                decoded_len,
                360 * 1024,
            );
            black_box(written);
            black_box(crc);
            black_box(chunks);
            black_box(calls);
        });
    });
}

fn bench_chunked(c: &mut Criterion, input: &[u8]) {
    let mut output = vec![0u8; input.len() + 64];
    c.bench_function("yenc_decode_chunked_awkward_splits", |b| {
        b.iter(|| {
            let mut state = DecodeState::new();
            let mut written = 0usize;
            let mut start = 0usize;
            while start < input.len() {
                let end = (start + 257).min(input.len());
                let n = decode_chunk(
                    black_box(&input[start..end]),
                    &mut output[written..],
                    &mut state,
                    DecodeOptions {
                        dot_unstuffing: true,
                    },
                )
                .unwrap();
                written += n;
                start = end;
            }
            black_box(written);
            black_box(state.finalize_crc());
        });
    });
}

fn benches(c: &mut Criterion) {
    let clean = clean_body();
    let escapes = escape_heavy_body();
    let dot_stuffed = dot_stuffed_body();
    let raw_terminator = raw_terminator_body();
    let bigbang_like = bigbang_like_body();
    let real_shape = real_yenc_128col_body();

    bench_body(
        c,
        "yenc_decode_mostly_clean",
        &clean,
        DecodeOptions::default(),
    );
    bench_body(
        c,
        "yenc_decode_escape_heavy",
        &escapes,
        DecodeOptions::default(),
    );
    bench_body(
        c,
        "yenc_decode_dot_stuffed_nntp",
        &dot_stuffed,
        DecodeOptions {
            dot_unstuffing: true,
        },
    );
    bench_body(
        c,
        "yenc_decode_raw_terminator",
        &raw_terminator,
        DecodeOptions {
            dot_unstuffing: true,
        },
    );
    bench_body(
        c,
        "yenc_decode_bigbang_like_body",
        &bigbang_like,
        DecodeOptions {
            dot_unstuffing: true,
        },
    );
    bench_body(
        c,
        "yenc_decode_realshape_128col",
        &real_shape,
        DecodeOptions {
            dot_unstuffing: true,
        },
    );
    bench_decode_only(c, "yenc_decode_only_realshape_128col", &real_shape);
    bench_decode_only(c, "yenc_decode_only_bigbang_like", &bigbang_like);
    bench_until_control(c, &bigbang_like);
    bench_chunked(c, &dot_stuffed);
    bench_real_article_if_configured(c);
}

criterion_group!(decode_simd, benches);
criterion_main!(decode_simd);

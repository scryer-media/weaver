use criterion::{Criterion, black_box, criterion_group, criterion_main};
use weaver_yenc::crc::Crc32;
use weaver_yenc::decode::{DecodeOptions, DecodeState, decode_body, decode_chunk};

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
    bench_chunked(c, &dot_stuffed);
}

criterion_group!(decode_simd, benches);
criterion_main!(decode_simd);

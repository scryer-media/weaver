//! Long-running single-fixture decode loop for hardware profiling (uProf).
//!
//!   profile_kernel [realshape|crlf|clean|esc] [iters]
//!
//! Hammers one fixture through `decode_rapidyenc` so a sampling profiler
//! collects thousands of samples inside `decode_kernel_avx2`.

use weaver_yenc::decode::decode_rapidyenc;

const DECODED_TARGET: usize = 768_000;

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

fn crlf_only_body() -> Vec<u8> {
    let mut body = Vec::with_capacity(DECODED_TARGET + DECODED_TARGET / 64 + 64);
    let mut produced = 0usize;
    while produced < DECODED_TARGET {
        let line = 128.min(DECODED_TARGET - produced);
        body.extend(std::iter::repeat_n(0x40u8, line));
        body.extend_from_slice(b"\r\n");
        produced += line;
    }
    body
}

fn clean_body() -> Vec<u8> {
    vec![0x40u8; DECODED_TARGET]
}

fn esc_only_body() -> Vec<u8> {
    let mut body = Vec::with_capacity(DECODED_TARGET + DECODED_TARGET / 64);
    for idx in 0..DECODED_TARGET {
        if idx % 256 == 128 {
            body.push(b'=');
            body.push(0x40);
        } else {
            body.push(0x40);
        }
    }
    body
}

fn main() {
    let mut args = std::env::args().skip(1);
    let fixture = args.next().unwrap_or_else(|| "realshape".into());
    let iters: usize = args
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or(250_000);

    let input = match fixture.as_str() {
        "crlf" => crlf_only_body(),
        "clean" => clean_body(),
        "esc" => esc_only_body(),
        _ => real_yenc_128col_body(),
    };
    let mut out = vec![0u8; input.len() + 64];
    eprintln!("profiling fixture={fixture} iters={iters} input_len={}", input.len());
    let mut acc = 0usize;
    for _ in 0..iters {
        acc = acc.wrapping_add(decode_rapidyenc(std::hint::black_box(&input), &mut out).unwrap());
    }
    std::hint::black_box(acc);
    eprintln!("done acc={acc}");
}

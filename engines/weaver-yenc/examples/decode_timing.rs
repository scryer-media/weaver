//! Standalone weaver-yenc decode timing harness (no rapidyenc dependency).
//!
//! Times `decode_rapidyenc` on the same five fixtures as the parity bench so a
//! before/after can be taken on a box where the rapidyenc shared library is not
//! available. Prints min + median microseconds per fixture over a fixed iter
//! count; compare min against the recorded rapidyenc baseline for the box.
//!
//!   cargo run --release --example decode_timing

use std::time::Instant;

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

fn clean_body() -> Vec<u8> {
    vec![0x40u8; DECODED_TARGET]
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

fn dots_body() -> Vec<u8> {
    let mut body = vec![0x40u8; DECODED_TARGET];
    let mut idx = 128usize;
    while idx < DECODED_TARGET {
        body[idx] = b'.';
        idx += 256;
    }
    body
}

fn time_fixture(name: &str, input: &[u8]) {
    let mut out = vec![0u8; input.len() + 64];
    // Warmup.
    for _ in 0..100 {
        let w = decode_rapidyenc(input, &mut out).unwrap();
        std::hint::black_box(w);
    }
    let iters = 2000usize;
    let mut samples = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t = Instant::now();
        let w = decode_rapidyenc(std::hint::black_box(input), &mut out).unwrap();
        let dt = t.elapsed();
        std::hint::black_box(w);
        samples.push(dt.as_nanos() as u64);
    }
    samples.sort_unstable();
    let min = samples[0] as f64 / 1000.0;
    let median = samples[iters / 2] as f64 / 1000.0;
    let decoded = decode_rapidyenc(input, &mut out).unwrap();
    let gbps = decoded as f64 / (min * 1e-6) / 1e9;
    println!(
        "{name:<12} min {min:>8.3} us  median {median:>8.3} us  ({gbps:.2} GB/s at min, {decoded} decoded)"
    );

    #[cfg(rapidyenc_linked)]
    {
        let mut rout = vec![0u8; input.len() + 64];
        // Byte-parity vs the real library before timing.
        let rw = unsafe {
            weaver_rapidyenc_decode(
                input.as_ptr() as *const core::ffi::c_void,
                rout.as_mut_ptr() as *mut core::ffi::c_void,
                input.len() as u64,
            )
        } as usize;
        assert_eq!(rw, decoded, "{name}: rapidyenc decoded length != weaver");
        assert_eq!(
            &rout[..rw],
            &out[..decoded],
            "{name}: rapidyenc bytes != weaver"
        );
        for _ in 0..100 {
            unsafe {
                weaver_rapidyenc_decode(
                    input.as_ptr() as *const core::ffi::c_void,
                    rout.as_mut_ptr() as *mut core::ffi::c_void,
                    input.len() as u64,
                );
            }
        }
        let mut rsamples = Vec::with_capacity(iters);
        for _ in 0..iters {
            let t = Instant::now();
            let w = unsafe {
                weaver_rapidyenc_decode(
                    std::hint::black_box(input.as_ptr()) as *const core::ffi::c_void,
                    rout.as_mut_ptr() as *mut core::ffi::c_void,
                    input.len() as u64,
                )
            };
            let dt = t.elapsed();
            std::hint::black_box(w);
            rsamples.push(dt.as_nanos() as u64);
        }
        rsamples.sort_unstable();
        let rmin = rsamples[0] as f64 / 1000.0;
        let rmedian = rsamples[iters / 2] as f64 / 1000.0;
        let rgbps = decoded as f64 / (rmin * 1e-6) / 1e9;
        println!(
            "{:<12} min {rmin:>8.3} us  median {rmedian:>8.3} us  ({rgbps:.2} GB/s at min)  [weaver/rapidyenc = {:.2}x]",
            "  rapidyenc",
            min / rmin
        );
    }
}

#[cfg(rapidyenc_linked)]
unsafe extern "C" {
    fn weaver_rapidyenc_decode_init();
    fn weaver_rapidyenc_decode(
        src: *const core::ffi::c_void,
        dest: *mut core::ffi::c_void,
        len: u64,
    ) -> u64;
}

fn main() {
    #[cfg(rapidyenc_linked)]
    unsafe {
        weaver_rapidyenc_decode_init();
    }
    println!("weaver-yenc decode timing (min of 2000 iters, 768000-byte decode target)\n");
    time_fixture("realshape", &real_yenc_128col_body());
    time_fixture("clean", &clean_body());
    time_fixture("crlf_only", &crlf_only_body());
    time_fixture("esc_only", &esc_only_body());
    time_fixture("dots_body", &dots_body());
}
